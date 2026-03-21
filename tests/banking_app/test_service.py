from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pytest
from di_unit_of_work.session_aspect import SessionAspect
from di_unit_of_work.session_cache import SessionCache
from di_unit_of_work.session_factory.sqlite_session_factory import SqlLiteConfig, SQLiteSessionFactory
from di_unit_of_work.session_provider import SessionProvider
from python_di_application.dependency import Dependency, DependencyInstance
from python_di_application.di_container import DIContainer
from sqlalchemy import case, func, select

from depot_tracking.components.analytics.service import AnalyticsService
from depot_tracking.components.data_operations.asset_value_data_operations import AssetValueDataOperations
from depot_tracking.components.data_operations.asset_value_repository import AssetValueRepository
from depot_tracking.components.data_operations.holding_snapshot_data_operations import HoldingSnapshotDataOperations
from depot_tracking.components.data_operations.holding_snapshot_repository import HoldingSnapshotRepository
from depot_tracking.components.data_operations.portfolio_monthly_history_data_operations import \
    PortfolioMonthlyHistoryDataOperations
from depot_tracking.components.data_operations.processed_file_data_operations import ProcessedFileDataOperations
from depot_tracking.components.data_operations.processed_file_repository import ProcessedFileRepository
from depot_tracking.components.data_operations.product_data_operations import ProductDataOperations
from depot_tracking.components.data_operations.product_repository import ProductRepository
from depot_tracking.components.data_operations.source_document_data_operations import SourceDocumentDataOperations
from depot_tracking.components.data_operations.transaction_data_operations import TransactionDataOperations
from depot_tracking.components.data_operations.transaction_repository import TransactionRepository
from depot_tracking.components.ingestion import DocumentDeduplicationService
from depot_tracking.components.ingestion import DocumentRouter
from depot_tracking.components.ingestion import IngestionService
from depot_tracking.components.ingestion import IngestionStore
from depot_tracking.components.ingestion.parsing.parser_factory import ParserFactory
from depot_tracking.components.ingestion.parsing.pdf_parser import UnsupportedPdfDocument
from depot_tracking.components.market.market_data import HistoricalPriceResult, MarketDataError, QuoteResult
from depot_tracking.components.market.market_data import YahooMarketDataClient
from depot_tracking.components.repair import RepairService
from depot_tracking.components.shared import (
    BankClassifier,
    CalendarMonthService,
    IdentifierCanonicalizer,
    RepairRulesLoader,
    SourceDocumentNormalizer,
)
from depot_tracking.config import BankingAppConfig, ParserConfig
from depot_tracking.core.db import initialize_database
from depot_tracking.core.models import (
    AssetValue,
    Base,
    HoldingSnapshot,
    ParsedHolding,
    ParsedTransaction,
    PortfolioMonthlyHistory,
    ProcessedFile,
    Product,
    Transaction,
    TransactionType,
)


class StubParser:
    parser_version = "test"

    def __init__(
            self,
            parsed_by_filename: dict[str, ParsedTransaction],
            depotauszug_by_filename: dict[str, tuple[date, list[ParsedHolding]]] | None = None,
    ) -> None:
        self.parsed_by_filename = parsed_by_filename
        self.depotauszug_by_filename = depotauszug_by_filename or {}

    def parse(self, pdf_path: str | Path) -> ParsedTransaction:
        name = Path(pdf_path).name
        if name in self.parsed_by_filename:
            return self.parsed_by_filename[name]
        raise UnsupportedPdfDocument(f"Unsupported PDF document type: {name}")

    def parse_depotauszug_holdings(self, pdf_path: str | Path) -> tuple[date, list[ParsedHolding]]:
        name = Path(pdf_path).name
        if name not in self.depotauszug_by_filename:
            raise UnsupportedPdfDocument(f"Not a Depotauszug document: {name}")
        return self.depotauszug_by_filename[name]


class StaticParserFactory:
    def __init__(self, parser: StubParser) -> None:
        self._parser = parser

    def build_parser(self, bank: str) -> StubParser:
        return self._parser


@dataclass
class StubMarketClient:
    symbol: str = "TEST.DE"
    value: float = 120.0
    quote_currency: str = "EUR"
    historical_currency: str = "EUR"
    fx_rate: float = 1.0
    historical_fx_rate: float = 1.0
    raise_on_quote: bool = False

    def fetch_quote(self, *, wkn: str, isin: str | None, name: str | None, ticker: str | None) -> QuoteResult:
        if self.raise_on_quote:
            raise MarketDataError(f"No Yahoo symbol found for {wkn}")
        return QuoteResult(symbol=self.symbol, value=self.value, currency=self.quote_currency)

    def resolve_symbol(self, *, wkn: str, isin: str | None, name: str | None, ticker: str | None) -> str:
        return ticker or self.symbol

    def fetch_historical_quote(
            self, symbol: str, on_date: date, *, lookback_days: int = 14, lookahead_days: int = 5
    ) -> HistoricalPriceResult:
        return HistoricalPriceResult(
            symbol=symbol,
            value=self.value,
            currency=self.historical_currency,
            price_date=on_date,
        )

    def fetch_historical_price(self, symbol: str, on_date: date, *, lookback_days: int = 14,
                               lookahead_days: int = 5) -> float:
        return self.value

    def fetch_fx_rate(self, *, base_currency: str, quote_currency: str = "EUR") -> float:
        if base_currency.upper() == quote_currency.upper():
            return 1.0
        return self.fx_rate

    def fetch_historical_fx_rate(
            self, *, base_currency: str, on_date: date, quote_currency: str = "EUR", lookback_days: int = 14,
            lookahead_days: int = 5
    ) -> float:
        if base_currency.upper() == quote_currency.upper():
            return 1.0
        return self.historical_fx_rate


@dataclass
class ServiceBundle:
    session_factory: SQLiteSessionFactory
    ingestion: IngestionService
    deduplication: DocumentDeduplicationService
    analytics: AnalyticsService
    repair: RepairService


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "test.sqlite"
    initialize_database(path)
    return path


def _touch_pdf(path: Path) -> None:
    path.write_bytes(f"%PDF-1.4\n{path.name}\n".encode("utf-8"))


def _build_services(
        db_path: Path,
        *,
        parser: StubParser | None = None,
        parser_bank: str = "auto",
        market_client: StubMarketClient | None = None,
) -> ServiceBundle:
    container = DIContainer()
    container.register_dependencies(
        [
            Dependency(dependency_type=SessionCache),
            Dependency(dependency_type=SessionAspect),
            Dependency(dependency_type=SessionProvider),
            Dependency(dependency_type=SQLiteSessionFactory),
            Dependency(dependency_type=ParserFactory),
            Dependency(dependency_type=DocumentRouter),
            Dependency(dependency_type=IngestionStore),
            Dependency(dependency_type=IngestionService),
            Dependency(dependency_type=DocumentDeduplicationService),
            Dependency(dependency_type=AnalyticsService),
            Dependency(dependency_type=RepairService),
            Dependency(dependency_type=SourceDocumentDataOperations),
            Dependency(dependency_type=ProductRepository),
            Dependency(dependency_type=ProductDataOperations),
            Dependency(dependency_type=TransactionRepository),
            Dependency(dependency_type=TransactionDataOperations),
            Dependency(dependency_type=HoldingSnapshotRepository),
            Dependency(dependency_type=HoldingSnapshotDataOperations),
            Dependency(dependency_type=ProcessedFileRepository),
            Dependency(dependency_type=ProcessedFileDataOperations),
            Dependency(dependency_type=AssetValueRepository),
            Dependency(dependency_type=AssetValueDataOperations),
            Dependency(dependency_type=PortfolioMonthlyHistoryDataOperations),
            Dependency(dependency_type=IdentifierCanonicalizer),
            Dependency(dependency_type=BankClassifier),
            Dependency(dependency_type=SourceDocumentNormalizer),
            Dependency(dependency_type=CalendarMonthService),
            Dependency(dependency_type=RepairRulesLoader),
        ]
    )
    container.register_instances(
        [
            DependencyInstance(BankingAppConfig(db_path=db_path)),
            DependencyInstance(SqlLiteConfig(path=str(db_path), metadata=Base.metadata)),
            DependencyInstance(ParserConfig(bank_hint=parser_bank)),
            DependencyInstance(StaticParserFactory(parser) if parser is not None else ParserFactory(),
                               dependency_interface=ParserFactory),
            DependencyInstance(market_client or StubMarketClient(), dependency_interface=YahooMarketDataClient),
        ]
    )
    for dependency_type in (
            SQLiteSessionFactory,
            IngestionService,
            DocumentDeduplicationService,
            AnalyticsService,
            RepairService,
    ):
        container.resolve_dependency(dependency_type)
    container.apply_post_init_wrappers()
    return ServiceBundle(
        session_factory=container.resolve_dependency(SQLiteSessionFactory),
        ingestion=container.resolve_dependency(IngestionService),
        deduplication=container.resolve_dependency(DocumentDeduplicationService),
        analytics=container.resolve_dependency(AnalyticsService),
        repair=container.resolve_dependency(RepairService),
    )


def test_ingest_directory_is_idempotent(db_path: Path, tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()

    buy_pdf = incoming / "buy.pdf"
    ertrags_pdf = incoming / "ertrags.pdf"
    _touch_pdf(buy_pdf)
    _touch_pdf(ertrags_pdf)

    parser = StubParser(
        {
            "buy.pdf": ParsedTransaction(
                tx_type=TransactionType.BUY,
                wkn="A1C1H5",
                isin="IE00B5L8K969",
                product_name="ETF Product",
                transaction_date=date(2026, 2, 2),
                quantity=4.0,
                gross_amount=1000.0,
                costs=0.0,
            ),
            "ertrags.pdf": ParsedTransaction(
                tx_type=TransactionType.ERTRAGSABRECHNUNG,
                wkn="A1C1H5",
                isin="IE00B5L8K969",
                product_name="ETF Product",
                transaction_date=date(2026, 2, 3),
                quantity=0.0,
                gross_amount=-25.0,
                costs=0.0,
            ),
        }
    )
    services = _build_services(db_path, parser=parser, market_client=StubMarketClient())

    first = services.ingestion.ingest_directory(incoming)
    second = services.ingestion.ingest_directory(incoming)

    assert first == {"seen": 2, "ingested": 2, "skipped": 0, "errors": 0}
    assert second == {"seen": 2, "ingested": 0, "skipped": 2, "errors": 0}

    with services.session_factory() as session:
        products = session.scalar(select(func.count()).select_from(Product))
        tx_count = session.scalar(select(func.count()).select_from(Transaction))
        files = session.scalar(select(func.count()).select_from(ProcessedFile))
        types = {row[0] for row in session.execute(select(Transaction.type)).all()}

    assert products == 1
    assert tx_count == 2
    assert files == 2
    assert types == {TransactionType.BUY, TransactionType.ERTRAGSABRECHNUNG}


def test_ingest_sets_transaction_bank_from_filename_patterns(db_path: Path, tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()

    ing_pdf = incoming / "Direkt_Depot_8013529518_Abrechnung_Kauf.pdf"
    consors_pdf = incoming / "KAUF_448609135_ord1_001_wknA2PLS9_dat20250409_id1.pdf"
    trade_republic_pdf = incoming / "trade_republic_statement.pdf"
    _touch_pdf(ing_pdf)
    _touch_pdf(consors_pdf)
    _touch_pdf(trade_republic_pdf)

    parser = StubParser(
        {
            ing_pdf.name: ParsedTransaction(
                tx_type=TransactionType.BUY,
                wkn="A1C1H5",
                isin="IE00B5L8K969",
                product_name="ING Product",
                transaction_date=date(2026, 2, 2),
                quantity=1.0,
                gross_amount=100.0,
                costs=0.0,
            ),
            consors_pdf.name: ParsedTransaction(
                tx_type=TransactionType.BUY,
                wkn="A2PKXG",
                isin="IE00BK5BQT80",
                product_name="Consors Product",
                transaction_date=date(2026, 2, 3),
                quantity=1.0,
                gross_amount=200.0,
                costs=0.0,
            ),
            trade_republic_pdf.name: ParsedTransaction(
                tx_type=TransactionType.BUY,
                wkn="A2PKXG",
                isin="IE00BK5BQT80",
                product_name="Trade Republic Product",
                transaction_date=date(2026, 2, 4),
                quantity=1.0,
                gross_amount=300.0,
                costs=0.0,
            ),
        }
    )
    services = _build_services(db_path, parser=parser, parser_bank="auto", market_client=StubMarketClient())

    stats = services.ingestion.ingest_directory(incoming)
    assert stats["ingested"] == 3

    with services.session_factory() as session:
        rows = session.execute(select(Transaction.source_file, Transaction.bank)).all()
    bank_by_file = {Path(source).name: bank for source, bank in rows}
    assert bank_by_file[ing_pdf.name] == "ING"
    assert bank_by_file[consors_pdf.name] == "CONSORS"
    assert bank_by_file[trade_republic_pdf.name] == "TRADE_REPUBLIC"


def test_ingest_uses_parser_bank_hint_for_generic_filenames(db_path: Path, tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()

    buy_pdf = incoming / "buy.pdf"
    _touch_pdf(buy_pdf)

    parser = StubParser(
        {
            buy_pdf.name: ParsedTransaction(
                tx_type=TransactionType.BUY,
                wkn="A1XB5U",
                isin="IE00BK1PV551",
                product_name="Generic Product",
                transaction_date=date(2026, 2, 2),
                quantity=1.0,
                gross_amount=100.0,
                costs=0.0,
            )
        }
    )
    services = _build_services(db_path, parser=parser, parser_bank="consors", market_client=StubMarketClient())

    stats = services.ingestion.ingest_directory(incoming)
    assert stats["ingested"] == 1

    with services.session_factory() as session:
        bank = session.scalar(select(Transaction.bank))
    assert bank == "CONSORS"


def test_ingest_skips_duplicate_suffix_files_by_canonical_name(db_path: Path, tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()

    base_pdf = incoming / "VERKAUF_448609135_ord316351240_001_wkn918422_dat20250102_id1442890266.pdf"
    dup_pdf = incoming / "VERKAUF_448609135_ord316351240_001_wkn918422_dat20250102_id1442890266_1.pdf"
    _touch_pdf(base_pdf)
    _touch_pdf(dup_pdf)

    tx = ParsedTransaction(
        tx_type=TransactionType.SELL,
        wkn="918422",
        isin="US67066G1040",
        product_name="NVIDIA CORP.",
        transaction_date=date(2025, 1, 2),
        quantity=1.0,
        gross_amount=100.0,
        costs=1.0,
    )
    parser = StubParser(
        {
            base_pdf.name: tx,
            dup_pdf.name: tx,
        }
    )
    services = _build_services(db_path, parser=parser, parser_bank="consors", market_client=StubMarketClient())

    stats = services.ingestion.ingest_directory(incoming)
    assert stats == {"seen": 2, "ingested": 1, "skipped": 1, "errors": 0}

    with services.session_factory() as session:
        tx_count = session.scalar(select(func.count()).select_from(Transaction))
    assert tx_count == 1


def test_cleanup_duplicate_documents_removes_existing_duplicate_rows_and_files(db_path: Path, tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()

    base_name = "VERKAUF_448609135_ord316351240_001_wkn918422_dat20250102_id1442890266.pdf"
    dup_name = "VERKAUF_448609135_ord316351240_001_wkn918422_dat20250102_id1442890266_1.pdf"
    base_pdf = incoming / base_name
    dup_pdf = incoming / dup_name
    _touch_pdf(base_pdf)
    _touch_pdf(dup_pdf)

    services = _build_services(
        db_path, parser=StubParser({}), parser_bank="consors", market_client=StubMarketClient()
    )
    with services.session_factory() as session:
        product = Product(wkn="918422", isin="US67066G1040", name="NVIDIA CORP.")
        session.add(product)
        session.flush()
        session.add(
            Transaction(
                product_id=product.id,
                type=TransactionType.SELL,
                transaction_date=date(2025, 1, 2),
                quantity=1.0,
                gross_amount=100.0,
                costs=1.0,
                currency="EUR",
                bank="CONSORS",
                source_file=str(base_pdf),
                source_hash="dup-hash-1",
            )
        )
        session.add(
            Transaction(
                product_id=product.id,
                type=TransactionType.SELL,
                transaction_date=date(2025, 1, 2),
                quantity=1.0,
                gross_amount=100.0,
                costs=1.0,
                currency="EUR",
                bank="CONSORS",
                source_file=str(dup_pdf),
                source_hash="dup-hash-2",
            )
        )
        session.add(
            ProcessedFile(
                file_path=str(base_pdf),
                file_hash="processed-dup-hash-1",
                parser_version="consors-v1",
            )
        )
        session.add(
            ProcessedFile(
                file_path=str(dup_pdf),
                file_hash="processed-dup-hash-2",
                parser_version="consors-v1",
            )
        )
        session.commit()

    stats = services.deduplication.cleanup_duplicate_documents(incoming)
    assert stats["files_removed"] == 1
    assert stats["tx_removed"] == 1
    assert stats["processed_removed"] == 1

    assert base_pdf.exists()
    assert not dup_pdf.exists()
    with services.session_factory() as session:
        tx_rows = session.execute(select(Transaction.source_file)).all()
        processed_rows = session.execute(select(ProcessedFile.file_path)).all()

    assert len(tx_rows) == 1
    assert len(processed_rows) == 1
    assert Path(tx_rows[0][0]).name == base_name
    assert Path(processed_rows[0][0]).name == base_name


def test_profit_view_includes_ertragsabrechnung_cashflow(db_path: Path, tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()

    buy_pdf = incoming / "buy.pdf"
    ertrags_pdf = incoming / "ertrags.pdf"
    _touch_pdf(buy_pdf)
    _touch_pdf(ertrags_pdf)

    parser = StubParser(
        {
            "buy.pdf": ParsedTransaction(
                tx_type=TransactionType.BUY,
                wkn="A1C1H5",
                isin="IE00B5L8K969",
                product_name="ETF Product",
                transaction_date=date(2026, 2, 2),
                quantity=10.0,
                gross_amount=1000.0,
                costs=0.0,
            ),
            "ertrags.pdf": ParsedTransaction(
                tx_type=TransactionType.ERTRAGSABRECHNUNG,
                wkn="A1C1H5",
                isin="IE00B5L8K969",
                product_name="ETF Product",
                transaction_date=date(2026, 2, 3),
                quantity=0.0,
                gross_amount=-50.0,
                costs=0.0,
            ),
        }
    )
    services = _build_services(db_path, parser=parser, market_client=StubMarketClient(value=120.0))

    ingest_stats = services.ingestion.ingest_directory(incoming)
    value_stats = services.analytics.update_open_asset_values()

    assert ingest_stats["ingested"] == 2
    assert value_stats["updated"] == 1

    total = services.analytics.fetch_current_profit()
    products = services.analytics.fetch_product_profit()

    assert total["net_cashflow"] == pytest.approx(-1050.0)
    assert total["current_portfolio_value"] == pytest.approx(1200.0)
    assert total["total_profit"] == pytest.approx(150.0)

    assert len(products) == 1
    assert products[0]["wkn"] == "A1C1H5"
    assert products[0]["net_cashflow"] == pytest.approx(-1050.0)
    assert products[0]["profit"] == pytest.approx(150.0)


def test_infer_missing_buys_from_depotauszug_snapshot(db_path: Path, tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()

    depotauszug_pdf = incoming / "depotauszug.pdf"
    _touch_pdf(depotauszug_pdf)

    parser = StubParser(
        parsed_by_filename={},
        depotauszug_by_filename={
            "depotauszug.pdf": (
                date(2024, 9, 30),
                [
                    ParsedHolding(
                        wkn="A0HGV0",
                        isin="IE00B0M62Q58",
                        product_name="iShs-MSCI World UCITS ETF",
                        quantity=10.0,
                        snapshot_price=100.0,
                    )
                ],
            )
        },
    )
    services = _build_services(db_path, parser=parser, market_client=StubMarketClient(value=1000.0))

    ingest_stats = services.ingestion.ingest_directory(incoming)
    infer_stats = services.analytics.infer_missing_buys_from_holdings()

    assert ingest_stats["ingested"] == 1
    assert infer_stats["snapshots"] == 1
    assert infer_stats["inferred"] == 1
    assert infer_stats["errors"] == 0

    with services.session_factory() as session:
        snapshot_count = session.scalar(select(func.count()).select_from(HoldingSnapshot))
        tx = session.execute(
            select(Transaction).where(Transaction.type == TransactionType.BUY, Transaction.quantity == 10.0)
        ).scalar_one()

    assert snapshot_count == 1
    assert tx.transaction_date.isoformat() == "2024-09-30"
    assert tx.gross_amount == pytest.approx(1000.0)


def test_update_open_asset_values_converts_usd_quote_to_eur(db_path: Path, tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()

    buy_pdf = incoming / "buy.pdf"
    _touch_pdf(buy_pdf)

    parser = StubParser(
        {
            "buy.pdf": ParsedTransaction(
                tx_type=TransactionType.BUY,
                wkn="A1C1H5",
                isin="IE00B5L8K969",
                product_name="ETF Product",
                transaction_date=date(2026, 2, 2),
                quantity=1.0,
                gross_amount=100.0,
                costs=0.0,
            ),
        }
    )
    services = _build_services(
        db_path,
        parser=parser,
        market_client=StubMarketClient(value=120.0, quote_currency="USD", fx_rate=0.9),
    )

    services.ingestion.ingest_directory(incoming)
    value_stats = services.analytics.update_open_asset_values()
    assert value_stats["updated"] == 1

    with services.session_factory() as session:
        asset_value = session.execute(select(AssetValue).order_by(AssetValue.recorded_at.desc())).scalars().first()
        assert asset_value is not None
        assert asset_value.value == pytest.approx(108.0)
        assert asset_value.currency == "EUR"
        assert "fx:USD->EUR" in asset_value.source

    products = services.analytics.fetch_product_profit()
    assert products[0]["current_value"] == pytest.approx(108.0)


def test_infer_missing_buys_converts_historical_usd_to_eur(db_path: Path, tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()

    depotauszug_pdf = incoming / "depotauszug.pdf"
    _touch_pdf(depotauszug_pdf)

    parser = StubParser(
        parsed_by_filename={},
        depotauszug_by_filename={
            "depotauszug.pdf": (
                date(2024, 9, 30),
                [
                    ParsedHolding(
                        wkn="A0HGV0",
                        isin="IE00B0M62Q58",
                        product_name="iShs-MSCI World UCITS ETF",
                        quantity=2.0,
                        snapshot_price=None,
                    )
                ],
            )
        },
    )
    services = _build_services(
        db_path,
        parser=parser,
        market_client=StubMarketClient(
            value=50.0,
            historical_currency="USD",
            historical_fx_rate=0.8,
        ),
    )

    ingest_stats = services.ingestion.ingest_directory(incoming)
    infer_stats = services.analytics.infer_missing_buys_from_holdings()

    assert ingest_stats["ingested"] == 1
    assert infer_stats["inferred"] == 1
    assert infer_stats["errors"] == 0

    with services.session_factory() as session:
        tx = session.execute(
            select(Transaction).where(Transaction.type == TransactionType.BUY, Transaction.quantity == 2.0)
        ).scalar_one()

    assert tx.gross_amount == pytest.approx(80.0)


def test_update_open_asset_values_falls_back_to_avg_buy_cost_when_yahoo_fails(db_path: Path, tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()

    buy_pdf = incoming / "buy.pdf"
    _touch_pdf(buy_pdf)

    parser = StubParser(
        {
            "buy.pdf": ParsedTransaction(
                tx_type=TransactionType.BUY,
                wkn="A2PKXG",
                isin="IE00BK5BQT80",
                product_name="FTSE All-World",
                transaction_date=date(2026, 2, 2),
                quantity=10.0,
                gross_amount=1000.0,
                costs=50.0,
            ),
        }
    )
    services = _build_services(
        db_path,
        parser=parser,
        market_client=StubMarketClient(raise_on_quote=True),
    )

    services.ingestion.ingest_directory(incoming)
    value_stats = services.analytics.update_open_asset_values()
    assert value_stats["updated"] == 1
    assert value_stats["fallbacks"] == 1
    assert value_stats["errors"] == 0

    with services.session_factory() as session:
        asset_value = session.execute(select(AssetValue).order_by(AssetValue.id.desc())).scalars().first()
        assert asset_value is not None
        assert asset_value.value == pytest.approx(105.0)
        assert asset_value.currency == "EUR"
        assert asset_value.source == "fallback:avg_buy_cost_eur"

    products = services.analytics.fetch_product_profit()
    assert products[0]["wkn"] == "A2PKXG"
    assert products[0]["current_value"] == pytest.approx(1050.0)


def test_repair_known_data_issues_neutralizes_alias_and_applies_split(db_path: Path) -> None:
    services = _build_services(db_path, parser=StubParser({}), market_client=StubMarketClient())

    with services.session_factory() as session:
        p_alias = Product(wkn="A3DUN5", isin="US0494681010", name="Atlassian Corp")
        p_canonical = Product(wkn="A2ABYA", isin="GB00BZ09BD16", name="Atlassian PLC")
        p_amzn = Product(wkn="906866", isin="US0231351067", name="Amazon")
        session.add_all([p_alias, p_canonical, p_amzn])
        session.flush()

        session.add(
            Transaction(
                product_id=p_canonical.id,
                type=TransactionType.BUY,
                transaction_date=date(2021, 2, 23),
                quantity=10.0,
                gross_amount=2009.9,
                costs=9.9,
                currency="EUR",
                source_file="buy_canonical.pdf",
                source_hash="hash-canon-buy",
            )
        )
        session.add(
            Transaction(
                product_id=p_alias.id,
                type=TransactionType.BUY,
                transaction_date=date(2023, 1, 15),
                quantity=10.0,
                gross_amount=1362.77,
                costs=0.0,
                currency="EUR",
                source_file="inferred_from_depotauszug:depotauszug.pdf",
                source_hash="hash-alias-inferred-buy",
            )
        )
        session.add(
            Transaction(
                product_id=p_amzn.id,
                type=TransactionType.BUY,
                transaction_date=date(2020, 11, 30),
                quantity=1.0,
                gross_amount=2661.53,
                costs=11.53,
                currency="EUR",
                source_file="buy_amzn.pdf",
                source_hash="hash-amzn-buy",
            )
        )
        # Simulate a previously applied old split repair_service (1:10); current repair_service should adjust it to 1:20.
        session.add(
            Transaction(
                product_id=p_amzn.id,
                type=TransactionType.BUY,
                transaction_date=date(2022, 6, 6),
                quantity=9.0,
                gross_amount=0.0,
                costs=0.0,
                currency="EUR",
                source_file="repair_split:906866:10:2022-06-06:amazon_1_to_10",
                source_hash="hash-amzn-old-split",
            )
        )
        session.commit()

    stats = services.repair.repair_known_data_issues()
    assert stats["applied"] == 3
    assert stats["errors"] == 0

    with services.session_factory() as session:
        qty_expr = func.sum(
            case(
                (Transaction.type == TransactionType.BUY, Transaction.quantity),
                (Transaction.type == TransactionType.SELL, -Transaction.quantity),
                (Transaction.type == TransactionType.SPLIT, Transaction.quantity),
                else_=0.0,
            )
        )
        alias_id = session.execute(select(Product.id).where(Product.wkn == "A3DUN5")).scalar_one()
        amzn_id = session.execute(select(Product.id).where(Product.wkn == "906866")).scalar_one()
        alias_qty = float(
            session.scalar(select(func.coalesce(qty_expr, 0.0)).where(Transaction.product_id == alias_id)) or 0.0
        )
        amzn_qty = float(
            session.scalar(select(func.coalesce(qty_expr, 0.0)).where(Transaction.product_id == amzn_id)) or 0.0
        )
        split_txs = session.execute(
            select(Transaction).where(Transaction.product_id == amzn_id, Transaction.source_file.like("repair_split:%"))
        ).scalars().all()
        alias_close_tx = session.execute(
            select(Transaction).where(Transaction.product_id == alias_id,
                                      Transaction.source_file.like("repair_alias_close:%"))
        ).scalar_one()

    assert alias_qty == pytest.approx(0.0)
    assert amzn_qty == pytest.approx(20.0)
    assert len(split_txs) == 2
    assert sum(tx.quantity for tx in split_txs) == pytest.approx(19.0)
    latest_split = [tx for tx in split_txs if "amazon_1_to_20" in tx.source_file][0]
    assert latest_split.type == TransactionType.SPLIT
    assert latest_split.quantity == pytest.approx(10.0)
    assert latest_split.gross_amount == pytest.approx(0.0)
    assert alias_close_tx.type == TransactionType.SELL


def test_repair_known_data_issues_adds_nvidia_splits_and_purges_legacy_alias(db_path: Path) -> None:
    services = _build_services(db_path, parser=StubParser({}), market_client=StubMarketClient())

    with services.session_factory() as session:
        p_nvidia = Product(wkn="918422", isin="US67066G1040", name="NVIDIA")
        p_alias = Product(wkn="191842", isin=None, name="NVIDIA legacy alias")
        session.add_all([p_nvidia, p_alias])
        session.flush()

        session.add(
            Transaction(
                product_id=p_nvidia.id,
                type=TransactionType.BUY,
                transaction_date=date(2020, 1, 10),
                quantity=7.0,
                gross_amount=1000.0,
                costs=0.0,
                currency="EUR",
                bank="CONSORS",
                source_file="buy_nvidia.pdf",
                source_hash="hash-nvidia-buy",
            )
        )
        session.add(
            Transaction(
                product_id=p_alias.id,
                type=TransactionType.BUY,
                transaction_date=date(2019, 3, 29),
                quantity=10.0,
                gross_amount=21601.5,
                costs=0.0,
                currency="EUR",
                bank="CONSORS",
                source_file="inferred_from_depotauszug:incoming_pdfs/cortal_consors/QUARTALSDEPOTAUSZUG_WERTPAPIERE_448609135_dat20190329_id769447287.pdf",
                source_hash="hash-nvidia-alias-inferred",
            )
        )
        session.commit()

    stats = services.repair.repair_known_data_issues()
    assert stats["errors"] == 0

    with services.session_factory() as session:
        qty_expr = func.sum(
            case(
                (Transaction.type == TransactionType.BUY, Transaction.quantity),
                (Transaction.type == TransactionType.SELL, -Transaction.quantity),
                (Transaction.type == TransactionType.SPLIT, Transaction.quantity),
                else_=0.0,
            )
        )
        nvidia_id = session.execute(select(Product.id).where(Product.wkn == "918422")).scalar_one()
        alias_id = session.execute(select(Product.id).where(Product.wkn == "191842")).scalar_one()
        nvidia_qty = float(
            session.scalar(select(func.coalesce(qty_expr, 0.0)).where(Transaction.product_id == nvidia_id)) or 0.0
        )
        alias_qty = float(
            session.scalar(select(func.coalesce(qty_expr, 0.0)).where(Transaction.product_id == alias_id)) or 0.0
        )
        nvidia_split_rows = session.execute(
            select(Transaction).where(
                Transaction.product_id == nvidia_id,
                Transaction.source_file.like("repair_split:918422:%"),
            )
        ).scalars().all()
        alias_rows = session.execute(
            select(Transaction).where(Transaction.product_id == alias_id)
        ).scalars().all()

    assert nvidia_qty == pytest.approx(280.0)
    assert alias_qty == pytest.approx(0.0)
    assert len(nvidia_split_rows) == 2
    assert any("nvidia_4_to_1" in row.source_file for row in nvidia_split_rows)
    assert any("nvidia_10_to_1" in row.source_file for row in nvidia_split_rows)
    assert alias_rows == []


def test_repair_known_data_issues_applies_xilinx_amd_share_exchange(db_path: Path) -> None:
    services = _build_services(db_path, parser=StubParser({}), market_client=StubMarketClient())

    with services.session_factory() as session:
        p_xilinx = Product(wkn="880135", isin="US9839191015", name="Xilinx")
        p_amd = Product(wkn="863186", isin="US0079031078", name="AMD")
        session.add_all([p_xilinx, p_amd])
        session.flush()

        session.add(
            Transaction(
                product_id=p_xilinx.id,
                type=TransactionType.BUY,
                transaction_date=date(2019, 4, 26),
                quantity=10.0,
                gross_amount=1044.0,
                costs=9.95,
                currency="EUR",
                bank="CONSORS",
                source_file="xilinx_buy.pdf",
                source_hash="hash-xilinx-buy",
            )
        )
        session.add(
            Transaction(
                product_id=p_amd.id,
                type=TransactionType.BUY,
                transaction_date=date(2020, 10, 9),
                quantity=5.0,
                gross_amount=500.0,
                costs=0.0,
                currency="EUR",
                bank="CONSORS",
                source_file="amd_buy.pdf",
                source_hash="hash-amd-buy",
            )
        )
        session.commit()

    stats = services.repair.repair_known_data_issues()
    assert stats["errors"] == 0
    assert stats["applied"] == 2

    with services.session_factory() as session:
        qty_expr = func.sum(
            case(
                (Transaction.type == TransactionType.BUY, Transaction.quantity),
                (Transaction.type == TransactionType.SELL, -Transaction.quantity),
                (Transaction.type == TransactionType.SPLIT, Transaction.quantity),
                else_=0.0,
            )
        )
        amd_id = session.execute(select(Product.id).where(Product.wkn == "863186")).scalar_one()
        xilinx_id = session.execute(select(Product.id).where(Product.wkn == "880135")).scalar_one()
        amd_qty = float(
            session.scalar(select(func.coalesce(qty_expr, 0.0)).where(Transaction.product_id == amd_id)) or 0.0)
        xilinx_qty = float(
            session.scalar(select(func.coalesce(qty_expr, 0.0)).where(Transaction.product_id == xilinx_id)) or 0.0
        )
        exchange_rows = session.execute(
            select(Transaction).where(Transaction.source_file.like("repair_exchange:880135->863186:%"))
        ).scalars().all()

    assert amd_qty == pytest.approx(22.234)
    assert xilinx_qty == pytest.approx(0.0)
    assert len(exchange_rows) == 2
    assert all(row.type == TransactionType.SPLIT for row in exchange_rows)
    source_leg = [row for row in exchange_rows if row.quantity < 0][0]
    target_leg = [row for row in exchange_rows if row.quantity > 0][0]
    assert source_leg.gross_amount == pytest.approx(-1053.95)
    assert target_leg.gross_amount == pytest.approx(1053.95)


def test_repair_known_data_issues_adds_manual_missing_buy_for_etf_oil(db_path: Path) -> None:
    services = _build_services(db_path, parser=StubParser({}), market_client=StubMarketClient())

    with services.session_factory() as session:
        product = Product(wkn="A0KRKM", isin="DE000A0KRKM5", name="ETF oil legacy")
        session.add(product)
        session.flush()
        session.add(
            Transaction(
                product_id=product.id,
                type=TransactionType.SELL,
                transaction_date=date(2016, 12, 22),
                quantity=70.0,
                gross_amount=1631.07,
                costs=9.95,
                currency="EUR",
                bank="CONSORS",
                source_file="sell.pdf",
                source_hash="hash-sell-oil",
            )
        )
        session.commit()

    stats = services.repair.repair_known_data_issues()
    assert stats["errors"] == 0
    assert stats["applied"] == 1

    with services.session_factory() as session:
        product_id = session.execute(select(Product.id).where(Product.wkn == "A0KRKM")).scalar_one()
        buy_row = session.execute(
            select(Transaction).where(
                Transaction.product_id == product_id,
                Transaction.type == TransactionType.BUY,
                Transaction.source_file.like("repair_missing_buy:A0KRKM:%"),
            )
        ).scalar_one()

    assert buy_row.transaction_date == date(2016, 12, 21)
    assert buy_row.quantity == pytest.approx(70.0)
    assert buy_row.gross_amount == pytest.approx(1000.0)
    assert buy_row.costs == pytest.approx(0.0)
    assert buy_row.bank == "CONSORS"


def test_repair_known_data_issues_adds_manual_buy_even_with_existing_buys(
        db_path: Path,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    services = _build_services(db_path, parser=StubParser({}), market_client=StubMarketClient())

    with services.session_factory() as session:
        product = Product(wkn="A2PKXG", isin="IE00BK5BQT80", name="FTSE All-World")
        session.add(product)
        session.flush()
        session.add(
            Transaction(
                product_id=product.id,
                type=TransactionType.BUY,
                transaction_date=date(2024, 1, 10),
                quantity=10.0,
                gross_amount=1000.0,
                costs=0.0,
                currency="EUR",
                bank="ING",
                source_file="existing_buy.pdf",
                source_hash="hash-existing-buy",
            )
        )
        session.commit()

    monkeypatch.setattr(
        RepairService,
        "_load_repair_rules_config",
        staticmethod(
            lambda: {
                "alias_neutralizations": [],
                "split_adjustments": [],
                "share_exchange_adjustments": [],
                "manual_missing_buys": [],
                "manual_buys": [
                    {
                        "wkn": "A2PKXG",
                        "buy_date": "2025-03-29",
                        "quantity": 80.0,
                        "gross_amount": 8959.2,
                        "costs": 0.0,
                        "bank": "TRADE_REPUBLIC",
                        "label": "trade_republic_buy_2025_03_29",
                        "source_file": "manual_entry:trade_republic:A2PKXG:2025-03-29",
                    }
                ],
            }
        ),
    )

    stats_first = services.repair.repair_known_data_issues()
    assert stats_first["errors"] == 0
    assert stats_first["applied"] == 1

    stats_second = services.repair.repair_known_data_issues()
    assert stats_second["errors"] == 0
    assert stats_second["applied"] == 0

    with services.session_factory() as session:
        product_id = session.execute(select(Product.id).where(Product.wkn == "A2PKXG")).scalar_one()
        buy_rows = session.execute(
            select(Transaction).where(
                Transaction.product_id == product_id,
                Transaction.type == TransactionType.BUY,
            )
        ).scalars().all()
        manual_buy = session.execute(
            select(Transaction).where(
                Transaction.product_id == product_id,
                Transaction.type == TransactionType.BUY,
                Transaction.source_file == "manual_entry:trade_republic:A2PKXG:2025-03-29",
            )
        ).scalar_one()

    assert len(buy_rows) == 2
    assert manual_buy.transaction_date == date(2025, 3, 29)
    assert manual_buy.quantity == pytest.approx(80.0)
    assert manual_buy.gross_amount == pytest.approx(8959.2)
    assert manual_buy.costs == pytest.approx(0.0)
    assert manual_buy.bank == "TRADE_REPUBLIC"


def test_ingest_canonicalizes_alias_identifiers_to_single_product(db_path: Path, tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()

    canonical_pdf = incoming / "canonical.pdf"
    alias_pdf = incoming / "alias.pdf"
    _touch_pdf(canonical_pdf)
    _touch_pdf(alias_pdf)

    parser = StubParser(
        {
            "canonical.pdf": ParsedTransaction(
                tx_type=TransactionType.BUY,
                wkn="A2ABYA",
                isin="GB00BZ09BD16",
                product_name="Atlassian PLC",
                transaction_date=date(2021, 2, 23),
                quantity=10.0,
                gross_amount=2009.9,
                costs=9.9,
            ),
            "alias.pdf": ParsedTransaction(
                tx_type=TransactionType.ERTRAGSABRECHNUNG,
                wkn="A3DUN5",
                isin="US0494681010",
                product_name="Atlassian Corp",
                transaction_date=date(2023, 1, 15),
                quantity=0.0,
                gross_amount=5.0,
                costs=0.0,
            ),
        }
    )
    services = _build_services(db_path, parser=parser, market_client=StubMarketClient())

    stats = services.ingestion.ingest_directory(incoming)
    assert stats["ingested"] == 2

    with services.session_factory() as session:
        products = session.execute(select(Product)).scalars().all()
        tx_rows = session.execute(select(Transaction)).scalars().all()

    assert len(products) == 1
    assert products[0].wkn == "A2ABYA"
    assert products[0].isin == "GB00BZ09BD16"
    assert len(tx_rows) == 2
    assert {tx.type for tx in tx_rows} == {TransactionType.BUY, TransactionType.ERTRAGSABRECHNUNG}


def test_build_portfolio_monthly_history_stores_monthly_values(db_path: Path) -> None:
    services = _build_services(db_path, parser=StubParser({}), market_client=StubMarketClient())

    with services.session_factory() as session:
        product = Product(wkn="A2PKXG", isin="IE00BK5BQT80", name="FTSE All-World")
        session.add(product)
        session.flush()

        tx_rows = [
            Transaction(
                product_id=product.id,
                type=TransactionType.BUY,
                transaction_date=date(2024, 1, 10),
                quantity=10.0,
                gross_amount=1000.0,
                costs=0.0,
                currency="EUR",
                source_file="buy.pdf",
                source_hash="hist-buy",
            ),
            Transaction(
                product_id=product.id,
                type=TransactionType.SELL,
                transaction_date=date(2024, 2, 10),
                quantity=2.0,
                gross_amount=250.0,
                costs=0.0,
                currency="EUR",
                source_file="sell.pdf",
                source_hash="hist-sell",
            ),
            Transaction(
                product_id=product.id,
                type=TransactionType.ERTRAGSABRECHNUNG,
                transaction_date=date(2024, 2, 15),
                quantity=0.0,
                gross_amount=20.0,
                costs=0.0,
                currency="EUR",
                source_file="ertrag.pdf",
                source_hash="hist-ertrag",
            ),
        ]
        session.add_all(tx_rows)
        session.add(
            AssetValue(
                product_id=product.id,
                recorded_at=datetime(2024, 1, 31, 23, 59, 59),
                value=110.0,
                currency="EUR",
                source="jan-close",
            )
        )
        session.add(
            AssetValue(
                product_id=product.id,
                recorded_at=datetime(2024, 2, 29, 23, 59, 59),
                value=120.0,
                currency="EUR",
                source="feb-close",
            )
        )
        session.commit()

    stats = services.analytics.build_portfolio_monthly_history(
        start_month=date(2024, 1, 1), end_month=date(2024, 2, 1)
    )
    assert stats["months"] == 2
    assert stats["created"] == 2
    assert stats["errors"] == 0

    with services.session_factory() as session:
        rows = session.execute(
            select(PortfolioMonthlyHistory)
            .where(PortfolioMonthlyHistory.month_date.in_([date(2024, 1, 1), date(2024, 2, 1)]))
            .order_by(PortfolioMonthlyHistory.month_date)
        ).scalars().all()

    assert len(rows) == 2
    jan, feb = rows
    assert jan.month_date.isoformat() == "2024-01-01"
    assert jan.invested_amount_eur == pytest.approx(1000.0)
    assert jan.portfolio_value_eur == pytest.approx(1100.0)
    assert jan.portfolio_profit_eur == pytest.approx(100.0)

    assert feb.month_date.isoformat() == "2024-02-01"
    assert feb.invested_amount_eur == pytest.approx(800.0)
    assert feb.portfolio_value_eur == pytest.approx(960.0)
    assert feb.portfolio_profit_eur == pytest.approx(230.0)


def test_backfill_monthly_market_values_from_yahoo_creates_and_updates_rows(db_path: Path) -> None:
    services = _build_services(
        db_path,
        parser=StubParser({}),
        market_client=StubMarketClient(value=100.0, historical_currency="USD", historical_fx_rate=0.9),
    )

    with services.session_factory() as session:
        product = Product(wkn="A2PKXG", isin="IE00BK5BQT80", name="FTSE All-World")
        session.add(product)
        session.flush()
        session.add(
            Transaction(
                product_id=product.id,
                type=TransactionType.BUY,
                transaction_date=date(2024, 1, 10),
                quantity=1.0,
                gross_amount=100.0,
                costs=0.0,
                currency="EUR",
                source_file="buy.pdf",
                source_hash="backfill-buy",
            )
        )
        session.commit()

    first = services.analytics.backfill_monthly_market_values_from_yahoo(
        start_month=date(2024, 1, 1), end_month=date(2024, 2, 1)
    )
    assert first["months"] == 2
    assert first["created"] == 2
    assert first["updated"] == 0
    assert first["errors"] == 0

    with services.session_factory() as session:
        rows = session.execute(
            select(AssetValue)
            .where(AssetValue.source.like("yahoo_hist_month:%"))
            .order_by(AssetValue.source)
        ).scalars().all()
    assert len(rows) == 2
    for row in rows:
        assert row.currency == "EUR"
        assert row.value == pytest.approx(90.0)

    second = services.analytics.backfill_monthly_market_values_from_yahoo(
        start_month=date(2024, 1, 1), end_month=date(2024, 2, 1)
    )
    assert second["months"] == 2
    assert second["created"] == 0
    assert second["updated"] == 2
    assert second["errors"] == 0
