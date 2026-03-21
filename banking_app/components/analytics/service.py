import hashlib
from datetime import date, datetime
from typing import Any

import requests
from di_unit_of_work.session_provider import SessionProvider
from di_unit_of_work.transactional_decorator import transactional
from sqlalchemy import text

from ...config import ParserConfig
from ...core.models import TransactionType
from ..data_operations.asset_value_data_operations import AssetValueDataOperations
from ..data_operations.asset_value_repository import AssetValueRepository
from ..data_operations.holding_snapshot_repository import HoldingSnapshotRepository
from ..data_operations.models import (
    AssetValueWriteModel,
    PortfolioMonthlyHistoryWriteModel,
    ProductTickerUpdateModel,
    TransactionWriteModel,
)
from ..data_operations.portfolio_monthly_history_data_operations import PortfolioMonthlyHistoryDataOperations
from ..data_operations.product_data_operations import ProductDataOperations
from ..data_operations.product_repository import ProductRepository
from ..data_operations.transaction_data_operations import TransactionDataOperations
from ..data_operations.transaction_repository import TransactionRepository
from ..market.market_data import MarketDataError, YahooMarketDataClient
from ..shared import BankClassifier, CalendarMonthService, IdentifierCanonicalizer


class AnalyticsService:
    def __init__(
            self,
            *,
            market_client: YahooMarketDataClient,
            parser_config: ParserConfig,
            session_provider: SessionProvider,
            snapshot_repo: HoldingSnapshotRepository,
            product_repo: ProductRepository,
            product_ops: ProductDataOperations,
            tx_repo: TransactionRepository,
            tx_ops: TransactionDataOperations,
            asset_repo: AssetValueRepository,
            asset_ops: AssetValueDataOperations,
            monthly_ops: PortfolioMonthlyHistoryDataOperations,
            bank_classifier: BankClassifier,
            month_service: CalendarMonthService,
            identifier_canonicalizer: IdentifierCanonicalizer,
    ) -> None:
        self._market = market_client
        self._parser_config = parser_config
        self._session_provider = session_provider
        self._snapshot_repo = snapshot_repo
        self._product_repo = product_repo
        self._product_ops = product_ops
        self._tx_repo = tx_repo
        self._tx_ops = tx_ops
        self._asset_repo = asset_repo
        self._asset_ops = asset_ops
        self._monthly_ops = monthly_ops
        self._bank_classifier = bank_classifier
        self._month_service = month_service
        self._identifier_canonicalizer = identifier_canonicalizer

    @transactional
    def infer_missing_buys_from_holdings(self) -> dict[str, int]:
        stats = {"snapshots": 0, "inferred": 0, "skipped": 0, "errors": 0}
        snapshots = self._snapshot_repo.list_earliest_per_product()
        stats["snapshots"] = len(snapshots)

        for snapshot in snapshots:
            product = self._product_repo.get_by_id(snapshot.product_id)
            if product is None:
                stats["errors"] += 1
                continue
            if self._identifier_canonicalizer.is_legacy_consors_alias_wkn(product.wkn):
                stats["skipped"] += 1
                continue

            existing_quantity = self._tx_repo.sum_signed_quantity_until(
                product_id=snapshot.product_id,
                until_date=snapshot.snapshot_date,
            )
            missing_quantity = round(snapshot.quantity - existing_quantity, 8)
            if missing_quantity <= 0:
                stats["skipped"] += 1
                continue

            inference_key = (
                f"inferred-buy|{snapshot.product_id}|{snapshot.snapshot_date.isoformat()}|"
                f"{missing_quantity:.8f}|{snapshot.source_hash}"
            )
            inferred_hash = hashlib.sha256(inference_key.encode("utf-8")).hexdigest()
            if self._tx_repo.exists_by_source_hash(inferred_hash):
                stats["skipped"] += 1
                continue

            symbol = product.ticker
            try:
                symbol = self._market.resolve_symbol(
                    wkn=product.wkn,
                    isin=product.isin,
                    name=product.name,
                    ticker=product.ticker,
                )
                historical_quote = self._market.fetch_historical_quote(symbol, snapshot.snapshot_date)
                approx_price = self._to_eur_historical_price(
                    value=historical_quote.value,
                    currency=historical_quote.currency,
                    on_date=snapshot.snapshot_date,
                )
                price_source = "market_eur" if historical_quote.currency.upper() == "EUR" else "market_fx_eur"
            except (MarketDataError, ValueError, requests.RequestException) as exc:
                if snapshot.snapshot_price is not None and snapshot.snapshot_price > 0:
                    approx_price = float(snapshot.snapshot_price)
                    price_source = "depotauszug_fallback"
                    print(
                        f"[INFER][WARN] Using Depotauszug price fallback for {product.wkn} {snapshot.snapshot_date}: {exc}"
                    )
                else:
                    stats["errors"] += 1
                    print(f"[INFER][ERROR] {product.wkn} {snapshot.snapshot_date}: {exc}")
                    continue

            if symbol and product.ticker != symbol:
                self._product_ops.update_ticker(ProductTickerUpdateModel(product_id=product.id, ticker=symbol))

            if snapshot.snapshot_price is not None and snapshot.snapshot_price > 0:
                ratio = approx_price / snapshot.snapshot_price if snapshot.snapshot_price else 1.0
                if ratio > 3.0 or ratio < (1 / 3.0):
                    approx_price = float(snapshot.snapshot_price)
                    price_source = "depotauszug_sanity"

            gross_amount = round(missing_quantity * approx_price, 2)
            bank = self._bank_classifier.infer_bank_from_file_path(
                snapshot.source_file,
                parser_bank_hint=self._parser_config.bank_hint,
            )
            self._tx_ops.create(
                TransactionWriteModel(
                    product_id=product.id,
                    type=TransactionType.BUY,
                    transaction_date=snapshot.snapshot_date,
                    quantity=missing_quantity,
                    gross_amount=gross_amount,
                    costs=0.0,
                    currency="EUR",
                    bank=bank,
                    source_file=f"inferred_from_depotauszug:{snapshot.source_file}",
                    source_hash=inferred_hash,
                )
            )
            stats["inferred"] += 1
            print(
                f"[INFER][OK] WKN={product.wkn} qty={missing_quantity} "
                f"date={snapshot.snapshot_date.isoformat()} approx_price={approx_price:.4f} source={price_source}"
            )

        return stats

    @transactional
    def update_open_asset_values(self) -> dict[str, int]:
        stats = {"positions": 0, "updated": 0, "errors": 0, "fallbacks": 0}

        positions = self._tx_repo.list_open_positions()
        stats["positions"] = len(positions)

        for product, _quantity in positions:
            quote = None
            try:
                quote = self._market.fetch_quote(
                    wkn=product.wkn,
                    isin=product.isin,
                    name=product.name,
                    ticker=product.ticker,
                )
                value_eur = self._to_eur_price(value=quote.value, currency=quote.currency)
                source = f"yahoo:{quote.symbol}"
                if quote.currency.upper() != "EUR":
                    source = f"{source}|fx:{quote.currency}->EUR"
                print(
                    f"[VALUES][OK] {product.wkn}: {quote.value:.6f} {quote.currency} -> {value_eur:.6f} EUR ({quote.symbol})"
                )
            except (MarketDataError, ValueError, requests.RequestException) as exc:
                fallback_value = self._fallback_cost_per_unit_eur(self._tx_repo, product_id=product.id)
                if fallback_value is None:
                    stats["errors"] += 1
                    print(f"[VALUES][ERROR] {product.wkn}: {exc}")
                    continue
                value_eur = fallback_value
                source = "fallback:avg_buy_cost_eur"
                stats["fallbacks"] += 1
                print(
                    f"[VALUES][WARN] {product.wkn}: Yahoo unavailable ({exc}); using avg buy cost {value_eur:.6f} EUR"
                )

            self._asset_ops.create(
                AssetValueWriteModel(
                    product_id=product.id,
                    value=value_eur,
                    currency="EUR",
                    source=source,
                )
            )

            if quote is not None and product.ticker != quote.symbol:
                self._product_ops.update_ticker(ProductTickerUpdateModel(product_id=product.id, ticker=quote.symbol))

            stats["updated"] += 1

        return stats

    @transactional
    def fetch_current_profit(self) -> dict[str, Any]:
        row = self._session_provider.get_session().execute(text("SELECT * FROM v_current_profit")).mappings().first()
        if row is None:
            return {"total_profit": 0.0, "current_portfolio_value": 0.0, "net_cashflow": 0.0}
        return dict(row)

    @transactional
    def fetch_product_profit(self) -> list[dict[str, Any]]:
        rows = self._session_provider.get_session().execute(
            text(
                """
                SELECT product_id,
                       wkn,
                       isin,
                       name,
                       ticker,
                       quantity_open,
                       invested_eur,
                       returned_eur,
                       net_cashflow,
                       current_value,
                       profit,
                       latest_value_timestamp
                FROM v_product_profit
                ORDER BY profit DESC
                """
            )
        ).mappings().all()
        return [dict(row) for row in rows]

    @transactional
    def build_portfolio_monthly_history(
            self,
            *,
            start_month: date | None = None,
            end_month: date | None = None,
    ) -> dict[str, int]:
        stats = {"months": 0, "created": 0, "updated": 0, "skipped": 0, "errors": 0}

        first_tx_date = self._tx_repo.get_first_transaction_date()
        if first_tx_date is None:
            return stats

        first_month = self._month_service.month_start(first_tx_date)
        current_month = self._month_service.month_start(date.today())
        start = self._month_service.month_start(start_month) if start_month else first_month
        end = self._month_service.month_start(end_month) if end_month else current_month

        if end < start:
            return stats

        month = start
        while month <= end:
            stats["months"] += 1
            month_end_date = self._month_service.month_end(month)
            if month_end_date > date.today():
                month_end_date = date.today()

            invested, portfolio_value, portfolio_profit = self._calculate_portfolio_snapshot_as_of(
                self._tx_repo,
                self._asset_repo,
                month_end_date,
            )

            _row, created = self._monthly_ops.upsert(
                PortfolioMonthlyHistoryWriteModel(
                    month_date=month,
                    month_end_date=month_end_date,
                    invested_amount_eur=round(invested, 2),
                    portfolio_value_eur=round(portfolio_value, 2),
                    portfolio_profit_eur=round(portfolio_profit, 2),
                    source="computed",
                )
            )
            if created:
                stats["created"] += 1
            else:
                stats["updated"] += 1

            month = self._month_service.next_month(month)

        return stats

    @transactional
    def backfill_monthly_market_values_from_yahoo(
            self,
            *,
            start_month: date | None = None,
            end_month: date | None = None,
    ) -> dict[str, int]:
        stats = {"months": 0, "positions": 0, "created": 0, "updated": 0, "skipped": 0, "errors": 0}

        first_tx_date = self._tx_repo.get_first_transaction_date()
        if first_tx_date is None:
            return stats

        first_month = self._month_service.month_start(first_tx_date)
        current_month = self._month_service.month_start(date.today())
        start = self._month_service.month_start(start_month) if start_month else first_month
        end = self._month_service.month_start(end_month) if end_month else current_month

        if end < start:
            return stats

        month = start
        while month <= end:
            stats["months"] += 1
            month_end_date = self._month_service.month_end(month)
            if month_end_date > date.today():
                month_end_date = date.today()

            open_positions = self._tx_repo.list_open_positions(as_of=month_end_date)
            month_key = month.strftime("%Y-%m")
            recorded_at = datetime.combine(month_end_date, datetime.max.time()).replace(microsecond=0)
            for product, _qty in open_positions:
                stats["positions"] += 1
                try:
                    symbol = self._market.resolve_symbol(
                        wkn=product.wkn,
                        isin=product.isin,
                        name=product.name,
                        ticker=product.ticker,
                    )
                    quote = self._market.fetch_historical_quote(symbol, month_end_date)
                    value_eur = self._to_eur_historical_price(
                        value=quote.value,
                        currency=quote.currency,
                        on_date=month_end_date,
                    )
                except (MarketDataError, ValueError, requests.RequestException) as exc:
                    stats["errors"] += 1
                    print(f"[MONTHLY][ERROR] {product.wkn} {month_key}: {exc}")
                    continue

                source = f"yahoo_hist_month:{month_key}:{symbol}"
                _row, created = self._asset_ops.upsert_by_product_and_source(
                    AssetValueWriteModel(
                        product_id=product.id,
                        recorded_at=recorded_at,
                        value=float(value_eur),
                        currency="EUR",
                        source=source,
                    )
                )
                if created:
                    stats["created"] += 1
                else:
                    stats["updated"] += 1

                if product.ticker != symbol:
                    self._product_ops.update_ticker(ProductTickerUpdateModel(product_id=product.id, ticker=symbol))

            month = self._month_service.next_month(month)

        return stats

    def _to_eur_price(self, *, value: float, currency: str) -> float:
        code = (currency or "EUR").upper()
        if code == "EUR":
            return float(value)

        fx = self._market.fetch_fx_rate(base_currency=code, quote_currency="EUR")
        return float(value) * float(fx)

    def _to_eur_historical_price(self, *, value: float, currency: str, on_date: date) -> float:
        code = (currency or "EUR").upper()
        if code == "EUR":
            return float(value)

        fx = self._market.fetch_historical_fx_rate(
            base_currency=code,
            quote_currency="EUR",
            on_date=on_date,
        )
        return float(value) * float(fx)

    @staticmethod
    def _calculate_portfolio_snapshot_as_of(
            tx_repo: TransactionRepository,
            asset_repo: AssetValueRepository,
            as_of: date,
    ) -> tuple[float, float, float]:
        net_cashflow = tx_repo.get_net_cashflow_until(as_of=as_of)
        open_positions = tx_repo.list_open_positions(as_of=as_of)

        invested_amount = 0.0
        portfolio_value = 0.0
        as_of_cutoff = datetime.combine(as_of, datetime.max.time())

        for product, quantity_open in open_positions:
            quantity = float(quantity_open or 0.0)
            if quantity <= 0:
                continue

            buy_total, buy_qty = tx_repo.get_buy_basis(product_id=product.id, until_date=as_of)
            avg_buy = (buy_total / buy_qty) if buy_qty > 0 else 0.0
            invested_amount += quantity * avg_buy

            latest_value = asset_repo.get_latest_eur_value_as_of(product_id=product.id, as_of=as_of_cutoff)
            unit_value = float(latest_value) if latest_value is not None else avg_buy
            portfolio_value += quantity * unit_value

        portfolio_profit = net_cashflow + portfolio_value
        return invested_amount, portfolio_value, portfolio_profit

    @staticmethod
    def _fallback_cost_per_unit_eur(tx_repo: TransactionRepository, *, product_id: int) -> float | None:
        invested_total, buy_qty_total = tx_repo.get_buy_basis(product_id=product_id)
        if buy_qty_total <= 0:
            return None
        return invested_total / buy_qty_total
