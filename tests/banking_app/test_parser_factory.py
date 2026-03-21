from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from python_di_application.dependency import Dependency, DependencyInstance
from python_di_application.di_container import DIContainer

from depot_tracking.components.ingestion import DocumentRouter
from depot_tracking.components.ingestion import IngestionService
from depot_tracking.components.ingestion import IngestionStore
from depot_tracking.components.ingestion.parsing.consors_pdf_parser import ConsorsPdfTransactionParser
from depot_tracking.components.ingestion.parsing.general_pdf_parser import GeneralPdfParser
from depot_tracking.components.ingestion.parsing.ing_pdf_parser import INGPdfParser
from depot_tracking.components.ingestion.parsing.parser_factory import ParserFactory
from depot_tracking.components.ingestion.parsing.trade_republic_pdf_parser import TradeRepublicPdfTransactionParser
from depot_tracking.components.shared import BankClassifier, SourceDocumentNormalizer
from depot_tracking.config import ParserConfig
from depot_tracking.core.models import ParsedHolding, ParsedTransaction, TransactionType
from depot_tracking.core.parser_errors import PdfParseError, UnsupportedPdfDocument


@dataclass
class _StubBankParser:
    parser_version: str
    parse_result: ParsedTransaction | None = None
    holdings_result: tuple[date, list[ParsedHolding]] | None = None
    parse_error: Exception | None = None
    holdings_error: Exception | None = None

    def parse(self, _path: str | Path) -> ParsedTransaction:
        if self.parse_error is not None:
            raise self.parse_error
        if self.parse_result is None:
            raise UnsupportedPdfDocument("unsupported")
        return self.parse_result

    def parse_depotauszug_holdings(self, _path: str | Path) -> tuple[date, list[ParsedHolding]]:
        if self.holdings_error is not None:
            raise self.holdings_error
        if self.holdings_result is None:
            raise UnsupportedPdfDocument("unsupported")
        return self.holdings_result


class _StubParserFactory:
    def __init__(self, *, ing: _StubBankParser, consors: _StubBankParser, trade_republic: _StubBankParser) -> None:
        self._parsers = {
            "ing": ing,
            "consors": consors,
            "trade_republic": trade_republic,
        }

    def build_parser(self, bank: str) -> _StubBankParser:
        return self._parsers[bank]


def _build_ingestion_service(factory: _StubParserFactory, *, parser_bank_hint: str = "auto") -> IngestionService:
    container = DIContainer()
    container.register_dependencies(
        [
            Dependency(dependency_type=BankClassifier),
            Dependency(dependency_type=SourceDocumentNormalizer),
            Dependency(dependency_type=DocumentRouter),
            Dependency(dependency_type=IngestionService),
        ]
    )
    container.register_instances(
        [
            DependencyInstance(factory, dependency_interface=ParserFactory),
            DependencyInstance(ParserConfig(bank_hint=parser_bank_hint)),
            DependencyInstance(create_autospec(IngestionStore, instance=True), dependency_interface=IngestionStore),
        ]
    )
    return container.resolve_dependency(IngestionService)


class TestParserFactory(unittest.TestCase):

    def setUp(self) -> None:
        self.general_parser = GeneralPdfParser()
        self.factory = ParserFactory(general_pdf_parser=self.general_parser)

    def test_build_parser_for_explicit_bank(self) -> None:
        assert isinstance(self.factory.build_parser("ing"), INGPdfParser)
        assert isinstance(self.factory.build_parser("consors"), ConsorsPdfTransactionParser)
        assert isinstance(self.factory.build_parser("trade_republic"), TradeRepublicPdfTransactionParser)

    def test_build_parser_rejects_auto(self) -> None:
        with pytest.raises(ValueError):
            self.factory.build_parser("auto")

    def test_build_parser_rejects_unknown_bank(self) -> None:
        with pytest.raises(ValueError):
            self.factory.build_parser("unknown")

    def test_parser_factory_injects_shared_general_pdf_parser(self) -> None:
        ing_parser = self.factory.build_ing_parser()
        consors_parser = self.factory.build_consors_parser()
        trade_republic_parser = self.factory.build_trade_republic_parser()

        assert ing_parser._general_pdf_parser is self.general_parser  # type: ignore[attr-defined]
        assert consors_parser._general_pdf_parser is self.general_parser  # type: ignore[attr-defined]
        assert trade_republic_parser._general_pdf_parser is self.general_parser  # type: ignore[attr-defined]

    def test_ingestion_service_auto_routing_falls_back_to_consors_for_consors_filename(self) -> None:
        expected = ParsedTransaction(
            tx_type=TransactionType.BUY,
            wkn="A2PLS9",
            isin="IE00BK5BQV03",
            product_name="ETF",
            transaction_date=date(2025, 4, 9),
            quantity=1.0,
            gross_amount=100.0,
            costs=0.0,
        )
        factory = _StubParserFactory(
            ing=_StubBankParser(parser_version="ing", parse_error=UnsupportedPdfDocument("unsupported")),
            consors=_StubBankParser(parser_version="consors", parse_result=expected),
            trade_republic=_StubBankParser(parser_version="tr", parse_error=UnsupportedPdfDocument("unsupported")),
        )
        service = _build_ingestion_service(factory)

        tx, used_parser = service._parse_transaction(Path("KAUF_448609135_ord1_001_wknA2PLS9_dat20250409_id1.pdf"))

        assert tx == expected
        assert used_parser is factory.build_parser("consors")

    def test_ingestion_service_auto_routing_keeps_hinted_parser_error(self) -> None:
        factory = _StubParserFactory(
            ing=_StubBankParser(parser_version="ing",
                                parse_error=PdfParseError("ing parse failed")),
            consors=_StubBankParser(
                parser_version="consors",
                parse_result=ParsedTransaction(
                    tx_type=TransactionType.BUY,
                    wkn="A2PLS9",
                    isin="IE00BK5BQV03",
                    product_name="ETF",
                    transaction_date=date(2025, 4, 9),
                    quantity=1.0,
                    gross_amount=100.0,
                    costs=0.0,
                ),
            ),
            trade_republic=_StubBankParser(parser_version="tr", parse_error=UnsupportedPdfDocument("unsupported")),
        )
        service = _build_ingestion_service(factory)

        with pytest.raises(PdfParseError):
            service._parse_transaction(Path("Direkt_Depot_8013529518_Abrechnung_Kauf_sample.pdf"))

    def test_ingestion_service_auto_routing_falls_back_without_filename_hint(self) -> None:
        expected = ParsedTransaction(
            tx_type=TransactionType.SELL,
            wkn="863186",
            isin="US0079031078",
            product_name="AMD",
            transaction_date=date(2025, 10, 29),
            quantity=1.0,
            gross_amount=100.0,
            costs=0.0,
        )
        factory = _StubParserFactory(
            ing=_StubBankParser(parser_version="ing", parse_error=PdfParseError("ing parse failed")),
            consors=_StubBankParser(parser_version="consors", parse_result=expected),
            trade_republic=_StubBankParser(parser_version="tr", parse_error=UnsupportedPdfDocument("unsupported")),
        )
        service = _build_ingestion_service(factory)

        tx, used_parser = service._parse_transaction(Path("mystery_filename.pdf"))

        assert tx == expected
        assert used_parser is factory.build_parser("consors")
