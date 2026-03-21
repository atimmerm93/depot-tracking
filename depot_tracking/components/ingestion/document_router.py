from datetime import date
from pathlib import Path

from depot_tracking.components.shared import BankClassifier
from depot_tracking.config import ParserConfig
from depot_tracking.core.models import ParsedHolding, ParsedTransaction
from depot_tracking.core.parser_errors import PdfParseError, UnsupportedPdfDocument
from .models import DocumentParseFailure, HoldingsDocument, IgnoredDocument, ParsedDocument, TransactionDocument
from .parsing.parser_factory import BankPdfParser, ParserFactory, SUPPORTED_BANKS


class DocumentRouter:
    def __init__(
            self,
            *,
            parser_factory: ParserFactory,
            parser_config: ParserConfig,
            bank_classifier: BankClassifier,
    ) -> None:
        self._parser_config = parser_config
        self._bank_classifier = bank_classifier
        self._parsers_by_bank: dict[str, BankPdfParser] = {
            bank: parser_factory.build_parser(bank) for bank in SUPPORTED_BANKS
        }
        supported_hints = {"auto", *SUPPORTED_BANKS}
        if self._parser_config.bank_hint not in supported_hints:
            raise ValueError(
                f"Unsupported parser hint '{self._parser_config.bank_hint}'. "
                f"Expected one of: {', '.join(sorted(supported_hints))}"
            )

    def parse_document(self, pdf_file: str | Path) -> ParsedDocument:
        file_path = Path(pdf_file)
        try:
            tx, parser_used = self.parse_transaction(file_path)
            return TransactionDocument(file_path=file_path, transaction=tx, parser=parser_used)
        except UnsupportedPdfDocument as exc:
            try:
                snapshot_date, holdings, parser_used = self.parse_depotauszug_holdings(file_path)
            except UnsupportedPdfDocument:
                return IgnoredDocument(file_path=file_path, reason=str(exc))
            except (PdfParseError, ValueError) as holdings_exc:
                return DocumentParseFailure(file_path=file_path, error=holdings_exc)
            return HoldingsDocument(
                file_path=file_path,
                snapshot_date=snapshot_date,
                holdings=holdings,
                parser=parser_used,
            )
        except (PdfParseError, ValueError) as exc:
            return DocumentParseFailure(file_path=file_path, error=exc)

    def parse_transaction(self, pdf_file: str | Path) -> tuple[ParsedTransaction, BankPdfParser]:
        parser_order, hinted = self._ordered_parsers(pdf_file)
        parse_errors: list[PdfParseError] = []
        for parser in parser_order:
            try:
                return parser.parse(pdf_file), parser
            except UnsupportedPdfDocument:
                continue
            except PdfParseError as exc:
                if parser is hinted:
                    raise
                parse_errors.append(exc)
        if parse_errors:
            raise parse_errors[0]
        raise UnsupportedPdfDocument(f"Unsupported PDF document type: {Path(pdf_file).name}")

    def parse_depotauszug_holdings(self, pdf_file: str | Path) -> tuple[date, list[ParsedHolding], BankPdfParser]:
        parser_order, hinted = self._ordered_parsers(pdf_file)
        parse_errors: list[PdfParseError] = []
        for parser in parser_order:
            try:
                snapshot_date, holdings = parser.parse_depotauszug_holdings(pdf_file)
                return snapshot_date, holdings, parser
            except UnsupportedPdfDocument:
                continue
            except PdfParseError as exc:
                if parser is hinted:
                    raise
                parse_errors.append(exc)
        if parse_errors:
            raise parse_errors[0]
        raise UnsupportedPdfDocument(f"Not a supported Depotauszug document: {Path(pdf_file).name}")

    def _ordered_parsers(self, pdf_path: str | Path) -> tuple[tuple[BankPdfParser, ...], BankPdfParser | None]:
        hinted_parser: BankPdfParser | None = None
        hint = self._parser_config.bank_hint
        if hint in SUPPORTED_BANKS:
            parser = self._parsers_by_bank[hint]
            return (parser,), parser

        ing = self._parsers_by_bank["ing"]
        consors = self._parsers_by_bank["consors"]
        trade_republic = self._parsers_by_bank["trade_republic"]
        preferred_bank = self._bank_classifier.preferred_parser_bank(pdf_path)

        if preferred_bank == "ing":
            hinted_parser = ing
            return (ing, consors, trade_republic), hinted_parser
        if preferred_bank == "consors":
            hinted_parser = consors
            return (consors, ing, trade_republic), hinted_parser
        if preferred_bank == "trade_republic":
            hinted_parser = trade_republic
            return (trade_republic, ing, consors), hinted_parser
        return (ing, consors, trade_republic), hinted_parser
