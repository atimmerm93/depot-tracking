from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Protocol

from banking_app.core.models import ParsedHolding, ParsedTransaction
from .consors_pdf_parser import ConsorsPdfTransactionParser
from .general_pdf_parser import GeneralPdfParser
from .ing_pdf_parser import INGPdfParser
from .trade_republic_pdf_parser import TradeRepublicPdfTransactionParser

SUPPORTED_BANKS = ("ing", "consors", "trade_republic")


class BankPdfParser(Protocol):
    parser_version: str

    def parse(self, pdf_path: str | Path) -> ParsedTransaction: ...

    def parse_depotauszug_holdings(self, pdf_path: str | Path) -> tuple[date, list[ParsedHolding]]: ...


@dataclass
class ParserFactory:
    general_pdf_parser: GeneralPdfParser = field(default_factory=GeneralPdfParser)

    def build_ing_parser(self) -> INGPdfParser:
        return INGPdfParser(general_pdf_parser=self.general_pdf_parser)

    def build_consors_parser(self) -> ConsorsPdfTransactionParser:
        return ConsorsPdfTransactionParser(general_pdf_parser=self.general_pdf_parser)

    def build_trade_republic_parser(self) -> TradeRepublicPdfTransactionParser:
        return TradeRepublicPdfTransactionParser(general_pdf_parser=self.general_pdf_parser)

    def build_parser(self, bank: str) -> BankPdfParser:
        bank_normalized = (bank or "").strip().lower()
        if bank_normalized not in SUPPORTED_BANKS:
            raise ValueError(f"Unsupported bank '{bank}'. Expected one of: {', '.join(SUPPORTED_BANKS)}")

        if bank_normalized == "ing":
            return self.build_ing_parser()
        if bank_normalized == "consors":
            return self.build_consors_parser()
        return self.build_trade_republic_parser()
