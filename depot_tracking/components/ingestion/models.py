from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from depot_tracking.core.models import ParsedHolding, ParsedTransaction
from depot_tracking.core.parser_errors import PdfParseError
from .parsing.parser_factory import BankPdfParser


@dataclass(frozen=True)
class TransactionDocument:
    file_path: Path
    transaction: ParsedTransaction
    parser: BankPdfParser


@dataclass(frozen=True)
class HoldingsDocument:
    file_path: Path
    snapshot_date: date
    holdings: list[ParsedHolding]
    parser: BankPdfParser


@dataclass(frozen=True)
class IgnoredDocument:
    file_path: Path
    reason: str
    parser_version: str = "router-v1-ignored"


@dataclass(frozen=True)
class DocumentParseFailure:
    file_path: Path
    error: PdfParseError | ValueError


ParsedDocument = TransactionDocument | HoldingsDocument | IgnoredDocument | DocumentParseFailure


@dataclass(frozen=True)
class IngestionFileResult:
    file_path: Path
    ingested: int = 0
    skipped: int = 0
    errors: int = 0
    log_message: str | None = None

    def apply_to(self, stats: dict[str, int]) -> None:
        stats["seen"] += 1
        stats["ingested"] += self.ingested
        stats["skipped"] += self.skipped
        stats["errors"] += self.errors
