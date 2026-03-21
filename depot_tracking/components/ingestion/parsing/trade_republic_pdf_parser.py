from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from pathlib import Path

from depot_tracking.core.models import ParsedHolding, ParsedTransaction
from depot_tracking.core.parser_errors import PdfParseError, UnsupportedPdfDocument
from .general_pdf_parser import GeneralPdfParser

AMOUNT_PATTERN = r"\d{1,3}(?:\.\d{3})*,\d+|\d+,\d+"
ISIN_TO_WKN = {
    # Known portfolio identifiers to keep product IDs stable across banks.
    "IE00BK5BQT80": "A2PKXG",
}


class TradeRepublicPdfTransactionParser:
    parser_version = "trade-republic-v1"

    def __init__(self, general_pdf_parser: GeneralPdfParser | None = None) -> None:
        self._general_pdf_parser = general_pdf_parser or GeneralPdfParser()

    def parse(self, pdf_path: str | Path) -> ParsedTransaction:
        path = Path(pdf_path)
        text = self._extract_text(path)
        if self._is_depotauszug_document(text, path):
            raise UnsupportedPdfDocument(f"Trade Republic Depotauszug document: {path.name}")
        if self._is_trade_republic_document(text, path):
            raise UnsupportedPdfDocument(f"Unsupported Trade Republic PDF document type: {path.name}")
        raise UnsupportedPdfDocument(f"Unsupported PDF document type: {path.name}")

    def parse_depotauszug_holdings(self, pdf_path: str | Path) -> tuple[date, list[ParsedHolding]]:
        path = Path(pdf_path)
        text = self._extract_text(path)
        if not self._is_depotauszug_document(text, path):
            raise UnsupportedPdfDocument(f"Not a Trade Republic Depotauszug document: {path.name}")

        snapshot_date = self._parse_snapshot_date(text, path)
        lines = [self._normalize_space(line) for line in text.splitlines() if self._normalize_space(line)]
        holdings: list[ParsedHolding] = []
        seen: set[tuple[str, float]] = set()

        idx = 0
        while idx < len(lines):
            quantity_match = re.match(
                rf"^(?P<qty>(?:{AMOUNT_PATTERN}))\s*Stk\.?\s*(?P<name>.+)$",
                lines[idx],
                flags=re.IGNORECASE,
            )
            if quantity_match is None:
                idx += 1
                continue

            quantity = abs(self._parse_european_number(quantity_match.group("qty")))
            block_end = self._find_block_end(lines, idx + 1)
            block = lines[idx:block_end]
            isin_idx, isin = self._find_isin(block)
            if isin is None:
                idx = block_end
                continue

            name_parts = [self._normalize_space(quantity_match.group("name"))]
            for candidate in block[1:isin_idx]:
                if self._is_name_noise_line(candidate):
                    continue
                name_parts.append(candidate)
            product_name = self._normalize_space(" ".join(part for part in name_parts if part))[
                :220] if name_parts else None

            snapshot_price = self._find_snapshot_price(block[isin_idx + 1:], path=path)
            wkn = self._resolve_wkn(isin)
            key = (wkn, round(quantity, 8))
            if key in seen:
                idx = block_end
                continue

            seen.add(key)
            holdings.append(
                ParsedHolding(
                    wkn=wkn,
                    isin=isin,
                    product_name=product_name,
                    quantity=quantity,
                    snapshot_price=snapshot_price,
                )
            )
            idx = block_end

        if not holdings:
            raise PdfParseError(f"Could not parse holdings from Trade Republic Depotauszug: {path.name}")
        return snapshot_date, holdings

    def _extract_text(self, path: Path) -> str:
        return self._general_pdf_parser.extract_text(path)

    def _is_trade_republic_document(self, text: str, path: Path) -> bool:
        lowered = f"{text}\n{path.name}".lower()
        return "trade republic" in lowered or "traderepublic" in lowered

    def _is_depotauszug_document(self, text: str, path: Path) -> bool:
        lowered = f"{text}\n{path.name}".lower()
        if not self._is_trade_republic_document(text, path):
            return False
        return "depotauszug" in lowered

    def _parse_snapshot_date(self, text: str, path: Path) -> date:
        for pattern in (r"\bzum\s+(\d{2}\.\d{2}\.\d{4})\b", r"\bDATUM\s+(\d{2}\.\d{2}\.\d{4})\b"):
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return datetime.strptime(match.group(1), "%d.%m.%Y").date()

        filename_match = re.search(r"(\d{8})(?=\.pdf$)", path.name, flags=re.IGNORECASE)
        if filename_match:
            return datetime.strptime(filename_match.group(1), "%Y%m%d").date()
        return date.fromtimestamp(path.stat().st_mtime)

    @staticmethod
    def _find_block_end(lines: list[str], start_idx: int) -> int:
        for idx in range(start_idx, len(lines)):
            candidate = lines[idx]
            if re.match(rf"^(?:{AMOUNT_PATTERN})\s*Stk\.?\s+.+$", candidate, flags=re.IGNORECASE):
                return idx
            if candidate.upper().startswith("ANZAHL POSITIONEN"):
                return idx
        return len(lines)

    @staticmethod
    def _find_isin(block_lines: list[str]) -> tuple[int, str | None]:
        for idx, line in enumerate(block_lines):
            match = re.search(r"\bISIN:?\s*([A-Z]{2}[A-Z0-9]{10})\b", line, flags=re.IGNORECASE)
            if match:
                return idx, match.group(1).upper()
        return 0, None

    def _find_snapshot_price(self, lines: list[str], *, path: Path) -> float | None:
        for line in lines:
            if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", line):
                continue
            if re.fullmatch(AMOUNT_PATTERN, line):
                return abs(self._parse_european_number(line))
        raise PdfParseError(f"Could not parse snapshot price in Trade Republic Depotauszug: {path.name}")

    @staticmethod
    def _is_name_noise_line(line: str) -> bool:
        lowered = line.lower()
        return lowered.startswith("stk.") or lowered.startswith("isin:")

    def _parse_european_number(self, value: str) -> float:
        return self._general_pdf_parser.parse_european_number(value)

    def _normalize_space(self, value: str) -> str:
        return self._general_pdf_parser.normalize_space(value)

    @staticmethod
    def _resolve_wkn(isin: str) -> str:
        known = ISIN_TO_WKN.get(isin.upper())
        if known:
            return known

        digest = hashlib.sha1(isin.encode("utf-8")).hexdigest().upper()
        return f"TR{digest[:4]}"
