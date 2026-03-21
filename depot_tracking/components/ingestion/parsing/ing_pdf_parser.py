from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

from depot_tracking.core.models import ParsedHolding, ParsedTransaction, TransactionType
from depot_tracking.core.parser_errors import PdfParseError, UnsupportedPdfDocument
from .general_pdf_parser import GeneralPdfParser

AMOUNT_PATTERN = r"[-+]?\s*\d{1,3}(?:[\.\s]\d{3})*(?:,\d+)?|[-+]?\s*\d+(?:,\d+)?"


class INGPdfParser:
    parser_version = "v1"

    def __init__(self, general_pdf_parser: GeneralPdfParser | None = None) -> None:
        self._general_pdf_parser = general_pdf_parser or GeneralPdfParser()

    def parse(self, pdf_path: str | Path) -> ParsedTransaction:
        path = Path(pdf_path)
        text = self._extract_text(path)

        tx_type = self._parse_transaction_type(text, path)
        wkn = self._parse_wkn(text)
        isin = self._parse_isin(text)
        product_name = self._parse_product_name(text)
        tx_date = self._parse_transaction_date(text, path)
        quantity = self._parse_quantity(text, tx_type)
        gross_amount = self._parse_gross_amount(text, tx_type)
        costs = self._parse_costs(text)

        return ParsedTransaction(
            tx_type=tx_type,
            wkn=wkn,
            isin=isin,
            product_name=product_name,
            transaction_date=tx_date,
            quantity=quantity,
            gross_amount=gross_amount,
            costs=costs,
        )

    def parse_depotauszug_holdings(self, pdf_path: str | Path) -> tuple[date, list[ParsedHolding]]:
        path = Path(pdf_path)
        text = self._extract_text(path)
        if not self._is_depotauszug_document(text, path):
            raise UnsupportedPdfDocument(f"Not a Depotauszug document: {path.name}")

        snapshot_date = self._parse_depotauszug_date(text, path)
        lines = [self._normalize_space(line) for line in text.splitlines() if self._normalize_space(line)]
        holdings: list[ParsedHolding] = []
        seen: set[tuple[str, float]] = set()

        for idx, line in enumerate(lines):
            isin_match = re.search(
                r"ISIN\s*\(WKN\)\s*:?\s*([A-Z]{2}[A-Z0-9]{10})\s*\(([A-Z0-9]{6})\)",
                line,
                flags=re.IGNORECASE,
            )
            if not isin_match:
                continue

            isin = isin_match.group(1).upper()
            wkn = isin_match.group(2).upper()
            quantity_line_idx, quantity, first_name_part, snapshot_price = self._find_depotauszug_quantity_line(lines,
                                                                                                                idx)
            if quantity is None:
                continue

            name_parts = [first_name_part] if first_name_part else []
            for name_idx in range(quantity_line_idx + 1, idx):
                candidate = lines[name_idx]
                if self._is_depotauszug_noise_line(candidate):
                    continue
                name_parts.append(candidate)
            product_name = self._normalize_space(" ".join(name_parts))[:220] if name_parts else None

            key = (wkn, round(quantity, 8))
            if key in seen:
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

        if not holdings:
            raise PdfParseError(f"Could not parse holdings from Depotauszug: {path.name}")
        return snapshot_date, holdings

    def _extract_text(self, path: Path) -> str:
        return self._general_pdf_parser.extract_text(path)

    def _first_match(self, patterns: list[str], text: str) -> str | None:
        return self._general_pdf_parser.first_match(patterns, text)

    def _parse_transaction_type(self, text: str, path: Path) -> TransactionType:
        haystack = f"{text}\n{path.name}".lower()
        filename = path.name.lower()

        if self._is_depotauszug_document(text, path):
            raise UnsupportedPdfDocument(f"Depotauszug document: {path.name}")
        if "ertragsabrechnung" in filename or "vorabpauschale" in filename:
            return TransactionType.ERTRAGSABRECHNUNG
        if "abrechnung_verkauf" in filename or ("verkauf" in filename and "abrechnung" in filename):
            return TransactionType.SELL
        if "abrechnung_kauf" in filename or ("kauf" in filename and "abrechnung" in filename):
            return TransactionType.BUY

        if "verkauf" in haystack:
            return TransactionType.SELL
        if "kauf" in haystack:
            return TransactionType.BUY
        if any(item in haystack for item in
               ("ertragsabrechnung", "vorabpauschale", "dividende", "ausschüttung", "ausschuettung", "steuer")):
            return TransactionType.ERTRAGSABRECHNUNG
        raise UnsupportedPdfDocument(f"Unsupported PDF document type: {path.name}")

    def _is_depotauszug_document(self, text: str, path: Path) -> bool:
        lowered = f"{text}\n{path.name}".lower()
        return "depotauszug" in lowered

    def _parse_wkn(self, text: str) -> str:
        value = self._first_match(
            [
                r"\bWKN\s*[:]?\s*([A-Z0-9]{6})\b",
                r"\bISIN\s*\(WKN\)\s*[A-Z]{2}[A-Z0-9]{10}\s*\(([A-Z0-9]{6})\)",
            ],
            text,
        )
        if not value:
            raise PdfParseError("Could not find WKN in PDF text")
        return value.upper()

    def _parse_isin(self, text: str) -> str | None:
        value = self._first_match(
            [
                r"\bISIN\s*[:]?\s*([A-Z]{2}[A-Z0-9]{10})\b",
                r"\b([A-Z]{2}[A-Z0-9]{10})\b",
            ],
            text,
        )
        return value.upper() if value else None

    def _parse_product_name(self, text: str) -> str | None:
        lines = [line.strip() for line in text.splitlines() if line.strip()]

        for idx, line in enumerate(lines):
            if re.search(r"^\s*Wertpapierbezeichnung\b", line, flags=re.IGNORECASE):
                name = re.sub(r"^\s*Wertpapierbezeichnung\b", "", line, flags=re.IGNORECASE).strip()
                if idx + 1 < len(lines):
                    next_line = lines[idx + 1]
                    if not re.search(
                            r"\b(ISIN|WKN|Nominale|St[üu]ck|Kurs|Handelsplatz|Ausf[üu]hrungstag|Order|Valuta)\b",
                            next_line,
                            re.IGNORECASE,
                    ):
                        name = f"{name} {next_line}".strip()
                return name[:180] if name else None

        for idx, line in enumerate(lines):
            if re.search(r"\bISIN\b", line, flags=re.IGNORECASE):
                for offset in (1, 2, 3):
                    fwd_idx = idx + offset
                    if fwd_idx >= len(lines):
                        break
                    candidate = lines[fwd_idx]
                    if re.search(r"\b(WKN|ISIN|Kauf|Verkauf|St[üu]ck|Order|Abrechnung)\b", candidate, re.IGNORECASE):
                        continue
                    return candidate[:180]

                for offset in (1, 2, 3):
                    back_idx = idx - offset
                    if back_idx < 0:
                        break
                    candidate = lines[back_idx]
                    if re.search(r"\b(WKN|ISIN|Kauf|Verkauf|St[üu]ck|Order|Abrechnung)\b", candidate, re.IGNORECASE):
                        continue
                    return candidate[:180]
                break
        return None

    def _parse_transaction_date(self, text: str, path: Path) -> date:
        labels = [
            "Schlusstag",
            "Handelstag",
            "Ausführungstag",
            "Valuta",
            "Buchungstag",
            "Ex-Tag",
            "Zahltag",
        ]
        for label in labels:
            match = re.search(rf"{label}[^\d]*(\d{{2}}\.\d{{2}}\.\d{{4}})", text, flags=re.IGNORECASE)
            if match:
                return datetime.strptime(match.group(1), "%d.%m.%Y").date()

        filename_match = re.search(r"(\d{8})(?=\.pdf$)", path.name, flags=re.IGNORECASE)
        if filename_match:
            return datetime.strptime(filename_match.group(1), "%Y%m%d").date()

        return date.fromtimestamp(path.stat().st_mtime)

    def _parse_quantity(self, text: str, tx_type: TransactionType) -> float:
        value = self._first_match(
            [
                rf"\bSt[üu]ck\b[^0-9+-]*({AMOUNT_PATTERN})",
                rf"\bNominale?\b[^0-9+-]*({AMOUNT_PATTERN})",
            ],
            text,
        )
        if value is None:
            if tx_type == TransactionType.ERTRAGSABRECHNUNG:
                return 0.0
            raise PdfParseError("Could not find quantity in PDF text")
        return abs(self._parse_european_number(value))

    def _parse_gross_amount(self, text: str, tx_type: TransactionType) -> float:
        if tx_type == TransactionType.ERTRAGSABRECHNUNG:
            for label in (
                    "Gesamtbetrag zu Ihren Lasten",
                    "Gesamtbetrag zu Ihren Gunsten",
                    "Betrag zu Ihren Lasten",
                    "Betrag zu Ihren Gunsten",
                    "Ausmachender Betrag",
            ):
                amount = self._find_labeled_amount(text, label, absolute=False)
                if amount is None:
                    continue
                if "Lasten" in label and amount > 0:
                    return -amount
                if "Gunsten" in label and amount < 0:
                    return -amount
                return amount

        for label in ("Ausmachender Betrag", "Betrag zu Ihren Lasten", "Betrag zu Ihren Gunsten", "Kurswert"):
            amount = self._find_labeled_amount(text, label, absolute=True)
            if amount is not None:
                return amount

        all_amounts = re.findall(rf"({AMOUNT_PATTERN})\s*EUR", text, flags=re.IGNORECASE) + re.findall(
            rf"EUR[^0-9+-]*({AMOUNT_PATTERN})",
            text,
            flags=re.IGNORECASE,
        )
        if not all_amounts:
            raise PdfParseError("Could not determine gross amount")

        parsed = [self._parse_european_number(item) for item in all_amounts]
        if tx_type == TransactionType.ERTRAGSABRECHNUNG:
            return parsed[0]
        return max(abs(item) for item in parsed)

    def _parse_costs(self, text: str) -> float:
        total = self._find_labeled_amount(text, "Gesamtkosten", absolute=True)
        if total is not None:
            return total

        labels = [
            "Provision",
            "Orderentgelt",
            "Transaktionsentgelt",
            "Börsengebühr",
            "Fremde Spesen",
            "Abwicklungsentgelt",
            "Handelsplatzentgelt",
        ]

        costs = 0.0
        for label in labels:
            amount = self._find_labeled_amount(text, label, absolute=True)
            if amount is not None:
                costs += amount
        return costs

    def _find_labeled_amount(self, text: str, label: str, *, absolute: bool) -> float | None:
        patterns = [
            rf"{label}[^\n\r]*?({AMOUNT_PATTERN})\s*EUR",
            rf"{label}[^\n\r]*?EUR[^0-9+-]*({AMOUNT_PATTERN})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = self._parse_european_number(match.group(1))
                return abs(value) if absolute else value
        return None

    def _parse_european_number(self, value: str) -> float:
        return self._general_pdf_parser.parse_european_number(value)

    def _parse_depotauszug_date(self, text: str, path: Path) -> date:
        patterns = [
            r"Depotauszug\s+per\s+(\d{2}\.\d{2}\.\d{4})",
            r"\bStand\s+(\d{2}\.\d{2}\.\d{4})",
            r"\bDatum\s*:?\s*(\d{2}\.\d{2}\.\d{4})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return datetime.strptime(match.group(1), "%d.%m.%Y").date()

        filename_match = re.search(r"(\d{8})(?=\.pdf$)", path.name, flags=re.IGNORECASE)
        if filename_match:
            return datetime.strptime(filename_match.group(1), "%Y%m%d").date()
        return date.fromtimestamp(path.stat().st_mtime)

    def _find_depotauszug_quantity_line(
            self, lines: list[str], isin_line_idx: int
    ) -> tuple[int, float | None, str | None, float | None]:
        for back_idx in range(isin_line_idx - 1, max(isin_line_idx - 8, -1), -1):
            line = lines[back_idx]
            detailed_match = re.search(
                rf"({AMOUNT_PATTERN})\s*St[üu]ck\b\s*(.*?)\s+({AMOUNT_PATTERN})\s*EUR\s+({AMOUNT_PATTERN})\s*EUR\s*$",
                line,
                flags=re.IGNORECASE,
            )
            if detailed_match:
                quantity = abs(self._parse_european_number(detailed_match.group(1)))
                name_part = self._normalize_space(detailed_match.group(2))
                unit_price = abs(self._parse_european_number(detailed_match.group(3)))
                return back_idx, quantity, name_part, unit_price

            match = re.search(rf"({AMOUNT_PATTERN})\s*St[üu]ck\b\s*(.*)", line, flags=re.IGNORECASE)
            if match:
                quantity = abs(self._parse_european_number(match.group(1)))
                name_part = self._normalize_space(match.group(2))
                return back_idx, quantity, name_part, None
        return isin_line_idx, None, None, None

    @staticmethod
    def _is_depotauszug_noise_line(line: str) -> bool:
        if not line:
            return True
        lowered = line.lower()
        blocked = (
            "isin",
            "verwahrart",
            "lagerland",
            "devisenkurs",
            "übertrag",
            "kurswert",
            "stücke/nominale",
            "anzahl posten",
            "depotinhaber",
            "direkt-depot",
            "seite:",
            "datum:",
        )
        return any(item in lowered for item in blocked)

    def _normalize_space(self, value: str) -> str:
        return self._general_pdf_parser.normalize_space(value)
