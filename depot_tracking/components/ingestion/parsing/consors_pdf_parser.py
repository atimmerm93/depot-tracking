from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

from depot_tracking.core.models import ParsedHolding, ParsedTransaction, TransactionType
from depot_tracking.core.parser_errors import PdfParseError, UnsupportedPdfDocument
from .general_pdf_parser import GeneralPdfParser

AMOUNT_PATTERN = r"\d{1,3}(?:\.\d{3})*,\d+|\d+,\d+"


class ConsorsPdfTransactionParser:
    parser_version = "consors-v1"

    def __init__(self, general_pdf_parser: GeneralPdfParser | None = None) -> None:
        self._general_pdf_parser = general_pdf_parser or GeneralPdfParser()

    def parse(self, pdf_path: str | Path) -> ParsedTransaction:
        path = Path(pdf_path)
        text = self._extract_text(path)
        compact = self._compact_text(text)

        tx_type = self._parse_transaction_type(path, compact)
        if self._is_depotauszug(path, compact):
            raise UnsupportedPdfDocument(f"Consors Depotauszug document: {path.name}")

        wkn = self._parse_wkn(path, compact)
        isin = self._parse_isin(compact, wkn=wkn)
        product_name = self._parse_product_name(text, compact)
        tx_date = self._parse_transaction_date(path, compact)
        quantity = self._parse_quantity(compact, tx_type)
        gross_amount = self._parse_gross_amount(compact, tx_type)
        costs = self._parse_costs(compact, tx_type)

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
        compact = self._compact_text(text)
        if not self._is_depotauszug(path, compact):
            raise UnsupportedPdfDocument(f"Not a Consors Depotauszug document: {path.name}")

        snapshot_date = self._parse_transaction_date(path, compact)
        holdings = self._parse_depotauszug_holdings_rows(compact)
        if not holdings and self._is_empty_depotauszug(compact):
            return snapshot_date, []
        if not holdings:
            raise PdfParseError(f"Could not parse holdings from Consors Depotauszug: {path.name}")
        return snapshot_date, holdings

    def _parse_depotauszug_holdings_rows(self, compact_text: str) -> list[ParsedHolding]:
        holdings: list[ParsedHolding] = []
        seen: set[tuple[str, float]] = set()
        row_pattern = re.compile(r"ST(?P<qty>\d+,\d+)(?P<body>.*?)(?=ST\d+,\d+|ANZAHLPOSTEN|GESAMTKURSWERT|$)")
        strict_amount_pattern = r"\d{1,4}(?:\.\d{3})*,\d+"
        for match in row_pattern.finditer(compact_text):
            qty = abs(self._parse_european_number(match.group("qty")))
            if qty <= 0:
                continue

            body = match.group("body")
            body = re.split(
                r"MitfreundlichenGrüßen|MitfreundlichenGruessen|IstderKurswerteinesWertpapiers|HinweiszuBail-inProdukten",
                body,
            )[0]
            parsed = self._parse_depotauszug_new_layout_row(body, qty, strict_amount_pattern)
            if parsed is None:
                parsed = self._parse_depotauszug_compact_quote_row(body, qty)
            if parsed is None:
                parsed = self._parse_depotauszug_legacy_layout_row(body, qty)
            if parsed is None:
                continue

            wkn, product_name, snapshot_price = parsed
            key = (wkn, round(qty, 8))
            if key in seen:
                continue
            seen.add(key)
            holdings.append(
                ParsedHolding(
                    wkn=wkn,
                    isin=None,
                    product_name=product_name,
                    quantity=qty,
                    snapshot_price=snapshot_price,
                )
            )
        return holdings

    def _parse_depotauszug_new_layout_row(
            self, body: str, qty: float, strict_amount_pattern: str
    ) -> tuple[str, str | None, float] | None:
        tail = re.search(
            rf"(?:Deutschland|Girosammelverwahrung|Wertpapierrechnung)"
            rf"(?P<wkn>[A-Z0-9]{{6}})(?P<price>{strict_amount_pattern})EUR1,00(?P<value>{strict_amount_pattern})",
            body,
        )
        if tail is None:
            return None

        wkn = tail.group("wkn").upper()
        price = abs(self._parse_european_number(tail.group("price")))
        name_part = body[: tail.start()]
        name_part = re.split(r"Girosammelverwahrung|Wertpapierrechnung", name_part)[0]
        product_name = self._humanize_compact_name(name_part)
        return wkn, product_name, price

    def _parse_depotauszug_compact_quote_row(self, body: str, qty: float) -> tuple[str, str | None, float] | None:
        compact = re.sub(r"^[,0]+", "", body)
        tail_match = re.search(
            r"(?P<prefix>.*)(?P<ccy>[A-Z]{3})(?P<trailer>[0-9\.,]+)$",
            compact,
        )
        if tail_match is None:
            return None

        ccy = tail_match.group("ccy").upper()
        trailer = tail_match.group("trailer")
        before_ccy = tail_match.group("prefix")
        if len(before_ccy) < 10:
            return None

        fx_value_candidates: list[tuple[float, float]] = []
        for split_at in range(4, len(trailer) - 3):
            fx_token = trailer[:split_at]
            value_token = trailer[split_at:]
            if re.fullmatch(r"\d{1,3},\d{2,4}", fx_token) is None:
                continue
            if re.fullmatch(r"\d{1,3}(?:\.\d{3})*,\d{2}", value_token) is None:
                continue
            if ccy == "EUR" and re.fullmatch(r"1,\d{2}", fx_token) is None:
                continue
            fx_value_candidates.append(
                (
                    self._parse_european_number(fx_token),
                    abs(self._parse_european_number(value_token)),
                )
            )
        if not fx_value_candidates:
            return None

        raw_price_match = re.search(r"(?P<raw>\d+,\d{2,6})$", before_ccy)
        if raw_price_match is None:
            return None
        raw_price = raw_price_match.group("raw")
        int_part, frac_part = raw_price.split(",", 1)

        best: tuple[float, int, float] | None = None
        raw_start = raw_price_match.start()
        for int_len in range(1, min(4, len(int_part)) + 1):
            candidate_token = f"{int_part[-int_len:]},{frac_part}"
            candidate_price = self._parse_european_number(candidate_token)
            for fx_rate, value_eur in fx_value_candidates:
                if ccy == "EUR":
                    expected_value_eur = qty * candidate_price
                else:
                    if fx_rate <= 0:
                        continue
                    expected_value_eur = (qty * candidate_price) / fx_rate
                error = abs(expected_value_eur - value_eur)
                if best is None or error < best[0]:
                    best = (error, int_len, value_eur)

        if best is None:
            return None

        error, best_int_len, value_eur = best
        tolerance = max(0.5, value_eur * 0.05)
        if error > tolerance:
            return None

        candidate_start = raw_start + (len(int_part) - best_int_len)
        wkn_start = candidate_start - 6
        if wkn_start < 0:
            return None

        wkn = before_ccy[wkn_start:candidate_start].upper()
        if re.fullmatch(r"[A-Z0-9]{6}", wkn) is None:
            return None

        snapshot_price = value_eur / qty if qty else 0.0
        name_part = before_ccy[:wkn_start]
        product_name = self._humanize_compact_name(name_part)
        return wkn, product_name, snapshot_price

    def _parse_depotauszug_legacy_layout_row(self, body: str, qty: float) -> tuple[str, str | None, float] | None:
        value_match = re.search(rf"(?P<price>{AMOUNT_PATTERN})(?P<ccy>[A-Z]{{3}})(?P<value>{AMOUNT_PATTERN})", body)
        if value_match is None:
            return None

        custody_match = re.search(r"Girosammelverwahrung|Wertpapierrechnung", body)
        if custody_match is None:
            return None
        tail = body[: custody_match.start()]
        wkn_candidates = list(re.finditer(r"(?=(?P<wkn>[A-Z0-9]{6})(?P<fx>\d{1,3},\d{2,3})$)", tail))
        if not wkn_candidates:
            return None

        selected = None
        for candidate in reversed(wkn_candidates):
            fx = self._parse_european_number(candidate.group("fx"))
            if 0.1 <= fx <= 20:
                selected = candidate
                break
        if selected is None:
            selected = wkn_candidates[-1]

        wkn = selected.group("wkn").upper()
        value_eur = abs(self._parse_european_number(value_match.group("value")))
        snapshot_price = value_eur / qty if qty else 0.0
        name_part = body[: value_match.start()]
        product_name = self._humanize_compact_name(name_part)
        return wkn, product_name, snapshot_price

    @staticmethod
    def _is_empty_depotauszug(compact_text: str) -> bool:
        lowered = compact_text.lower()
        return "keinebeständeverbucht" in lowered or "keinebestandeverbucht" in lowered

    def _extract_text(self, path: Path) -> str:
        return self._general_pdf_parser.extract_text(path)

    def _compact_text(self, text: str) -> str:
        return self._general_pdf_parser.compact_text(text)

    def _parse_transaction_type(self, path: Path, compact_text: str) -> TransactionType:
        filename = path.name.lower()
        if "verkauf" in filename:
            return TransactionType.SELL
        if "kauf" in filename:
            return TransactionType.BUY
        if "dividendengutschrift" in filename:
            return TransactionType.ERTRAGSABRECHNUNG

        lowered = compact_text.lower()
        if "orderabrechnungverkauf" in lowered:
            return TransactionType.SELL
        if "orderabrechnungkauf" in lowered:
            return TransactionType.BUY
        if "dividendengutschrift" in lowered:
            return TransactionType.ERTRAGSABRECHNUNG

        raise UnsupportedPdfDocument(f"Unsupported Consors PDF document type: {path.name}")

    def _is_depotauszug(self, path: Path, compact_text: str) -> bool:
        filename = path.name.lower()
        if "jahresdepotauszug" in filename or "quartalsdepotauszug" in filename:
            return True
        lowered = compact_text.lower()
        return "jahresdepotauszugwertpapiere" in lowered or "quartalsdepotauszugwertpapiere" in lowered

    def _parse_wkn(self, path: Path, compact_text: str) -> str:
        by_file = re.search(r"_wkn([A-Z0-9]{6})_", path.name, flags=re.IGNORECASE)
        if by_file:
            return by_file.group(1).upper()

        match = re.search(r"WKNISIN.*?([A-Z0-9]{6})([A-Z]{2}[A-Z0-9]{10})", compact_text)
        if match:
            return match.group(1).upper()
        raise PdfParseError("Could not find WKN in Consors PDF")

    @staticmethod
    def _parse_isin(compact_text: str, *, wkn: str) -> str | None:
        match = re.search(rf"{re.escape(wkn)}([A-Z]{{2}}[A-Z0-9]{{10}})", compact_text)
        if match:
            candidate = match.group(1).upper()
            if ConsorsPdfTransactionParser._looks_like_isin(candidate):
                return candidate

        contextual = re.search(rf"WKNISIN.*?([A-Z]{{2}}[A-Z0-9]{{10}})", compact_text)
        if contextual:
            candidate = contextual.group(1).upper()
            if ConsorsPdfTransactionParser._looks_like_isin(candidate):
                return candidate
        return None

    @staticmethod
    def _looks_like_isin(value: str) -> bool:
        if not value or len(value) != 12:
            return False
        if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{10}", value) is None:
            return False
        digit_count = sum(1 for item in value if item.isdigit())
        return digit_count >= 2

    def _parse_product_name(self, text: str, compact_text: str) -> str | None:
        for pattern in (
                r"Bezeichnung\s*WKNISIN\s*(.*?)\s+[A-Z0-9]{6}[A-Z]{2}[A-Z0-9]{10}",
                r"Wertpapierbezeichnung\s*WKNISIN\s*(.*?)\s+[A-Z0-9]{6}[A-Z]{2}[A-Z0-9]{10}",
        ):
            match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
            if match:
                raw = " ".join(match.group(1).split())
                return raw[:220] if raw else None

        match = re.search(r"WKNISIN(.*?)[A-Z0-9]{6}[A-Z]{2}[A-Z0-9]{10}", compact_text)
        if match:
            return self._humanize_compact_name(match.group(1))
        return None

    def _parse_transaction_date(self, path: Path, compact_text: str) -> date:
        file_match = re.search(r"_dat(\d{8})_", path.name, flags=re.IGNORECASE)
        if file_match:
            return datetime.strptime(file_match.group(1), "%Y%m%d").date()

        match = re.search(r"Datum:?(\d{2}\.\d{2}\.\d{4})", compact_text)
        if match:
            return datetime.strptime(match.group(1), "%d.%m.%Y").date()
        return date.fromtimestamp(path.stat().st_mtime)

    def _parse_quantity(self, compact_text: str, tx_type: TransactionType) -> float:
        if tx_type == TransactionType.ERTRAGSABRECHNUNG:
            return 0.0

        for pattern in (r"UmsatzST(" + AMOUNT_PATTERN + r")", r"EinheitUmsatzST(" + AMOUNT_PATTERN + r")"):
            match = re.search(pattern, compact_text)
            if match:
                return abs(self._parse_european_number(match.group(1)))
        raise PdfParseError("Could not find quantity in Consors PDF")

    def _parse_gross_amount(self, compact_text: str, tx_type: TransactionType) -> float:
        if tx_type in (TransactionType.BUY, TransactionType.SELL):
            value = self._find_amount_after_label(compact_text, "Kurswert")
            if value is not None:
                return value

        if tx_type == TransactionType.ERTRAGSABRECHNUNG:
            value = self._find_amount_after_label(compact_text, "BruttoinEUR")
            if value is not None:
                return value

            match = re.search(rf"Brutto({AMOUNT_PATTERN})EUR", compact_text, flags=re.IGNORECASE)
            if match:
                return self._parse_european_number(match.group(1))

            match = re.search(
                rf"KAPST-PFLICHTIGERKAPITALERTRAGEUR({AMOUNT_PATTERN})",
                compact_text,
                flags=re.IGNORECASE,
            )
            if match:
                return self._parse_european_number(match.group(1))

            match = re.search(
                rf"SteuerpflichtigerGesamtertrag({AMOUNT_PATTERN})EUR",
                compact_text,
                flags=re.IGNORECASE,
            )
            if match:
                return self._parse_european_number(match.group(1))

            value = self._find_net_credit_amount(compact_text)
            if value is not None:
                return value

        raise PdfParseError("Could not determine gross amount in Consors PDF")

    def _parse_costs(self, compact_text: str, tx_type: TransactionType) -> float:
        labels: tuple[str, ...]
        if tx_type == TransactionType.BUY:
            labels = ("Handelsplatzkosten", "Provision", "Grundgebühr", "Eig.Spesen", "EigeneSpesen")
        elif tx_type == TransactionType.SELL:
            labels = (
                "Kapitalertragssteuer",
                "Solidaritätszuschlag",
                "Kirchensteuer",
                "Provision",
                "Grundgebühr",
                "Handelsplatzkosten",
                "Eig.Spesen",
                "EigeneSpesen",
            )
        else:
            labels = ("Kapitalertragssteuer", "Solidaritätszuschlag", "Kirchensteuer")

        total = 0.0

        if tx_type == TransactionType.ERTRAGSABRECHNUNG:
            source_tax = self._find_source_tax_amount(compact_text)
            if source_tax is not None:
                total += abs(source_tax)

        for label in labels:
            amount = self._find_amount_after_label(compact_text, label)
            if amount is not None:
                total += abs(amount)
        return total

    def _find_amount_after_label(self, compact_text: str, label: str) -> float | None:
        patterns = (
            rf"{re.escape(label)}({AMOUNT_PATTERN})[A-Z]{{3}}",
            rf"{re.escape(label)}[A-Z]{{3}}({AMOUNT_PATTERN})",
        )
        for pattern in patterns:
            match = re.search(pattern, compact_text, flags=re.IGNORECASE)
            if match:
                return self._parse_european_number(match.group(1))
        return None

    def _find_net_credit_amount(self, compact_text: str) -> float | None:
        match = re.search(
            rf"Nettozugunsten(?:IBAN|KONTO-NR\.?|KONTO)?.*?({AMOUNT_PATTERN})EUR",
            compact_text,
            flags=re.IGNORECASE,
        )
        if match:
            return self._parse_european_number(match.group(1))

        match = re.search(
            rf"UMGER\.ZUMDEV\.-KURS(?:{AMOUNT_PATTERN})EUR({AMOUNT_PATTERN})WERT",
            compact_text,
            flags=re.IGNORECASE,
        )
        if match:
            return self._parse_european_number(match.group(1))
        return None

    def _find_source_tax_amount(self, compact_text: str) -> float | None:
        amount = self._find_amount_after_label(compact_text, "QuellensteuerinEUR")
        if amount is not None:
            return amount

        match = re.search(
            rf"abzgl\.Quellensteuer.*?({AMOUNT_PATTERN})EUR({AMOUNT_PATTERN})EUR",
            compact_text,
            flags=re.IGNORECASE,
        )
        if match:
            return self._parse_european_number(match.group(2))

        match = re.search(
            rf"QUST(?:{AMOUNT_PATTERN})%EUR({AMOUNT_PATTERN})",
            compact_text,
            flags=re.IGNORECASE,
        )
        if match:
            return self._parse_european_number(match.group(1))

        match = re.search(
            rf"AnrechenbareQuellensteuer.*?({AMOUNT_PATTERN})EUR",
            compact_text,
            flags=re.IGNORECASE,
        )
        if match:
            return self._parse_european_number(match.group(1))
        return None

    def _parse_european_number(self, value: str) -> float:
        return self._general_pdf_parser.parse_european_number(value)

    @staticmethod
    def _humanize_compact_name(value: str) -> str | None:
        if not value:
            return None
        cleaned = re.sub(r"[^A-Za-z0-9\.\-&,/ ]+", " ", value).strip()
        if not cleaned:
            return None
        cleaned = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", cleaned)
        cleaned = " ".join(cleaned.split())
        return cleaned[:220] if cleaned else None
