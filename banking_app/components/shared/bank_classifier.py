from __future__ import annotations

from pathlib import Path


class BankClassifier:
    def __init__(self, *, consors_filename_prefixes: tuple[str, ...] | None = None) -> None:
        self._consors_filename_prefixes = consors_filename_prefixes or (
            "kauf_",
            "verkauf_",
            "dividendengutschrift_",
            "jahresdepotauszug_",
            "quartalsdepotauszug_",
            "wertpapier-jahresdepotauszug_",
        )

    def infer_bank_from_file_path(self, source_file: str | Path, *, parser_bank_hint: str = "auto") -> str:
        hint = (parser_bank_hint or "auto").strip().lower()
        if hint == "ing":
            return "ING"
        if hint == "consors":
            return "CONSORS"
        if hint == "trade_republic":
            return "TRADE_REPUBLIC"

        preferred = self.preferred_parser_bank(source_file)
        if preferred == "ing":
            return "ING"
        if preferred == "consors":
            return "CONSORS"
        if preferred == "trade_republic":
            return "TRADE_REPUBLIC"
        return "UNKNOWN"

    def preferred_parser_bank(self, source_file: str | Path) -> str | None:
        raw = str(source_file)
        lowered = raw.lower()
        if ":" in raw:
            _, suffix = raw.split(":", 1)
            if suffix:
                lowered = f"{lowered}|{suffix.lower()}|{Path(suffix).name.lower()}"
        filename = Path(raw).name.lower()

        if "direkt_depot" in lowered or "direkt_depot" in filename:
            return "ing"
        if any(filename.startswith(prefix) for prefix in self._consors_filename_prefixes):
            return "consors"
        if "cortal_consors" in lowered or "consors" in lowered:
            return "consors"
        if "trade_republic" in lowered or "traderepublic" in lowered or "trade republic" in lowered:
            return "trade_republic"
        if "ertragsabrechnung" in lowered or "abrechnung_kauf" in lowered or "abrechnung_verkauf" in lowered:
            return "ing"
        return None
