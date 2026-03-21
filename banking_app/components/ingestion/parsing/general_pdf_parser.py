from __future__ import annotations

import re
from pathlib import Path

from pypdf import PdfReader

from banking_app.core.parser_errors import PdfParseError


class GeneralPdfParser:
    """Shared low-level PDF parsing helpers used by bank-specific parsers."""

    @staticmethod
    def extract_text(path: Path) -> str:
        reader = PdfReader(str(path))
        content: list[str] = []
        for page in reader.pages:
            content.append(page.extract_text() or "")
        text = "\n".join(content).replace("\xa0", " ")
        if not text.strip():
            raise PdfParseError(f"No text could be extracted from {path}")
        return text

    @staticmethod
    def compact_text(text: str) -> str:
        return re.sub(r"\s+", "", text)

    @staticmethod
    def parse_european_number(value: str) -> float:
        normalized = value.replace(" ", "").replace(".", "").replace(",", ".").replace("+", "")
        return float(normalized)

    @staticmethod
    def normalize_space(value: str) -> str:
        return " ".join(value.split())

    @staticmethod
    def first_match(patterns: list[str], text: str) -> str | None:
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
            if match:
                return match.group(1).strip()
        return None
