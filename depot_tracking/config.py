from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BankingAppConfig:
    db_path: Path


@dataclass(frozen=True)
class ParserConfig:
    bank_hint: str = "auto"
