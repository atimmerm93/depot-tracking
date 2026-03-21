from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class RepairRulesLoader:
    def __init__(self, *, repair_rules_path: Path | None = None) -> None:
        self._repair_rules_path = repair_rules_path or (
            Path(__file__).resolve().parent.parent / "repair" / "repair_rules.json"
        )

    def load(self) -> dict[str, Any]:
        payload = json.loads(self._repair_rules_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Repair config must be a JSON object: {self._repair_rules_path}")
        return payload

    @property
    def repair_rules_path(self) -> Path:
        return self._repair_rules_path
