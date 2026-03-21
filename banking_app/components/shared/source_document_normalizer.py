from __future__ import annotations

import re
from pathlib import Path
from typing import Any


class SourceDocumentNormalizer:
    def canonical_duplicate_file_target(self, file_path: Path) -> Path | None:
        match = re.match(r"^(?P<stem>.+)_(?P<idx>\d+)\.pdf$", file_path.name, flags=re.IGNORECASE)
        if not match:
            return None
        return file_path.with_name(f"{match.group('stem')}.pdf")

    def canonical_source_key(self, source_file: str) -> str:
        raw = (source_file or "").strip()
        if not raw:
            return ""

        candidate = raw
        if ":" in raw:
            prefix, suffix = raw.split(":", 1)
            if prefix in {"inferred_from_depotauszug", "repair_alias_close", "repair_split", "repair_exchange"} and suffix:
                candidate = suffix

        path = Path(candidate)
        name = path.name
        match = re.match(r"^(?P<stem>.+)_(?P<idx>\d+)\.pdf$", name, flags=re.IGNORECASE)
        if match:
            name = f"{match.group('stem')}.pdf"
            path = path.with_name(name)
        return str(path).lower()

    def select_preferred_source_row(self, rows: list[Any], *, source_attr: str) -> Any:
        def sort_key(row: Any) -> tuple[int, int]:
            source = str(getattr(row, source_attr, "") or "")
            file_name = Path(source).name
            has_suffix_dup = re.search(r"_\d+\.pdf$", file_name, flags=re.IGNORECASE) is not None
            penalty = 1 if has_suffix_dup else 0
            return penalty, int(getattr(row, "id", 0))

        return sorted(rows, key=sort_key)[0]
