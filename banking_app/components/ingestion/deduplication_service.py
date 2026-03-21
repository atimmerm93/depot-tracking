from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from di_unit_of_work.transactional_decorator import transactional

from banking_app.components.data_operations.holding_snapshot_data_operations import HoldingSnapshotDataOperations
from banking_app.components.data_operations.holding_snapshot_repository import HoldingSnapshotRepository
from banking_app.components.data_operations.processed_file_data_operations import ProcessedFileDataOperations
from banking_app.components.data_operations.processed_file_repository import ProcessedFileRepository
from banking_app.components.data_operations.transaction_data_operations import TransactionDataOperations
from banking_app.components.data_operations.transaction_repository import TransactionRepository
from banking_app.components.shared import SourceDocumentNormalizer


@dataclass
class DuplicateCleanupStats:
    files_seen: int = 0
    files_removed: int = 0
    tx_removed: int = 0
    snapshots_removed: int = 0
    processed_removed: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "files_seen": self.files_seen,
            "files_removed": self.files_removed,
            "tx_removed": self.tx_removed,
            "snapshots_removed": self.snapshots_removed,
            "processed_removed": self.processed_removed,
        }


class DocumentDeduplicationService:
    def __init__(
            self,
            *,
            tx_repo: TransactionRepository,
            tx_ops: TransactionDataOperations,
            snapshot_repo: HoldingSnapshotRepository,
            snapshot_ops: HoldingSnapshotDataOperations,
            processed_files: ProcessedFileRepository,
            processed_file_ops: ProcessedFileDataOperations,
            source_document_normalizer: SourceDocumentNormalizer,
    ) -> None:
        self._tx_repo = tx_repo
        self._tx_ops = tx_ops
        self._snapshot_repo = snapshot_repo
        self._snapshot_ops = snapshot_ops
        self._processed_files = processed_files
        self._processed_file_ops = processed_file_ops
        self._source_document_normalizer = source_document_normalizer

    @transactional
    def cleanup_duplicate_documents(self, pdf_dir: str | Path) -> dict[str, int]:
        directory = Path(pdf_dir)
        stats = DuplicateCleanupStats()
        self._remove_duplicate_files(directory=directory, stats=stats)
        stats.tx_removed = self._remove_duplicate_rows(
            rows=self._tx_repo.list_all(),
            key_func=self._transaction_dedup_key,
            source_attr="source_file",
            delete_by_id=self._tx_ops.delete_by_id,
        )
        stats.snapshots_removed = self._remove_duplicate_rows(
            rows=self._snapshot_repo.list_all(),
            key_func=self._snapshot_dedup_key,
            source_attr="source_file",
            delete_by_id=self._snapshot_ops.delete_by_id,
        )
        stats.processed_removed = self._remove_duplicate_rows(
            rows=self._processed_files.list_all(),
            key_func=self._processed_file_dedup_key,
            source_attr="file_path",
            delete_by_id=self._processed_file_ops.delete_by_id,
        )
        return stats.as_dict()

    def _remove_duplicate_files(self, directory: Path, *, stats: DuplicateCleanupStats) -> None:
        if not directory.exists():
            return

        for pdf_file in sorted(directory.rglob("*.pdf")):
            stats.files_seen += 1
            canonical_file = self._source_document_normalizer.canonical_duplicate_file_target(pdf_file)
            if canonical_file is None or not canonical_file.exists():
                continue
            pdf_file.unlink()
            stats.files_removed += 1
            print(f"[DEDUP][FILE] Removed duplicate: {pdf_file}")

    def _remove_duplicate_rows(
            self,
            *,
            rows: list[Any],
            key_func: Callable[[Any], tuple[Any, ...]],
            source_attr: str,
            delete_by_id: Callable[[int], None],
    ) -> int:
        removed = 0
        for grouped_rows in self._group_rows_by_key(rows=rows, key_func=key_func).values():
            if len(grouped_rows) <= 1:
                continue
            keep = self._source_document_normalizer.select_preferred_source_row(grouped_rows, source_attr=source_attr)
            for row in grouped_rows:
                if row.id == keep.id:
                    continue
                delete_by_id(row.id)
                removed += 1
        return removed

    @staticmethod
    def _group_rows_by_key(
            *, rows: list[Any], key_func: Callable[[Any], tuple[Any, ...]]
    ) -> dict[tuple[Any, ...], list[Any]]:
        groups: dict[tuple[Any, ...], list[Any]] = {}
        for row in rows:
            groups.setdefault(key_func(row), []).append(row)
        return groups

    def _transaction_dedup_key(self, row: Any) -> tuple[Any, ...]:
        return (
            row.product_id,
            str(row.type),
            row.transaction_date.isoformat(),
            round(float(row.quantity), 8),
            round(float(row.gross_amount), 8),
            round(float(row.costs), 8),
            str(row.currency or ""),
            str(row.bank or ""),
            self._source_document_normalizer.canonical_source_key(row.source_file),
        )

    def _snapshot_dedup_key(self, row: Any) -> tuple[Any, ...]:
        return (
            row.product_id,
            row.snapshot_date.isoformat(),
            round(float(row.quantity), 8),
            round(float(row.snapshot_price), 8) if row.snapshot_price is not None else None,
            self._source_document_normalizer.canonical_source_key(row.source_file),
        )

    def _processed_file_dedup_key(self, row: Any) -> tuple[Any, ...]:
        return (
            str(row.parser_version or ""),
            self._source_document_normalizer.canonical_source_key(row.file_path),
        )
