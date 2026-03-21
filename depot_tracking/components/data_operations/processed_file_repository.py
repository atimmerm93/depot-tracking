from __future__ import annotations

from di_unit_of_work.base_dao import BaseDao
from di_unit_of_work.transactional_decorator import transactional

from .models import ProcessedFileModel
from ...core.models import ProcessedFile


class ProcessedFileRepository(BaseDao):

    @transactional
    def exists_by_file_hash(self, file_hash: str) -> bool:
        processed_file = self._session.query(ProcessedFile).where(
            ProcessedFile.source_document.has(file_hash=file_hash)).first()
        return processed_file is not None

    @transactional
    def list_all(self) -> list[ProcessedFileModel]:
        rows = self._session.query(ProcessedFile).all()
        return [self._to_model(row) for row in rows]

    def list_file_paths(self) -> list[str]:
        return [item.file_path for item in self.list_all()]

    @staticmethod
    def _to_model(row: ProcessedFile) -> ProcessedFileModel:
        return ProcessedFileModel(
            id=row.id,
            source_document_id=row.source_document_id,
            parser_version=row.parser_version,
            file_path=row.source_document.file_path,
            file_hash=row.source_document.file_hash,
        )
