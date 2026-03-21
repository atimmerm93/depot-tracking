from __future__ import annotations

from di_unit_of_work.base_dao import BaseDao
from di_unit_of_work.transactional_decorator import transactional
from sqlalchemy import select

from .models import SourceDocumentModel
from ...core.models import SourceDocument


class SourceDocumentRepository(BaseDao):

    @transactional
    def get_by_hash(self, file_hash: str) -> SourceDocumentModel | None:
        row = self._session.execute(
            select(SourceDocument).where(SourceDocument.file_hash == file_hash)).scalar_one_or_none()
        if row is None:
            return None
        return SourceDocumentModel(id=row.id, file_path=row.file_path, file_hash=row.file_hash,
                                   created_at=row.created_at)

    @transactional
    def get_by_id(self, source_document_id: int) -> SourceDocumentModel | None:
        row = self._session.get(SourceDocument, source_document_id)
        if row is None:
            return None
        return SourceDocumentModel(id=row.id, file_path=row.file_path, file_hash=row.file_hash,
                                   created_at=row.created_at)
