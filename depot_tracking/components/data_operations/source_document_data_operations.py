from di_unit_of_work.base_dao import BaseDao
from di_unit_of_work.transactional_decorator import transactional

from .models import SourceDocumentModel, SourceDocumentWriteModel
from ...core.models import SourceDocument


class SourceDocumentDataOperations(BaseDao):

    @transactional
    def get_or_create(self, payload: SourceDocumentWriteModel) -> SourceDocumentModel:
        source_document = self._session.query(SourceDocument).where(
            SourceDocument.file_hash == payload.file_hash).one_or_none()
        if source_document is None:
            source_document = SourceDocument(file_path=payload.file_path, file_hash=payload.file_hash)
            self._add_to_db(source_document)
        else:
            # Keep the stored path aligned with the latest resolved source path.
            source_document.file_path = payload.file_path
            self._session.flush()

        return SourceDocumentModel(id=source_document.id, file_path=source_document.file_path,
                                   file_hash=source_document.file_hash,
                                   created_at=source_document.created_at)
