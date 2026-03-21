from di_unit_of_work.base_dao import BaseDao
from di_unit_of_work.session_provider import SessionProvider
from di_unit_of_work.transactional_decorator import transactional

from .models import (
    ProcessedFileModel,
    ProcessedFileWriteModel,
    SourceDocumentWriteModel,
)
from .source_document_data_operations import SourceDocumentDataOperations
from ...core.models import ProcessedFile


class ProcessedFileDataOperations(BaseDao):

    def __init__(self,
                 session_provider: SessionProvider,
                 source_document_operations: SourceDocumentDataOperations) -> None:
        super().__init__(session_provider)
        self._source_documents = source_document_operations

    @transactional
    def create(self, payload: ProcessedFileWriteModel) -> ProcessedFileModel:
        source_document = self._source_documents.get_or_create(
            SourceDocumentWriteModel(file_path=payload.file_path, file_hash=payload.file_hash)
        )
        row = ProcessedFile(
            source_document_id=source_document.id,
            parser_version=payload.parser_version,
        )
        self._session.add(row)
        self._session.flush()
        return ProcessedFileModel(
            id=row.id,
            source_document_id=row.source_document_id,
            parser_version=row.parser_version,
            file_path=source_document.file_path,
            file_hash=source_document.file_hash,
        )

    def delete_by_id(self, processed_file_id: int) -> None:
        row = self._session.get(ProcessedFile, processed_file_id)
        if row is None:
            return
        self._session.delete(row)
