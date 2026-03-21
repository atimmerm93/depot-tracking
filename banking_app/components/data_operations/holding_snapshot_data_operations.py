from di_unit_of_work.base_dao import BaseDao
from di_unit_of_work.session_provider import SessionProvider
from di_unit_of_work.transactional_decorator import transactional

from .models import HoldingSnapshotModel, HoldingSnapshotWriteModel, SourceDocumentWriteModel
from .source_document_data_operations import SourceDocumentDataOperations
from ...core.models import HoldingSnapshot


class HoldingSnapshotDataOperations(BaseDao):
    def __init__(self,
                 session_provider: SessionProvider,
                 source_documents_data_operations: SourceDocumentDataOperations) -> None:
        super().__init__(session_provider)
        self._source_documents = source_documents_data_operations

    @transactional
    def create(self, payload: HoldingSnapshotWriteModel) -> HoldingSnapshotModel:
        source_document = self._source_documents.get_or_create(
            SourceDocumentWriteModel(file_path=payload.source_file, file_hash=payload.source_hash)
        )

        holding_snapshot = HoldingSnapshot(
            product_id=payload.product_id,
            source_document_id=source_document.id,
            snapshot_date=payload.snapshot_date,
            quantity=float(payload.quantity),
            snapshot_price=float(payload.snapshot_price) if payload.snapshot_price is not None else None,
        )

        self._add_to_db(holding_snapshot)

        return HoldingSnapshotModel(
            id=holding_snapshot.id,
            product_id=holding_snapshot.product_id,
            source_document_id=source_document.id,
            snapshot_date=holding_snapshot.snapshot_date,
            quantity=float(holding_snapshot.quantity),
            snapshot_price=float(holding_snapshot.snapshot_price)
            if holding_snapshot.snapshot_price is not None else None,
            source_file=source_document.file_path,
            source_hash=source_document.file_hash,
        )

    @transactional
    def delete_by_id(self, snapshot_id: int) -> None:
        holding_snapshot = self._session.query(HoldingSnapshot).where(HoldingSnapshot.id == snapshot_id).one_or_none()
        if holding_snapshot is None:
            return
        self._session.delete(holding_snapshot)
