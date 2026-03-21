from di_unit_of_work.base_dao import BaseDao
from di_unit_of_work.session_provider import SessionProvider
from di_unit_of_work.transactional_decorator import transactional

from .models import SourceDocumentWriteModel, TransactionModel, TransactionUpdateModel, TransactionWriteModel
from .source_document_data_operations import SourceDocumentDataOperations
from ...core.models import Transaction


class TransactionDataOperations(BaseDao):
    def __init__(self, session_provider: SessionProvider, source_documents: SourceDocumentDataOperations) -> None:
        super().__init__(session_provider)
        self._source_documents = source_documents

    @transactional
    def create(self, payload: TransactionWriteModel) -> TransactionModel:
        source_document = self._source_documents.get_or_create(
            SourceDocumentWriteModel(file_path=payload.source_file, file_hash=payload.source_hash)
        )

        row = Transaction(
            product_id=payload.product_id,
            source_document_id=source_document.id,
            type=payload.type,
            transaction_date=payload.transaction_date,
            quantity=float(payload.quantity),
            gross_amount=float(payload.gross_amount),
            costs=float(payload.costs),
            currency=payload.currency,
            bank=payload.bank,
        )
        self._session.add(row)
        self._session.flush()
        return TransactionModel(
            id=row.id,
            product_id=row.product_id,
            type=row.type,
            transaction_date=row.transaction_date,
            quantity=float(row.quantity),
            gross_amount=float(row.gross_amount),
            costs=float(row.costs),
            currency=row.currency,
            bank=row.bank,
            source_document_id=source_document.id,
            source_file=source_document.file_path,
            source_hash=source_document.file_hash,
        )

    @transactional
    def delete_by_id(self, transaction_id: int) -> None:
        row = self._session.get(Transaction, transaction_id)
        if row is None:
            return
        self._session.delete(row)

    @transactional
    def update(self, payload: TransactionUpdateModel) -> None:
        row = self._session.get(Transaction, payload.transaction_id)
        if row is None:
            return

        if payload.type is not None:
            row.type = payload.type
        if payload.quantity is not None:
            row.quantity = float(payload.quantity)
        if payload.gross_amount is not None:
            row.gross_amount = float(payload.gross_amount)
        if payload.costs is not None:
            row.costs = float(payload.costs)

        self._session.flush()
