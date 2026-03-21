from banking_app.components.data_operations.holding_snapshot_data_operations import HoldingSnapshotDataOperations
from banking_app.components.data_operations.holding_snapshot_repository import HoldingSnapshotRepository
from banking_app.components.data_operations.models import HoldingSnapshotWriteModel, ProcessedFileWriteModel, \
    ProductUpsertModel, TransactionWriteModel
from banking_app.components.data_operations.processed_file_data_operations import ProcessedFileDataOperations
from banking_app.components.data_operations.processed_file_repository import ProcessedFileRepository
from banking_app.components.data_operations.product_data_operations import ProductDataOperations
from banking_app.components.data_operations.transaction_data_operations import TransactionDataOperations
from banking_app.components.data_operations.transaction_repository import TransactionRepository
from banking_app.components.shared import SourceDocumentNormalizer
from .models import HoldingsDocument, TransactionDocument


class IngestionStore:
    def __init__(
            self,
            *,
            processed_files: ProcessedFileRepository,
            processed_file_ops: ProcessedFileDataOperations,
            products: ProductDataOperations,
            tx_repo: TransactionRepository,
            tx_ops: TransactionDataOperations,
            snapshot_repo: HoldingSnapshotRepository,
            snapshot_ops: HoldingSnapshotDataOperations,
            source_document_normalizer: SourceDocumentNormalizer,
    ) -> None:
        self._processed_files = processed_files
        self._processed_file_ops = processed_file_ops
        self._products = products
        self._tx_repo = tx_repo
        self._tx_ops = tx_ops
        self._snapshot_repo = snapshot_repo
        self._snapshot_ops = snapshot_ops
        self._source_document_normalizer = source_document_normalizer

    def list_processed_doc_keys(self) -> set[str]:
        return {
            self._source_document_normalizer.canonical_source_key(path)
            for path in self._processed_files.list_file_paths()
        }

    def is_file_hash_processed(self, file_hash: str) -> bool:
        return self._processed_files.exists_by_file_hash(file_hash)

    def record_ignored_document(self, *, file_path: str, file_hash: str, parser_version: str) -> None:
        self._processed_file_ops.create(
            ProcessedFileWriteModel(
                file_path=file_path,
                file_hash=file_hash,
                parser_version=parser_version,
            )
        )

    def store_transaction_document(self, document: TransactionDocument, *, file_hash: str, bank: str) -> None:
        product = self._products.upsert(
            ProductUpsertModel(
                wkn=document.transaction.wkn,
                isin=document.transaction.isin,
                name=document.transaction.product_name,
            )
        )
        if not self._tx_repo.exists_by_source_hash(file_hash):
            self._tx_ops.create(
                TransactionWriteModel(
                    product_id=product.id,
                    type=document.transaction.tx_type,
                    transaction_date=document.transaction.transaction_date,
                    quantity=document.transaction.quantity,
                    gross_amount=document.transaction.gross_amount,
                    costs=document.transaction.costs,
                    bank=bank,
                    currency="EUR",
                    source_file=str(document.file_path),
                    source_hash=file_hash,
                )
            )
        self._processed_file_ops.create(
            ProcessedFileWriteModel(
                file_path=str(document.file_path),
                file_hash=file_hash,
                parser_version=document.parser.parser_version,
            )
        )

    def store_holdings_document(self, document: HoldingsDocument, *, file_hash: str) -> None:
        for holding in document.holdings:
            product = self._products.upsert(
                ProductUpsertModel(
                    wkn=holding.wkn,
                    isin=holding.isin,
                    name=holding.product_name,
                )
            )
            if self._snapshot_repo.exists_by_product_and_source_hash(
                    product_id=product.id,
                    source_hash=file_hash,
            ):
                continue
            self._snapshot_ops.create(
                HoldingSnapshotWriteModel(
                    product_id=product.id,
                    snapshot_date=document.snapshot_date,
                    quantity=holding.quantity,
                    snapshot_price=holding.snapshot_price,
                    source_file=str(document.file_path),
                    source_hash=file_hash,
                )
            )

        self._processed_file_ops.create(
            ProcessedFileWriteModel(
                file_path=str(document.file_path),
                file_hash=file_hash,
                parser_version=f"{document.parser.parser_version}-depotauszug",
            )
        )
