from datetime import date
from pathlib import Path

from di_unit_of_work.transactional_decorator import transactional

from banking_app.components.shared import BankClassifier, SourceDocumentNormalizer
from banking_app.config import ParserConfig
from banking_app.core.db import sha256_file
from banking_app.core.models import ParsedHolding, ParsedTransaction
from .document_router import DocumentRouter
from .models import (
    DocumentParseFailure,
    HoldingsDocument,
    IgnoredDocument,
    IngestionFileResult,
    ParsedDocument,
)
from .parsing.parser_factory import BankPdfParser
from .store import IngestionStore


class IngestionService:
    def __init__(
            self,
            *,
            parser_config: ParserConfig,
            router: DocumentRouter,
            store: IngestionStore,
            source_document_normalizer: SourceDocumentNormalizer,
            bank_classifier: BankClassifier,
    ) -> None:
        self._parser_config = parser_config
        self._router = router
        self._store = store
        self._source_document_normalizer = source_document_normalizer
        self._bank_classifier = bank_classifier

    def ingest_directory(self, pdf_dir: str | Path) -> dict[str, int]:
        directory = Path(pdf_dir)
        directory.mkdir(parents=True, exist_ok=True)

        stats = {"seen": 0, "ingested": 0, "skipped": 0, "errors": 0}
        processed_doc_keys = self._store.list_processed_doc_keys()
        for pdf_file in sorted(directory.glob("*.pdf")):
            result = self.ingest_file(pdf_file, processed_doc_keys=processed_doc_keys)
            result.apply_to(stats)
            self._log_file_result(result)

        return stats

    @transactional
    def ingest_file(self, pdf_file: str | Path, *, processed_doc_keys: set[str] | None = None) -> IngestionFileResult:
        file_path = Path(pdf_file)
        canonical_doc_key = self._source_document_normalizer.canonical_source_key(str(file_path))
        if processed_doc_keys is not None and canonical_doc_key in processed_doc_keys:
            return IngestionFileResult(file_path=file_path, skipped=1)

        file_hash = sha256_file(file_path)
        if self._store.is_file_hash_processed(file_hash):
            return IngestionFileResult(file_path=file_path, skipped=1)

        document = self._router.parse_document(file_path)
        result = self._persist_document(document=document, file_hash=file_hash)
        if result.ingested and processed_doc_keys is not None:
            processed_doc_keys.add(canonical_doc_key)
        return result

    def _persist_document(
            self,
            *,
            document: ParsedDocument,
            file_hash: str,
    ) -> IngestionFileResult:
        if isinstance(document, DocumentParseFailure):
            return IngestionFileResult(
                file_path=document.file_path,
                errors=1,
                log_message=f"[INGEST][ERROR] {document.file_path.name}: {document.error}",
            )
        if isinstance(document, IgnoredDocument):
            self._store.record_ignored_document(
                file_path=str(document.file_path),
                file_hash=file_hash,
                parser_version=document.parser_version,
            )
            return IngestionFileResult(
                file_path=document.file_path,
                skipped=1,
                log_message=f"[INGEST][SKIP] {document.reason}",
            )
        if isinstance(document, HoldingsDocument):
            self._store.store_holdings_document(document, file_hash=file_hash)
            return IngestionFileResult(
                file_path=document.file_path,
                ingested=1,
                log_message=(
                    f"[INGEST][HOLDINGS] {document.file_path.name}: "
                    f"{len(document.holdings)} positions on {document.snapshot_date.isoformat()}"
                ),
            )

        bank = self._bank_classifier.infer_bank_from_file_path(
            str(document.file_path),
            parser_bank_hint=self._parser_config.bank_hint,
        )
        self._store.store_transaction_document(document, file_hash=file_hash, bank=bank)
        return IngestionFileResult(file_path=document.file_path, ingested=1)

    def _parse_transaction(self, pdf_file: Path) -> tuple[ParsedTransaction, BankPdfParser]:
        return self._router.parse_transaction(pdf_file)

    def _parse_depotauszug_holdings(self, pdf_file: Path) -> tuple[date, list[ParsedHolding], BankPdfParser]:
        return self._router.parse_depotauszug_holdings(pdf_file)

    @staticmethod
    def _log_file_result(result: IngestionFileResult) -> None:
        if result.log_message:
            print(result.log_message)
