import argparse

from python_di_application.application import Application
from python_di_application.di_container import DIContainer, Dependency

from banking_app.applications.di import (
    register_default_instances,
    register_session_dependencies,
    register_shared_dependencies,
    resolve_application,
)
from banking_app.components.cli.ingestion_output import IngestionCliOutput
from banking_app.components.data_operations.asset_value_data_operations import AssetValueDataOperations
from banking_app.components.data_operations.asset_value_repository import AssetValueRepository
from banking_app.components.data_operations.holding_snapshot_data_operations import HoldingSnapshotDataOperations
from banking_app.components.data_operations.holding_snapshot_repository import HoldingSnapshotRepository
from banking_app.components.data_operations.portfolio_monthly_history_data_operations import \
    PortfolioMonthlyHistoryDataOperations
from banking_app.components.data_operations.processed_file_data_operations import ProcessedFileDataOperations
from banking_app.components.data_operations.processed_file_repository import ProcessedFileRepository
from banking_app.components.data_operations.product_data_operations import ProductDataOperations
from banking_app.components.data_operations.product_repository import ProductRepository
from banking_app.components.data_operations.source_document_data_operations import SourceDocumentDataOperations
from banking_app.components.data_operations.transaction_data_operations import TransactionDataOperations
from banking_app.components.data_operations.transaction_repository import TransactionRepository
from banking_app.components.ingestion import DocumentDeduplicationService, IngestionService
from banking_app.components.ingestion import DocumentRouter, IngestionStore
from banking_app.components.ingestion.parsing import ParserFactory
from banking_app.config import BankingAppConfig
from banking_app.core.db import initialize_database


class IngestionApplication(Application):
    def __init__(
            self,
            app_config: BankingAppConfig,
            ingestion_service: IngestionService,
            deduplication_service: DocumentDeduplicationService,
            output: IngestionCliOutput,
    ) -> None:
        self._db_path = app_config.db_path
        self._ingestion_service = ingestion_service
        self._deduplication_service = deduplication_service
        self._output = output

    @classmethod
    def _default_container(cls) -> DIContainer:
        container = DIContainer()
        register_session_dependencies(container)
        register_shared_dependencies(container)
        container.register_dependencies(
            dependencies_types_with_kwargs=[
                Dependency(dependency_type=ParserFactory),
                Dependency(dependency_type=DocumentRouter),
                Dependency(dependency_type=IngestionStore),
                Dependency(dependency_type=IngestionService),
                Dependency(dependency_type=DocumentDeduplicationService),
                Dependency(dependency_type=SourceDocumentDataOperations),
                Dependency(dependency_type=ProductRepository),
                Dependency(dependency_type=ProductDataOperations),
                Dependency(dependency_type=TransactionRepository),
                Dependency(dependency_type=TransactionDataOperations),
                Dependency(dependency_type=HoldingSnapshotRepository),
                Dependency(dependency_type=HoldingSnapshotDataOperations),
                Dependency(dependency_type=ProcessedFileRepository),
                Dependency(dependency_type=ProcessedFileDataOperations),
                Dependency(dependency_type=AssetValueRepository),
                Dependency(dependency_type=AssetValueDataOperations),
                Dependency(dependency_type=PortfolioMonthlyHistoryDataOperations),
                Dependency(dependency_type=IngestionCliOutput),
                Dependency(dependency_type=cls),
            ]
        )
        register_default_instances(container)
        return container

    @classmethod
    def _build(cls, container: DIContainer) -> tuple[DIContainer, "IngestionApplication"]:
        return resolve_application(container, cls)

    def run(self, args: argparse.Namespace) -> int:
        initialize_database(self._db_path)

        if args.command == "init-db":
            print(f"Initialized database: {self._db_path}")
            return 0

        if args.command == "ingest":
            stats = self._ingestion_service.ingest_directory(args.pdf_dir)
            self._output.print_ingest_stats(stats)
            return 0

        if args.command == "dedupe-docs":
            stats = self._deduplication_service.cleanup_duplicate_documents(args.pdf_dir)
            self._output.print_dedupe_stats(stats)
            return 0

        raise SystemExit("Unsupported command")
