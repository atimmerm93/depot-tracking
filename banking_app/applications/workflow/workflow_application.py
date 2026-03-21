import argparse
import time

from python_di_application.application import Application
from python_di_application.di_container import DIContainer, Dependency

from banking_app.applications.di import (
    register_default_instances,
    register_session_dependencies,
    register_shared_dependencies,
    resolve_application,
)
from banking_app.components.analytics import AnalyticsService
from banking_app.components.cli import IngestionCliOutput, PortfolioCliOutput, RepairCliOutput
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
from banking_app.components.ingestion import DocumentRouter, IngestionService, IngestionStore
from banking_app.components.ingestion.parsing import ParserFactory
from banking_app.components.market import YahooMarketDataClient
from banking_app.config import BankingAppConfig
from banking_app.core.db import initialize_database


class WorkflowApplication(Application):
    def __init__(
            self,
            app_config: BankingAppConfig,
            ingestion_service: IngestionService,
            analytics_service: AnalyticsService,
            ingestion_output: IngestionCliOutput,
            repair_output: RepairCliOutput,
            portfolio_output: PortfolioCliOutput,
    ) -> None:
        self._db_path = app_config.db_path
        self._ingestion_service = ingestion_service
        self._analytics_service = analytics_service
        self._ingestion_output = ingestion_output
        self._repair_output = repair_output
        self._portfolio_output = portfolio_output

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
                Dependency(dependency_type=YahooMarketDataClient),
                Dependency(dependency_type=AnalyticsService),
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
                Dependency(dependency_type=RepairCliOutput),
                Dependency(dependency_type=PortfolioCliOutput),
                Dependency(dependency_type=cls),
            ]
        )
        register_default_instances(container)
        return container

    @classmethod
    def _build(cls, container: DIContainer) -> tuple[DIContainer, "WorkflowApplication"]:
        return resolve_application(container, cls)

    def run(self, args: argparse.Namespace) -> int:
        initialize_database(self._db_path)

        if args.command == "run-once":
            return self._run_once(args)

        if args.command == "monitor":
            return self._monitor(args)

        raise SystemExit("Unsupported command")

    def _run_once(self, args: argparse.Namespace) -> int:
        ingest_stats = self._ingestion_service.ingest_directory(args.pdf_dir)
        self._ingestion_output.print_ingest_stats(ingest_stats)

        infer_stats = self._analytics_service.infer_missing_buys_from_holdings()
        self._repair_output.print_infer_stats(infer_stats)

        value_stats = self._analytics_service.update_open_asset_values()
        self._portfolio_output.print_value_stats(value_stats)

        history_stats = self._analytics_service.build_portfolio_monthly_history()
        self._portfolio_output.print_monthly_history_stats(history_stats)

        self._portfolio_output.print_report(
            total=self._analytics_service.fetch_current_profit(),
            rows=self._analytics_service.fetch_product_profit(),
            limit=args.limit,
        )

        has_errors = any(
            stats["errors"] > 0
            for stats in (ingest_stats, infer_stats, value_stats, history_stats)
        )
        return 1 if has_errors else 0

    def _monitor(self, args: argparse.Namespace) -> int:
        while True:
            exit_code = self._run_once(args)
            if exit_code != 0:
                return exit_code
            time.sleep(args.interval_seconds)
