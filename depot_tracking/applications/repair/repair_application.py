import argparse

from python_di_application.application import Application
from python_di_application.di_container import DIContainer, Dependency

from depot_tracking.applications.di import (
    register_default_instances,
    register_session_dependencies,
    register_shared_dependencies,
    resolve_application,
)
from depot_tracking.components.analytics import AnalyticsService
from depot_tracking.components.cli.repair_output import RepairCliOutput
from depot_tracking.components.data_operations.asset_value_data_operations import AssetValueDataOperations
from depot_tracking.components.data_operations.asset_value_repository import AssetValueRepository
from depot_tracking.components.data_operations.holding_snapshot_repository import HoldingSnapshotRepository
from depot_tracking.components.data_operations.portfolio_monthly_history_data_operations import \
    PortfolioMonthlyHistoryDataOperations
from depot_tracking.components.data_operations.product_data_operations import ProductDataOperations
from depot_tracking.components.data_operations.product_repository import ProductRepository
from depot_tracking.components.data_operations.source_document_data_operations import SourceDocumentDataOperations
from depot_tracking.components.data_operations.transaction_data_operations import TransactionDataOperations
from depot_tracking.components.data_operations.transaction_repository import TransactionRepository
from depot_tracking.components.market import YahooMarketDataClient
from depot_tracking.components.repair import RepairService
from depot_tracking.config import BankingAppConfig
from depot_tracking.core.db import initialize_database


class RepairApplication(Application):
    def __init__(
            self,
            app_config: BankingAppConfig,
            analytics_service: AnalyticsService,
            repair_service: RepairService,
            repair_output: RepairCliOutput,
    ) -> None:
        self._db_path = app_config.db_path
        self._analytics_service = analytics_service
        self._repair_service = repair_service
        self._repair_output = repair_output

    @classmethod
    def _default_container(cls) -> DIContainer:
        container = DIContainer()
        register_session_dependencies(container)
        register_shared_dependencies(container)
        container.register_dependencies(
            dependencies_types_with_kwargs=[
                Dependency(dependency_type=RepairService),
                Dependency(dependency_type=YahooMarketDataClient),
                Dependency(dependency_type=AnalyticsService),
                Dependency(dependency_type=HoldingSnapshotRepository),
                Dependency(dependency_type=ProductDataOperations),
                Dependency(dependency_type=ProductRepository),
                Dependency(dependency_type=SourceDocumentDataOperations),
                Dependency(dependency_type=TransactionRepository),
                Dependency(dependency_type=TransactionDataOperations),
                Dependency(dependency_type=AssetValueRepository),
                Dependency(dependency_type=AssetValueDataOperations),
                Dependency(dependency_type=PortfolioMonthlyHistoryDataOperations),
                Dependency(dependency_type=RepairCliOutput),
                Dependency(dependency_type=cls),
            ]
        )
        register_default_instances(container)
        return container

    @classmethod
    def _build(cls, container: DIContainer) -> tuple[DIContainer, "RepairApplication"]:
        return resolve_application(container, cls)

    def run(self, args: argparse.Namespace) -> int:
        initialize_database(self._db_path)

        if args.command == "infer-buys":
            stats = self._analytics_service.infer_missing_buys_from_holdings()
            self._repair_output.print_infer_stats(stats)
            return 0 if stats["errors"] == 0 else 1

        if args.command == "repair-db":
            stats = self._repair_service.repair_known_data_issues()
            self._repair_output.print_repair_stats(stats)
            return 0 if stats["errors"] == 0 else 1

        raise SystemExit("Unsupported command")
