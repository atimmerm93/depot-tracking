import argparse

from python_di_application.application import Application
from python_di_application.di_container import DIContainer, Dependency

from banking_app.applications.di import (
    register_default_instances,
    register_session_dependencies,
    register_shared_dependencies,
    resolve_application,
)
from banking_app.components.analytics import AnalyticsService
from banking_app.components.cli.portfolio_support import MonthArgumentParser, PortfolioCliOutput
from banking_app.components.data_operations.asset_value_data_operations import AssetValueDataOperations
from banking_app.components.data_operations.asset_value_repository import AssetValueRepository
from banking_app.components.data_operations.holding_snapshot_repository import HoldingSnapshotRepository
from banking_app.components.data_operations.portfolio_monthly_history_data_operations import \
    PortfolioMonthlyHistoryDataOperations
from banking_app.components.data_operations.product_data_operations import ProductDataOperations
from banking_app.components.data_operations.product_repository import ProductRepository
from banking_app.components.data_operations.source_document_data_operations import SourceDocumentDataOperations
from banking_app.components.data_operations.transaction_data_operations import TransactionDataOperations
from banking_app.components.data_operations.transaction_repository import TransactionRepository
from banking_app.components.market import YahooMarketDataClient
from banking_app.components.reporting import plot_portfolio_monthly_history
from banking_app.config import BankingAppConfig
from banking_app.core.db import initialize_database


class PortfolioApplication(Application):
    def __init__(
            self,
            app_config: BankingAppConfig,
            analytics_service: AnalyticsService,
            output: PortfolioCliOutput,
            month_parser: MonthArgumentParser,
    ) -> None:
        self._db_path = app_config.db_path
        self._analytics_service = analytics_service
        self._output = output
        self._month_parser = month_parser

    @classmethod
    def _default_container(cls) -> DIContainer:
        container = DIContainer()
        register_session_dependencies(container)
        register_shared_dependencies(container)
        container.register_dependencies(
            dependencies_types_with_kwargs=[
                Dependency(dependency_type=YahooMarketDataClient),
                Dependency(dependency_type=AnalyticsService),
                Dependency(dependency_type=HoldingSnapshotRepository),
                Dependency(dependency_type=ProductRepository),
                Dependency(dependency_type=ProductDataOperations),
                Dependency(dependency_type=SourceDocumentDataOperations),
                Dependency(dependency_type=TransactionRepository),
                Dependency(dependency_type=TransactionDataOperations),
                Dependency(dependency_type=AssetValueRepository),
                Dependency(dependency_type=AssetValueDataOperations),
                Dependency(dependency_type=PortfolioMonthlyHistoryDataOperations),
                Dependency(dependency_type=PortfolioCliOutput),
                Dependency(dependency_type=MonthArgumentParser),
                Dependency(dependency_type=cls),
            ]
        )
        register_default_instances(container)
        return container

    @classmethod
    def _build(cls, container: DIContainer) -> tuple[DIContainer, "PortfolioApplication"]:
        return resolve_application(container, cls)

    def run(self, args: argparse.Namespace) -> int:
        initialize_database(self._db_path)

        if args.command == "update-values":
            stats = self._analytics_service.update_open_asset_values()
            self._output.print_value_stats(stats)
            return 0 if args.ignore_network_errors or stats["errors"] == 0 else 1

        if args.command == "build-monthly-history":
            stats = self._analytics_service.build_portfolio_monthly_history(
                start_month=self._month_parser.parse(args.start_month, "--start-month"),
                end_month=self._month_parser.parse(args.end_month, "--end-month"),
            )
            self._output.print_monthly_history_stats(stats)
            return 0 if stats["errors"] == 0 else 1

        if args.command == "plot-history":
            output_path = plot_portfolio_monthly_history(
                self._db_path,
                output_file=args.output_file,
                title=args.title,
                interactive=args.interactive,
            )
            print(f"Saved portfolio history plot: {output_path}")
            return 0

        if args.command == "report":
            self._output.print_report(
                total=self._analytics_service.fetch_current_profit(),
                rows=self._analytics_service.fetch_product_profit(),
                limit=args.limit,
            )
            return 0

        raise SystemExit("Unsupported command")
