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
from depot_tracking.components.cli.portfolio_support import MonthArgumentParser, PortfolioCliOutput
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
from depot_tracking.components.reporting import plot_portfolio_monthly_history
from depot_tracking.config import BankingAppConfig
from depot_tracking.core.db import initialize_database


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

        if args.command == "backfill-monthly-values":
            start_month = self._month_parser.parse(args.start_month, "--start-month")
            end_month = self._month_parser.parse(args.end_month, "--end-month")
            market_stats = self._analytics_service.backfill_monthly_market_values_from_yahoo(
                start_month=start_month,
                end_month=end_month,
            )
            history_stats = None
            if not args.skip_history_rebuild:
                history_stats = self._analytics_service.build_portfolio_monthly_history(
                    start_month=start_month,
                    end_month=end_month,
                )
            self._output.print_backfill_stats(market_stats, history_stats)
            total_errors = market_stats.get("errors", 0) + (history_stats or {}).get("errors", 0)
            return 0 if total_errors == 0 else 1

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
