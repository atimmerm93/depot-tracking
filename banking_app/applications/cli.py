from __future__ import annotations

import argparse
from pathlib import Path

from di_unit_of_work.session_factory.sqlite_session_factory import SqlLiteConfig
from python_di_application.dependency import DependencyInstance

from banking_app.applications.download.download_service.downloader_factory import SUPPORTED_DOWNLOADER_BANKS
from banking_app.applications.download.downloading_application import DownloadingApplication
from banking_app.applications.ingestion.ingestion_application import IngestionApplication
from banking_app.applications.portfolio.portfolio_application import PortfolioApplication
from banking_app.applications.repair.repair_application import RepairApplication
from banking_app.applications.workflow.workflow_application import WorkflowApplication
from banking_app.components.ingestion.parsing import SUPPORTED_BANKS as SUPPORTED_PARSER_BANKS
from ..config import BankingAppConfig, ParserConfig
from ..core.models import Base

INGESTION_COMMANDS = {"init-db", "ingest", "dedupe-docs"}
DOWNLOADING_COMMANDS = {"download-docs"}
REPAIR_COMMANDS = {
    "infer-buys",
    "repair-db",
}
PORTFOLIO_COMMANDS = {
    "update-values",
    "build-monthly-history",
    "plot-history",
    "report",
}
WORKFLOW_COMMANDS = {
    "run-once",
    "monitor",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="banking-analyzer",
        description="Analyze banking PDFs into SQLite and compute portfolio profit.",
    )
    parser.add_argument("--db-path", default="banking.sqlite", help="Path to SQLite database file")
    parser.add_argument(
        "--parser-bank",
        default="auto",
        choices=("auto", *SUPPORTED_PARSER_BANKS),
        help="PDF parser selection for ingestion commands (auto/ing/consors/trade_republic)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Create or migrate the SQLite schema")

    ingest = subparsers.add_parser("ingest", help="Ingest new PDF transactions from folder")
    ingest.add_argument("--pdf-dir", default="incoming_pdfs", help="Folder containing PDF files")

    values = subparsers.add_parser("update-values", help="Fetch and store current values for open positions")
    values.add_argument("--ignore-network-errors", action="store_true", help="Keep exit code 0 on fetch failures")

    subparsers.add_parser(
        "infer-buys",
        help="Infer missing BUY transactions from Depotauszug holdings using historical market prices",
    )
    subparsers.add_parser(
        "repair-db",
        help="Apply known data repairs (alias duplicates and stock split adjustments)",
    )
    monthly_history = subparsers.add_parser(
        "build-monthly-history",
        help="Compute and store portfolio history snapshots per month",
    )
    monthly_history.add_argument("--start-month", default=None, help="Optional start month (YYYY-MM)")
    monthly_history.add_argument("--end-month", default=None, help="Optional end month (YYYY-MM)")
    plot_history = subparsers.add_parser(
        "plot-history",
        help="Draw a line chart from v_portfolio_monthly_history",
    )
    plot_history.add_argument("--output-file", default="portfolio_monthly_history.png", help="Output image file path")
    plot_history.add_argument("--title", default="Portfolio Monthly History", help="Plot title")
    plot_history.add_argument(
        "--interactive",
        action="store_true",
        help="Open an interactive matplotlib window (zoom/pan available)",
    )

    report = subparsers.add_parser("report", help="Print current profit report")
    report.add_argument("--limit", type=int, default=20, help="Max rows in product report")

    run_once = subparsers.add_parser(
        "run-once",
        help="Ingest PDFs, infer buys, refresh values, rebuild monthly history, and print report",
    )
    run_once.add_argument("--pdf-dir", default="incoming_pdfs", help="Folder containing PDF files")
    run_once.add_argument("--limit", type=int, default=20, help="Max rows in product report")

    dedupe = subparsers.add_parser(
        "dedupe-docs",
        help="Delete duplicate *_N.pdf files and remove duplicated DB rows created from them",
    )
    dedupe.add_argument("--pdf-dir", default="incoming_pdfs", help="Folder containing PDF files")

    monitor = subparsers.add_parser("monitor", help="Poll folder continuously and update values")
    monitor.add_argument("--pdf-dir", default="incoming_pdfs", help="Folder containing PDF files")
    monitor.add_argument("--interval-seconds", type=int, default=60, help="Polling interval in seconds")
    monitor.add_argument("--limit", type=int, default=20, help="Max rows in product report")

    download = subparsers.add_parser(
        "download-docs",
        help="Attach to your current Chrome tab and download supported bank documents",
    )
    download.add_argument(
        "--debugger-address",
        default="127.0.0.1:9222",
        help="Chrome DevTools address used for attaching to your current tab",
    )
    download.add_argument("--download-dir", default="incoming_pdfs", help="Directory where PDFs are saved")
    download.add_argument(
        "--bank",
        default="ing",
        choices=SUPPORTED_DOWNLOADER_BANKS,
        help="Selenium downloader selection",
    )
    download.add_argument("--state-file", default=None, help="Optional JSON state file for already downloaded rows")
    download.add_argument(
        "--reset-state",
        action="store_true",
        help="Ignore saved Selenium row signatures and process rows as fresh",
    )
    download.add_argument("--max-documents", type=int, default=None, help="Stop after N successful downloads")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command in INGESTION_COMMANDS:
        app_cls = IngestionApplication
    elif args.command in DOWNLOADING_COMMANDS:
        app_cls = DownloadingApplication
    elif args.command in REPAIR_COMMANDS:
        app_cls = RepairApplication
    elif args.command in PORTFOLIO_COMMANDS:
        app_cls = PortfolioApplication
    elif args.command in WORKFLOW_COMMANDS:
        app_cls = WorkflowApplication
    else:
        raise SystemExit(f"Unsupported command: {args.command}")

    if app_cls is DownloadingApplication:
        app = app_cls.build(ignore_unused_dependencies=True)
        return app.run(args)

    app = app_cls.build(
        override_instances=[
            DependencyInstance(BankingAppConfig(db_path=Path(args.db_path))),
            DependencyInstance(SqlLiteConfig(path=str(args.db_path), metadata=Base.metadata)),
            DependencyInstance(ParserConfig(bank_hint=args.parser_bank)),
        ],
        ignore_unused_dependencies=True,
    )
    return app.run(args)


if __name__ == "__main__":
    raise SystemExit(main())
