from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from depot_tracking.applications.cli import main as cli_main


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backfill-monthly-yahoo-history",
        description="Backfill month-end Yahoo EUR asset values and rebuild portfolio monthly history.",
    )
    parser.add_argument("--db-path", default="banking.sqlite", help="Path to SQLite database file")
    parser.add_argument("--start-month", default=None, help="Optional start month (YYYY-MM)")
    parser.add_argument("--end-month", default=None, help="Optional end month (YYYY-MM)")
    parser.add_argument(
        "--skip-history-rebuild",
        action="store_true",
        help="Only backfill month-end Yahoo values, do not rebuild portfolio_monthly_history",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    cli_argv = ["--db-path", args.db_path, "backfill-monthly-values"]
    if args.start_month:
        cli_argv += ["--start-month", args.start_month]
    if args.end_month:
        cli_argv += ["--end-month", args.end_month]
    if args.skip_history_rebuild:
        cli_argv.append("--skip-history-rebuild")

    return cli_main(cli_argv)


if __name__ == "__main__":
    raise SystemExit(main())
