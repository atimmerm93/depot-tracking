from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from banking_app.components.service import BankingService
from banking_app.core.db import initialize_database


def _parse_month(value: str | None, arg_name: str) -> date | None:
    if value is None:
        return None
    try:
        parsed = datetime.strptime(value, "%Y-%m").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid {arg_name} value '{value}'. Expected format YYYY-MM.") from exc
    return parsed.replace(day=1)


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
    parser = build_parser()
    args = parser.parse_args(argv)

    db_path = Path(args.db_path)
    initialize_database(db_path)
    service = BankingService(db_path)

    start_month = _parse_month(args.start_month, "--start-month")
    end_month = _parse_month(args.end_month, "--end-month")

    market_stats = service.backfill_monthly_market_values_from_yahoo(
        start_month=start_month,
        end_month=end_month,
    )
    print(
        "Monthly Yahoo backfill summary: "
        f"months={market_stats.get('months', 0)}, "
        f"positions={market_stats.get('positions', 0)}, "
        f"created={market_stats.get('created', 0)}, "
        f"updated={market_stats.get('updated', 0)}, "
        f"errors={market_stats.get('errors', 0)}"
    )

    if args.skip_history_rebuild:
        return 0 if market_stats.get("errors", 0) == 0 else 1

    history_stats = service.build_portfolio_monthly_history(
        start_month=start_month,
        end_month=end_month,
    )
    print(
        "Monthly history rebuild summary: "
        f"months={history_stats.get('months', 0)}, "
        f"created={history_stats.get('created', 0)}, "
        f"updated={history_stats.get('updated', 0)}, "
        f"errors={history_stats.get('errors', 0)}"
    )
    return 0 if (market_stats.get("errors", 0) + history_stats.get("errors", 0)) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
