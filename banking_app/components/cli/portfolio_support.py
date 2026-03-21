from datetime import date, datetime
from typing import Any


class PortfolioCliOutput:
    @staticmethod
    def print_value_stats(stats: dict[str, int]) -> None:
        fallbacks = stats.get("fallbacks", 0)
        print(
            "Value update summary: "
            f"positions={stats['positions']}, updated={stats['updated']}, fallbacks={fallbacks}, errors={stats['errors']}"
        )

    @staticmethod
    def print_monthly_history_stats(stats: dict[str, int]) -> None:
        print(
            "Monthly history summary: "
            f"months={stats.get('months', 0)}, "
            f"created={stats.get('created', 0)}, "
            f"updated={stats.get('updated', 0)}, "
            f"errors={stats.get('errors', 0)}"
        )

    @staticmethod
    def print_report(*, total: dict[str, float], rows: list[dict[str, Any]], limit: int) -> None:
        print(
            "Current profit: "
            f"total_profit={total['total_profit']} EUR, "
            f"current_portfolio_value={total['current_portfolio_value']} EUR, "
            f"net_cashflow={total['net_cashflow']} EUR"
        )

        rows = rows[:limit]
        if not rows:
            print("No product transactions found yet.")
            return

        print("Top products by realized+unrealized profit:")
        for row in rows:
            print(
                f"- {row['wkn']} | {row['name']} | qty={row['quantity_open']} | "
                f"profit={row['profit']} EUR | invested={row['invested_eur']} EUR | value={row['current_value']} EUR"
            )


class MonthArgumentParser:
    @staticmethod
    def parse(value: str | None, arg_name: str) -> date | None:
        if value is None:
            return None
        try:
            parsed = datetime.strptime(value, "%Y-%m").date()
        except ValueError as exc:
            raise SystemExit(f"Invalid {arg_name} value '{value}'. Expected format YYYY-MM.") from exc
        return parsed.replace(day=1)
