from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class HistoryPoint:
    month_date: datetime
    invested_amount_eur: float
    portfolio_value_eur: float
    portfolio_profit_eur: float


def load_portfolio_monthly_history(db_path: str | Path) -> list[HistoryPoint]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT month_date, invested_amount_eur, portfolio_value_eur, portfolio_profit_eur
            FROM v_portfolio_monthly_history
            ORDER BY month_date
            """
        ).fetchall()

    points: list[HistoryPoint] = []
    for month_date, invested_amount, portfolio_value, portfolio_profit in rows:
        dt = datetime.strptime(str(month_date), "%Y-%m-%d")
        points.append(
            HistoryPoint(
                month_date=dt,
                invested_amount_eur=float(invested_amount),
                portfolio_value_eur=float(portfolio_value),
                portfolio_profit_eur=float(portfolio_profit),
            )
        )
    return points


def plot_portfolio_monthly_history(
    db_path: str | Path,
    *,
    output_file: str | Path = "portfolio_monthly_history.png",
    title: str = "Portfolio Monthly History",
    interactive: bool = False,
) -> Path:
    points = load_portfolio_monthly_history(db_path)
    if not points:
        raise RuntimeError(
            "No rows found in v_portfolio_monthly_history. Build history first with "
            "'build-monthly-history' or the backfill script."
        )

    try:
        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt
        from matplotlib.ticker import FuncFormatter
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "matplotlib is required for plotting. Install it (e.g. `uv add matplotlib`) and retry."
        ) from exc

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    x = [point.month_date for point in points]
    portfolio_value = [point.portfolio_value_eur for point in points]
    portfolio_profit = [point.portfolio_profit_eur for point in points]
    realized_earnings = [
        point.portfolio_profit_eur - (point.portfolio_value_eur - point.invested_amount_eur)
        for point in points
    ]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(x, portfolio_value, label="Portfolio Value", linewidth=2.4, color="#0f766e")
    ax.plot(x, portfolio_profit, label="Portfolio Profit", linewidth=2.4, color="#b45309")
    ax.plot(x, realized_earnings, label="Realized Earnings", linewidth=2.2, color="#4338ca")

    ax.set_title(title)
    ax.set_xlabel("Time")
    ax.set_ylabel("EUR")
    ax.grid(True, alpha=0.3)
    ax.legend()

    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _pos: f"{v:,.0f} EUR"))
    fig.autofmt_xdate()

    fig.tight_layout()
    fig.savefig(output_path, dpi=170)
    if interactive:
        backend = str(plt.get_backend()).lower()
        if "agg" in backend:
            raise RuntimeError(
                "Interactive plotting requested, but the active matplotlib backend is non-interactive "
                f"({backend}). Use a GUI backend (for example, qtagg or tkagg) and retry."
            )
        plt.show()
    plt.close(fig)
    return output_path
