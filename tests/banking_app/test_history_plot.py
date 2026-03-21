from __future__ import annotations

from pathlib import Path

import pytest
from di_unit_of_work.session_factory.sqlite_session_factory import SqlLiteConfig, SQLiteSessionFactory
from sqlalchemy import text

from banking_app.components.reporting.history_plot import load_portfolio_monthly_history, plot_portfolio_monthly_history
from banking_app.core.db import initialize_database
from banking_app.core.models import Base


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "history_plot.sqlite"
    initialize_database(path)
    return path


def test_load_portfolio_monthly_history_reads_sorted_rows(db_path: Path) -> None:
    session_factory = SQLiteSessionFactory(SqlLiteConfig(path=str(db_path), metadata=Base.metadata))
    with session_factory() as session:
        session.execute(
            text(
                """
                INSERT INTO portfolio_monthly_history (month_date, month_end_date, invested_amount_eur,
                                                       portfolio_value_eur, portfolio_profit_eur, source)
                VALUES ('2024-02-01', '2024-02-29', 800.0, 960.0, 230.0, 'computed'),
                       ('2024-01-01', '2024-01-31', 1000.0, 1100.0, 100.0, 'computed')
                """
            )
        )
        session.commit()

    rows = load_portfolio_monthly_history(db_path)
    assert len(rows) == 2
    assert rows[0].month_date.isoformat().startswith("2024-01-01")
    assert rows[0].invested_amount_eur == pytest.approx(1000.0)
    assert rows[0].portfolio_value_eur == pytest.approx(1100.0)
    assert rows[1].month_date.isoformat().startswith("2024-02-01")
    assert rows[1].portfolio_profit_eur == pytest.approx(230.0)


def test_plot_portfolio_monthly_history_raises_when_no_data(db_path: Path) -> None:
    with pytest.raises(RuntimeError, match="No rows found in v_portfolio_monthly_history"):
        plot_portfolio_monthly_history(db_path, output_file="ignored.png")
