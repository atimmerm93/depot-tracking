from __future__ import annotations

import sqlite3
from pathlib import Path

from di_unit_of_work.session_factory.sqlite_session_factory import SqlLiteConfig, SQLiteSessionFactory
from sqlalchemy import text

from depot_tracking.core.db import initialize_database
from depot_tracking.core.models import Base


def _insert_source_document(conn: sqlite3.Connection, *, source_file: str, source_hash: str) -> int:
    conn.execute(
        "INSERT INTO source_documents (file_path, file_hash) VALUES (?, ?)",
        (source_file, source_hash),
    )
    row = conn.execute("SELECT id FROM source_documents WHERE file_hash = ?", (source_hash,)).fetchone()
    assert row is not None
    return int(row[0])


def _insert_transaction(
        conn: sqlite3.Connection,
        *,
        product_id: int,
        tx_type: str,
        transaction_date: str,
        quantity: float,
        gross_amount: float,
        costs: float,
        currency: str,
        source_file: str,
        source_hash: str,
        bank: str = "UNKNOWN",
) -> None:
    source_document_id = _insert_source_document(conn, source_file=source_file, source_hash=source_hash)
    conn.execute(
        """
        INSERT INTO transactions (product_id, source_document_id, type, transaction_date, quantity, gross_amount, costs,
                                  currency, bank)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (product_id, source_document_id, tx_type, transaction_date, quantity, gross_amount, costs, currency, bank),
    )


def test_initialize_database_creates_profit_views(tmp_path: Path) -> None:
    db_path = tmp_path / "schema.sqlite"
    initialize_database(db_path)

    session_factory = SQLiteSessionFactory(SqlLiteConfig(path=str(db_path), metadata=Base.metadata))
    with session_factory() as session:
        row = session.execute(text("SELECT name FROM sqlite_master WHERE type='view' AND name='v_current_profit'"))
        result = row.first()

    assert result is not None
    with session_factory() as session:
        sold_row = session.execute(
            text("SELECT name FROM sqlite_master WHERE type='view' AND name='v_sold_transactions'"))
        sold_result = sold_row.first()
        ertrag_row = session.execute(
            text("SELECT name FROM sqlite_master WHERE type='view' AND name='v_ertragsabrechnungen_by_year'")
        )
        ertrag_result = ertrag_row.first()
        profit_year_row = session.execute(
            text("SELECT name FROM sqlite_master WHERE type='view' AND name='v_profit_by_year'")
        )
        profit_year_result = profit_year_row.first()
        roe_year_row = session.execute(
            text("SELECT name FROM sqlite_master WHERE type='view' AND name='v_return_on_equity_by_year'")
        )
        roe_year_result = roe_year_row.first()
        unrealized_row = session.execute(
            text("SELECT name FROM sqlite_master WHERE type='view' AND name='v_current_positions_unrealized_profit'")
        )
        unrealized_result = unrealized_row.first()
        monthly_history_row = session.execute(
            text("SELECT name FROM sqlite_master WHERE type='view' AND name='v_portfolio_monthly_history'")
        )
        monthly_history_result = monthly_history_row.first()
    assert sold_result is not None
    assert ertrag_result is not None
    assert profit_year_result is not None
    assert roe_year_result is not None
    assert unrealized_result is not None
    assert monthly_history_result is not None


def test_initialize_database_migrates_old_transaction_type_check(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE products
            (
                id  INTEGER PRIMARY KEY AUTOINCREMENT,
                wkn TEXT NOT NULL UNIQUE
            );

            CREATE TABLE transactions
            (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id       INTEGER NOT NULL,
                type             TEXT    NOT NULL CHECK (type IN ('BUY', 'SELL')),
                transaction_date TEXT    NOT NULL,
                quantity         REAL    NOT NULL CHECK (quantity > 0),
                gross_amount     REAL    NOT NULL CHECK (gross_amount >= 0),
                costs            REAL    NOT NULL DEFAULT 0 CHECK (costs >= 0),
                currency         TEXT    NOT NULL DEFAULT 'EUR',
                source_file      TEXT    NOT NULL,
                source_hash      TEXT    NOT NULL UNIQUE,
                created_at       TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE RESTRICT
            );
            """
        )
        conn.commit()

    initialize_database(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        columns = conn.execute("PRAGMA table_info(transactions)").fetchall()
        names = {item[1] for item in columns}
        assert "bank" in names
        conn.execute("INSERT INTO products (wkn) VALUES (?)", ("A1C1H5",))
        _insert_transaction(
            conn,
            product_id=1,
            tx_type="ERTRAGSABRECHNUNG",
            transaction_date="2025-02-19",
            quantity=0,
            gross_amount=-5.78,
            costs=0,
            currency="EUR",
            source_file="x.pdf",
            source_hash="hash-x",
        )
        _insert_transaction(
            conn,
            product_id=1,
            tx_type="SPLIT",
            transaction_date="2025-02-20",
            quantity=-1.5,
            gross_amount=0,
            costs=0,
            currency="EUR",
            source_file="split.pdf",
            source_hash="hash-split",
        )
        split_row = conn.execute(
            """
            SELECT t.type, t.quantity
            FROM transactions t
                     JOIN source_documents sd ON sd.id = t.source_document_id
            WHERE sd.file_hash = ?
            """,
            ("hash-split",),
        ).fetchone()
        bank = conn.execute(
            """
            SELECT t.bank
            FROM transactions t
                     JOIN source_documents sd ON sd.id = t.source_document_id
            WHERE sd.file_hash = ?
            """,
            ("hash-x",),
        ).fetchone()
        conn.commit()
    assert bank is not None
    assert bank[0] == "UNKNOWN"
    assert split_row == ("SPLIT", -1.5)


def test_initialize_database_adds_and_backfills_transactions_bank_column(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy_bank.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE products
            (
                id  INTEGER PRIMARY KEY AUTOINCREMENT,
                wkn TEXT NOT NULL UNIQUE
            );

            CREATE TABLE transactions
            (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id       INTEGER NOT NULL,
                type             TEXT    NOT NULL CHECK (type IN ('BUY', 'SELL', 'ERTRAGSABRECHNUNG')),
                transaction_date TEXT    NOT NULL,
                quantity         REAL    NOT NULL DEFAULT 0 CHECK (quantity >= 0),
                gross_amount     REAL    NOT NULL,
                costs            REAL    NOT NULL DEFAULT 0 CHECK (costs >= 0),
                currency         TEXT    NOT NULL DEFAULT 'EUR',
                source_file      TEXT    NOT NULL,
                source_hash      TEXT    NOT NULL UNIQUE,
                created_at       TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE RESTRICT
            );

            INSERT INTO products (id, wkn)
            VALUES (1, 'A2PKXG');
            INSERT INTO products (id, wkn)
            VALUES (2, 'A1J4U4');

            INSERT INTO transactions (product_id, type, transaction_date, quantity, gross_amount, costs, currency,
                                      source_file, source_hash)
            VALUES (1, 'BUY', '2025-01-01', 1.0, 100.0, 0.0, 'EUR', 'incoming_pdfs/cortal_consors/KAUF_foo.pdf', 'h1'),
                   (2, 'BUY', '2025-01-01', 1.0, 100.0, 0.0, 'EUR', 'incoming_pdfs/Direkt_Depot_Abrechnung_Kauf.pdf',
                    'h2'),
                   (2, 'BUY', '2025-01-01', 1.0, 100.0, 0.0, 'EUR', 'incoming_pdfs/trade_republic/Wertpapiere.pdf',
                    'h3');
            """
        )
        conn.commit()

    initialize_database(db_path)

    with sqlite3.connect(db_path) as conn:
        cols = conn.execute("PRAGMA table_info(transactions)").fetchall()
        names = {item[1] for item in cols}
        assert "bank" in names
        rows = conn.execute(
            """
            SELECT sd.file_hash, t.bank
            FROM transactions t
                     JOIN source_documents sd ON sd.id = t.source_document_id
            ORDER BY sd.file_hash
            """
        ).fetchall()
        idx = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_transactions_bank'"
        ).fetchone()

    assert rows == [("h1", "CONSORS"), ("h2", "ING"), ("h3", "TRADE_REPUBLIC")]
    assert idx is not None


def test_initialize_database_adds_snapshot_price_column_to_legacy_holding_snapshots(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy_holding.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE products
            (
                id  INTEGER PRIMARY KEY AUTOINCREMENT,
                wkn TEXT NOT NULL UNIQUE
            );

            CREATE TABLE holding_snapshots
            (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id    INTEGER NOT NULL,
                snapshot_date TEXT    NOT NULL,
                quantity      REAL    NOT NULL,
                source_file   TEXT    NOT NULL,
                source_hash   TEXT    NOT NULL
            );
            """
        )
        conn.commit()

    initialize_database(db_path)

    with sqlite3.connect(db_path) as conn:
        columns = conn.execute("PRAGMA table_info(holding_snapshots)").fetchall()
    names = {item[1] for item in columns}
    assert "snapshot_price" in names


def test_profit_view_uses_latest_eur_asset_value_by_id(tmp_path: Path) -> None:
    db_path = tmp_path / "eur_latest.sqlite"
    initialize_database(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            "INSERT INTO products (id, wkn, isin, name, ticker) VALUES (?, ?, ?, ?, ?)",
            (1, "A2PKXG", "IE00BK5BQT80", "FTSE All-World", "VWRA.L"),
        )
        _insert_transaction(
            conn,
            product_id=1,
            tx_type="BUY",
            transaction_date="2025-01-01",
            quantity=10.0,
            gross_amount=1000.0,
            costs=0.0,
            currency="EUR",
            source_file="buy.pdf",
            source_hash="hash-buy-1",
        )
        # Simulate same-second inserts where a non-EUR value exists; view must select latest EUR row by id.
        conn.execute(
            "INSERT INTO asset_values (id, product_id, recorded_at, value, currency, source) VALUES (?, ?, ?, ?, ?, ?)",
            (100, 1, "2026-02-22 20:00:00", 200.0, "USD", "old-usd"),
        )
        conn.execute(
            "INSERT INTO asset_values (id, product_id, recorded_at, value, currency, source) VALUES (?, ?, ?, ?, ?, ?)",
            (101, 1, "2026-02-22 20:00:00", 150.0, "EUR", "new-eur"),
        )
        conn.commit()

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT quantity_open, current_value FROM v_product_profit WHERE product_id = 1"
        ).fetchone()

    assert row is not None
    assert row[0] == 10.0
    assert row[1] == 1500.0


def test_sold_transactions_view_calculates_buy_sell_profit(tmp_path: Path) -> None:
    db_path = tmp_path / "sold_view.sqlite"
    initialize_database(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            "INSERT INTO products (id, wkn, isin, name, ticker) VALUES (?, ?, ?, ?, ?)",
            (1, "A2PKXG", "IE00BK5BQT80", "FTSE All-World", "VWRA.L"),
        )
        _insert_transaction(
            conn,
            product_id=1,
            tx_type="BUY",
            transaction_date="2024-01-10",
            quantity=10.0,
            gross_amount=1000.0,
            costs=10.0,
            currency="EUR",
            source_file="buy1.pdf",
            source_hash="hash-buy-1",
        )
        _insert_transaction(
            conn,
            product_id=1,
            tx_type="BUY",
            transaction_date="2024-02-10",
            quantity=10.0,
            gross_amount=1200.0,
            costs=0.0,
            currency="EUR",
            source_file="buy2.pdf",
            source_hash="hash-buy-2",
        )
        _insert_transaction(
            conn,
            product_id=1,
            tx_type="SELL",
            transaction_date="2024-03-10",
            quantity=5.0,
            gross_amount=700.0,
            costs=10.0,
            currency="EUR",
            source_file="sell1.pdf",
            source_hash="hash-sell-1",
        )
        conn.commit()

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT quantity_sold,
                   avg_buy_price_eur,
                   avg_sell_price_eur,
                   buy_total_eur,
                   sell_total_eur,
                   profit_eur
            FROM v_sold_transactions
            WHERE wkn = 'A2PKXG'
            """
        ).fetchone()

    assert row is not None
    assert row[0] == 5.0
    assert row[1] == 110.5
    assert row[2] == 138.0
    assert row[3] == 552.5
    assert row[4] == 690.0
    assert row[5] == 137.5


def test_sold_transactions_view_uses_split_quantity_and_basis(tmp_path: Path) -> None:
    db_path = tmp_path / "sold_view_split.sqlite"
    initialize_database(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            "INSERT INTO products (id, wkn, isin, name, ticker) VALUES (?, ?, ?, ?, ?)",
            (1, "918422", "US67066G1040", "NVIDIA", "NVDA"),
        )
        rows = [
            (1, "BUY", "2020-01-10", 10.0, 1000.0, 0.0, "EUR", "buy.pdf", "h-buy"),
            (1, "SPLIT", "2021-07-20", 30.0, 0.0, 0.0, "EUR", "split.pdf", "h-split"),
            (1, "SELL", "2022-01-10", 10.0, 600.0, 0.0, "EUR", "sell.pdf", "h-sell"),
        ]
        for item in rows:
            _insert_transaction(
                conn,
                product_id=item[0],
                tx_type=item[1],
                transaction_date=item[2],
                quantity=item[3],
                gross_amount=item[4],
                costs=item[5],
                currency=item[6],
                source_file=item[7],
                source_hash=item[8],
            )
        conn.commit()

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT quantity_sold, avg_buy_price_eur, buy_total_eur, sell_total_eur, profit_eur
            FROM v_sold_transactions
            WHERE wkn = '918422'
            """
        ).fetchone()

    assert row is not None
    assert row[0] == 10.0
    assert row[1] == 25.0
    assert row[2] == 250.0
    assert row[3] == 600.0
    assert row[4] == 350.0


def test_ertragsabrechnungen_by_year_groups_per_product_and_year(tmp_path: Path) -> None:
    db_path = tmp_path / "ertrag_view.sqlite"
    initialize_database(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            "INSERT INTO products (id, wkn, isin, name, ticker) VALUES (?, ?, ?, ?, ?)",
            (1, "A2PKXG", "IE00BK5BQT80", "FTSE All-World", "VWRA.L"),
        )
        conn.execute(
            "INSERT INTO products (id, wkn, isin, name, ticker) VALUES (?, ?, ?, ?, ?)",
            (2, "A1JX52", "IE00B3RBWM25", "FTSE All-World Dist", "VWRD.L"),
        )
        rows = [
            (1, "ERTRAGSABRECHNUNG", "2024-01-15", 0.0, 50.0, 2.0, "EUR", "e1.pdf", "hash-e1"),
            (1, "ERTRAGSABRECHNUNG", "2024-06-15", 0.0, 30.0, 1.0, "EUR", "e2.pdf", "hash-e2"),
            (1, "ERTRAGSABRECHNUNG", "2025-02-15", 0.0, 40.0, 0.0, "EUR", "e3.pdf", "hash-e3"),
            (2, "ERTRAGSABRECHNUNG", "2024-03-01", 0.0, 20.0, 0.5, "EUR", "e4.pdf", "hash-e4"),
        ]
        for item in rows:
            _insert_transaction(
                conn,
                product_id=item[0],
                tx_type=item[1],
                transaction_date=item[2],
                quantity=item[3],
                gross_amount=item[4],
                costs=item[5],
                currency=item[6],
                source_file=item[7],
                source_hash=item[8],
            )
        conn.commit()

    with sqlite3.connect(db_path) as conn:
        result = conn.execute(
            """
            SELECT wkn, year, entries_count, gross_amount_sum_eur, costs_sum_eur, net_amount_sum_eur
            FROM v_ertragsabrechnungen_by_year
            ORDER BY wkn, year
            """
        ).fetchall()

    assert result == [
        ("A1JX52", 2024, 1, 20.0, 0.5, 19.5),
        ("A2PKXG", 2024, 2, 80.0, 3.0, 77.0),
        ("A2PKXG", 2025, 1, 40.0, 0.0, 40.0),
    ]


def test_profit_by_year_combines_ertrag_and_sold_profit(tmp_path: Path) -> None:
    db_path = tmp_path / "profit_by_year.sqlite"
    initialize_database(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            "INSERT INTO products (id, wkn, isin, name, ticker) VALUES (?, ?, ?, ?, ?)",
            (1, "A2PKXG", "IE00BK5BQT80", "FTSE All-World", "VWRA.L"),
        )

        transactions = [
            (1, "BUY", "2023-12-20", 10.0, 1000.0, 0.0, "EUR", "buy.pdf", "hash-buy"),
            (1, "SELL", "2024-03-10", 5.0, 600.0, 0.0, "EUR", "sell-2024.pdf", "hash-sell-2024"),
            (1, "SELL", "2025-03-10", 5.0, 450.0, 0.0, "EUR", "sell-2025.pdf", "hash-sell-2025"),
            (1, "ERTRAGSABRECHNUNG", "2024-04-01", 0.0, 30.0, 0.0, "EUR", "ertrag-2024.pdf", "hash-ertrag-2024"),
            (1, "ERTRAGSABRECHNUNG", "2025-04-01", 0.0, 40.0, 0.0, "EUR", "ertrag-2025.pdf", "hash-ertrag-2025"),
        ]
        for item in transactions:
            _insert_transaction(
                conn,
                product_id=item[0],
                tx_type=item[1],
                transaction_date=item[2],
                quantity=item[3],
                gross_amount=item[4],
                costs=item[5],
                currency=item[6],
                source_file=item[7],
                source_hash=item[8],
            )
        conn.commit()

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT year, ertragsabrechnung_profit_eur, sold_profit_eur, total_profit_eur
            FROM v_profit_by_year
            ORDER BY year
            """
        ).fetchall()

    assert rows == [
        (2024, 30.0, 100.0, 130.0),
        (2025, 40.0, -50.0, -10.0),
    ]


def test_return_on_equity_by_year_uses_average_yearly_invested_amount(tmp_path: Path) -> None:
    db_path = tmp_path / "roe_by_year.sqlite"
    initialize_database(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            "INSERT INTO products (id, wkn, isin, name, ticker) VALUES (?, ?, ?, ?, ?)",
            (1, "A2PKXG", "IE00BK5BQT80", "FTSE All-World", "VWRA.L"),
        )

        transactions = [
            (1, "BUY", "2023-12-20", 10.0, 1000.0, 0.0, "EUR", "buy.pdf", "hash-buy"),
            (1, "SELL", "2024-03-10", 5.0, 600.0, 0.0, "EUR", "sell-2024.pdf", "hash-sell-2024"),
            (1, "SELL", "2025-03-10", 5.0, 450.0, 0.0, "EUR", "sell-2025.pdf", "hash-sell-2025"),
            (1, "ERTRAGSABRECHNUNG", "2024-04-01", 0.0, 30.0, 0.0, "EUR", "ertrag-2024.pdf", "hash-ertrag-2024"),
            (1, "ERTRAGSABRECHNUNG", "2025-04-01", 0.0, 40.0, 0.0, "EUR", "ertrag-2025.pdf", "hash-ertrag-2025"),
        ]
        for item in transactions:
            _insert_transaction(
                conn,
                product_id=item[0],
                tx_type=item[1],
                transaction_date=item[2],
                quantity=item[3],
                gross_amount=item[4],
                costs=item[5],
                currency=item[6],
                source_file=item[7],
                source_hash=item[8],
            )
        history_rows = [
            ("2024-01-01", "2024-01-31", 1000.0, 1100.0, 100.0, "computed"),
            ("2024-02-01", "2024-02-29", 1000.0, 1080.0, 80.0, "computed"),
            ("2025-01-01", "2025-01-31", 500.0, 550.0, 50.0, "computed"),
            ("2025-02-01", "2025-02-28", 500.0, 540.0, 40.0, "computed"),
        ]
        conn.executemany(
            """
            INSERT INTO portfolio_monthly_history (month_date, month_end_date, invested_amount_eur, portfolio_value_eur,
                                                   portfolio_profit_eur, source)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            history_rows,
        )
        conn.commit()

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT year, total_profit_eur, avg_invested_amount_eur, return_on_equity_percent
            FROM v_return_on_equity_by_year
            ORDER BY year
            """
        ).fetchall()

    assert rows == [
        (2024, 130.0, 1000.0, 13.0),
        (2025, -10.0, 500.0, -2.0),
    ]


def test_current_positions_unrealized_profit_uses_open_position_only(tmp_path: Path) -> None:
    db_path = tmp_path / "unrealized.sqlite"
    initialize_database(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            "INSERT INTO products (id, wkn, isin, name, ticker) VALUES (?, ?, ?, ?, ?)",
            (1, "A2PKXG", "IE00BK5BQT80", "FTSE All-World", "VWRA.L"),
        )
        conn.execute(
            "INSERT INTO products (id, wkn, isin, name, ticker) VALUES (?, ?, ?, ?, ?)",
            (2, "A1JX52", "IE00B3RBWM25", "FTSE Dist", "VWRD.L"),
        )

        tx_rows = [
            (1, "BUY", "2024-01-01", 10.0, 1000.0, 0.0, "EUR", "b1.pdf", "h-b1"),
            (1, "SELL", "2024-06-01", 4.0, 600.0, 0.0, "EUR", "s1.pdf", "h-s1"),
            (1, "ERTRAGSABRECHNUNG", "2024-07-01", 0.0, 50.0, 0.0, "EUR", "e1.pdf", "h-e1"),
            (2, "BUY", "2024-01-01", 5.0, 500.0, 0.0, "EUR", "b2.pdf", "h-b2"),
            (2, "SELL", "2024-02-01", 5.0, 550.0, 0.0, "EUR", "s2.pdf", "h-s2"),
        ]
        for item in tx_rows:
            _insert_transaction(
                conn,
                product_id=item[0],
                tx_type=item[1],
                transaction_date=item[2],
                quantity=item[3],
                gross_amount=item[4],
                costs=item[5],
                currency=item[6],
                source_file=item[7],
                source_hash=item[8],
            )
        conn.execute(
            "INSERT INTO asset_values (id, product_id, recorded_at, value, currency, source) VALUES (?, ?, ?, ?, ?, ?)",
            (1, 1, "2026-01-01 10:00:00", 120.0, "EUR", "quote-eur"),
        )
        conn.execute(
            "INSERT INTO asset_values (id, product_id, recorded_at, value, currency, source) VALUES (?, ?, ?, ?, ?, ?)",
            (2, 2, "2026-01-01 10:00:00", 999.0, "EUR", "closed-position"),
        )
        conn.commit()

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT wkn,
                   quantity_open,
                   avg_buy_price_eur,
                   invested_open_eur,
                   current_price_eur,
                   current_value_eur,
                   unrealized_profit_eur
            FROM v_current_positions_unrealized_profit
            ORDER BY wkn
            """
        ).fetchall()

    assert rows == [
        ("A2PKXG", 6.0, 100.0, 600.0, 120.0, 720.0, 120.0),
    ]
