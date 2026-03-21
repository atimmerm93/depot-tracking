from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

DEFAULT_SQL_DIR = Path(__file__).resolve().parent.parent.parent / "sql"




def initialize_database(db_path: str | Path, sql_dir: str | Path = DEFAULT_SQL_DIR) -> None:
    sql_path = Path(sql_dir)
    scripts = sorted(sql_path.glob("*.sql"))
    if not scripts:
        raise FileNotFoundError(f"No SQL files found in {sql_path}")

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        # Apply migrations first so legacy schemas can be brought to the current shape
        # before running idempotent DDL scripts.
        _migrate_transactions_type_constraint_if_needed(conn)
        _migrate_holding_snapshots_add_snapshot_price_if_needed(conn)
        _migrate_source_documents_normalization_if_needed(conn)
        _migrate_transactions_add_bank_column_if_needed(conn)
        for script in scripts:
            conn.executescript(script.read_text(encoding="utf-8"))
        conn.commit()


def sha256_file(file_path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(file_path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(item[1]).lower() for item in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _drop_profit_views(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP VIEW IF EXISTS v_open_positions;
        DROP VIEW IF EXISTS v_product_profit;
        DROP VIEW IF EXISTS v_current_profit;
        DROP VIEW IF EXISTS v_sold_transactions;
        DROP VIEW IF EXISTS v_ertragsabrechnungen_by_year;
        DROP VIEW IF EXISTS v_profit_by_year;
        DROP VIEW IF EXISTS v_return_on_equity_by_year;
        DROP VIEW IF EXISTS v_current_positions_unrealized_profit;
        DROP VIEW IF EXISTS v_portfolio_monthly_history;
        """
    )


def _migrate_transactions_type_constraint_if_needed(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'transactions'"
    ).fetchone()
    if row is None:
        return

    create_sql = (row[0] or "").upper()
    has_split_type = "SPLIT" in create_sql
    has_signed_quantity = "QUANTITY >= 0" not in create_sql and "QUANTITY > 0" not in create_sql
    if has_split_type and has_signed_quantity:
        return

    columns = _column_names(conn, "transactions")
    # This legacy migration only applies to pre-normalization layouts.
    if "source_file" not in columns or "source_hash" not in columns:
        return

    _drop_profit_views(conn)
    bank_case_expr = _bank_case_sql("source_file")
    conn.executescript(
        f"""
        CREATE TABLE transactions__new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            type TEXT NOT NULL CHECK (type IN ('BUY', 'SELL', 'ERTRAGSABRECHNUNG', 'SPLIT')),
            transaction_date TEXT NOT NULL,
            quantity REAL NOT NULL DEFAULT 0,
            gross_amount REAL NOT NULL,
            costs REAL NOT NULL DEFAULT 0 CHECK (costs >= 0),
            currency TEXT NOT NULL DEFAULT 'EUR',
            bank TEXT NOT NULL DEFAULT 'UNKNOWN',
            source_file TEXT NOT NULL,
            source_hash TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE RESTRICT
        );

        INSERT INTO transactions__new (
            id, product_id, type, transaction_date, quantity,
            gross_amount, costs, currency, bank, source_file, source_hash, created_at
        )
        SELECT
            id, product_id, type, transaction_date, quantity,
            gross_amount, costs, currency, {bank_case_expr}, source_file, source_hash, created_at
        FROM transactions;

        DROP TABLE transactions;
        ALTER TABLE transactions__new RENAME TO transactions;
        """
    )


def _migrate_transactions_add_bank_column_if_needed(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "transactions"):
        return

    columns = _column_names(conn, "transactions")
    if "bank" not in columns:
        conn.execute("ALTER TABLE transactions ADD COLUMN bank TEXT NOT NULL DEFAULT 'UNKNOWN'")
        columns.add("bank")

    bank_expression: str | None = None
    if "source_file" in columns:
        bank_expression = _bank_case_sql("source_file")
    elif "source_document_id" in columns and _table_exists(conn, "source_documents"):
        bank_expression = f"(SELECT {_bank_case_sql('sd.file_path')} FROM source_documents sd WHERE sd.id = transactions.source_document_id)"

    if bank_expression is None:
        return

    conn.execute(
        f"""
        UPDATE transactions
        SET bank = COALESCE({bank_expression}, 'UNKNOWN')
        WHERE bank IS NULL OR TRIM(bank) = '' OR UPPER(bank) = 'UNKNOWN'
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_bank ON transactions(bank)")


def _migrate_holding_snapshots_add_snapshot_price_if_needed(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "holding_snapshots"):
        return

    column_names = _column_names(conn, "holding_snapshots")
    if "snapshot_price" in column_names:
        return
    conn.execute("ALTER TABLE holding_snapshots ADD COLUMN snapshot_price REAL")


def _migrate_source_documents_normalization_if_needed(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS source_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL,
            file_hash TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_source_documents_hash ON source_documents(file_hash);
        """
    )

    if _table_exists(conn, "transactions"):
        tx_cols = _column_names(conn, "transactions")
    else:
        tx_cols = set()
    if _table_exists(conn, "holding_snapshots"):
        snapshot_cols = _column_names(conn, "holding_snapshots")
    else:
        snapshot_cols = set()
    if _table_exists(conn, "processed_files"):
        processed_cols = _column_names(conn, "processed_files")
    else:
        processed_cols = set()

    has_legacy_tx = {"source_file", "source_hash"}.issubset(tx_cols) and "source_document_id" not in tx_cols
    has_legacy_snapshot = {"source_file", "source_hash"}.issubset(snapshot_cols) and "source_document_id" not in snapshot_cols
    has_legacy_processed = {"file_path", "file_hash"}.issubset(processed_cols) and "source_document_id" not in processed_cols

    if not (has_legacy_tx or has_legacy_snapshot or has_legacy_processed):
        return

    if has_legacy_tx:
        conn.execute(
            """
            INSERT OR IGNORE INTO source_documents (file_path, file_hash)
            SELECT source_file, source_hash
            FROM transactions
            WHERE source_hash IS NOT NULL AND TRIM(source_hash) <> ''
            """
        )

    if has_legacy_snapshot:
        conn.execute(
            """
            INSERT OR IGNORE INTO source_documents (file_path, file_hash)
            SELECT source_file, source_hash
            FROM holding_snapshots
            WHERE source_hash IS NOT NULL AND TRIM(source_hash) <> ''
            """
        )

    if has_legacy_processed:
        conn.execute(
            """
            INSERT OR IGNORE INTO source_documents (file_path, file_hash)
            SELECT file_path, file_hash
            FROM processed_files
            WHERE file_hash IS NOT NULL AND TRIM(file_hash) <> ''
            """
        )

    _drop_profit_views(conn)

    if has_legacy_tx:
        conn.executescript(
            """
            CREATE TABLE transactions__new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                source_document_id INTEGER NOT NULL UNIQUE,
                type TEXT NOT NULL CHECK (type IN ('BUY', 'SELL', 'ERTRAGSABRECHNUNG', 'SPLIT')),
                transaction_date TEXT NOT NULL,
                quantity REAL NOT NULL DEFAULT 0,
                gross_amount REAL NOT NULL,
                costs REAL NOT NULL DEFAULT 0 CHECK (costs >= 0),
                currency TEXT NOT NULL DEFAULT 'EUR',
                bank TEXT NOT NULL DEFAULT 'UNKNOWN',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE RESTRICT,
                FOREIGN KEY (source_document_id) REFERENCES source_documents(id) ON DELETE RESTRICT
            );

            INSERT INTO transactions__new (
                id, product_id, source_document_id, type, transaction_date,
                quantity, gross_amount, costs, currency, bank, created_at
            )
            SELECT
                t.id,
                t.product_id,
                sd.id,
                t.type,
                t.transaction_date,
                t.quantity,
                t.gross_amount,
                t.costs,
                t.currency,
                t.bank,
                t.created_at
            FROM transactions t
            JOIN source_documents sd ON sd.file_hash = t.source_hash;

            DROP TABLE transactions;
            ALTER TABLE transactions__new RENAME TO transactions;
            CREATE INDEX IF NOT EXISTS idx_transactions_product_id ON transactions(product_id);
            CREATE INDEX IF NOT EXISTS idx_transactions_source_document_id ON transactions(source_document_id);
            CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);
            CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type);
            """
        )

    if has_legacy_snapshot:
        snapshot_price_select = "s.snapshot_price" if "snapshot_price" in snapshot_cols else "NULL"
        snapshot_created_at_select = "s.created_at" if "created_at" in snapshot_cols else "CURRENT_TIMESTAMP"
        conn.executescript(
            f"""
            CREATE TABLE holding_snapshots__new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                source_document_id INTEGER NOT NULL,
                snapshot_date TEXT NOT NULL,
                quantity REAL NOT NULL CHECK (quantity >= 0),
                snapshot_price REAL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
                FOREIGN KEY (source_document_id) REFERENCES source_documents(id) ON DELETE CASCADE,
                UNIQUE (product_id, source_document_id)
            );

            INSERT INTO holding_snapshots__new (
                id,
                product_id,
                source_document_id,
                snapshot_date,
                quantity,
                snapshot_price,
                created_at
            )
            SELECT
                s.id,
                s.product_id,
                sd.id,
                s.snapshot_date,
                s.quantity,
                {snapshot_price_select},
                {snapshot_created_at_select}
            FROM holding_snapshots s
            JOIN source_documents sd ON sd.file_hash = s.source_hash;

            DROP TABLE holding_snapshots;
            ALTER TABLE holding_snapshots__new RENAME TO holding_snapshots;
            CREATE INDEX IF NOT EXISTS idx_holding_snapshots_product_date
                ON holding_snapshots(product_id, snapshot_date);
            CREATE INDEX IF NOT EXISTS idx_holding_snapshots_source_document_id
                ON holding_snapshots(source_document_id);
            """
        )

    if has_legacy_processed:
        conn.executescript(
            """
            CREATE TABLE processed_files__new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_document_id INTEGER NOT NULL UNIQUE,
                processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                parser_version TEXT NOT NULL DEFAULT 'v1',
                FOREIGN KEY (source_document_id) REFERENCES source_documents(id) ON DELETE CASCADE
            );

            INSERT INTO processed_files__new (
                id,
                source_document_id,
                processed_at,
                parser_version
            )
            SELECT
                p.id,
                sd.id,
                p.processed_at,
                p.parser_version
            FROM processed_files p
            JOIN source_documents sd ON sd.file_hash = p.file_hash;

            DROP TABLE processed_files;
            ALTER TABLE processed_files__new RENAME TO processed_files;
            CREATE INDEX IF NOT EXISTS idx_processed_files_source_document_id
                ON processed_files(source_document_id);
            """
        )


def _bank_case_sql(source_column: str) -> str:
    lowered = f"LOWER(COALESCE({source_column}, ''))"
    return (
        "CASE "
        f"WHEN {lowered} LIKE '%direkt_depot%' THEN 'ING' "
        f"WHEN {lowered} LIKE '%abrechnung_kauf%' THEN 'ING' "
        f"WHEN {lowered} LIKE '%abrechnung_verkauf%' THEN 'ING' "
        f"WHEN {lowered} LIKE '%ertragsabrechnung%' THEN 'ING' "
        f"WHEN {lowered} LIKE '%consors%' THEN 'CONSORS' "
        f"WHEN {lowered} LIKE '%cortal_consors%' THEN 'CONSORS' "
        f"WHEN {lowered} LIKE '%dividendengutschrift%' THEN 'CONSORS' "
        f"WHEN {lowered} LIKE '%wertpapier-jahresdepotauszug%' THEN 'CONSORS' "
        f"WHEN {lowered} LIKE '%quartalsdepotauszug%' THEN 'CONSORS' "
        f"WHEN {lowered} LIKE '%jahresdepotauszug%' THEN 'CONSORS' "
        f"WHEN {lowered} LIKE '%/kauf_%' THEN 'CONSORS' "
        f"WHEN {lowered} LIKE '%/verkauf_%' THEN 'CONSORS' "
        f"WHEN {lowered} LIKE '%trade_republic%' THEN 'TRADE_REPUBLIC' "
        f"WHEN {lowered} LIKE '%traderepublic%' THEN 'TRADE_REPUBLIC' "
        "ELSE 'UNKNOWN' END"
    )
