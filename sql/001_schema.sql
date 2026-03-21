PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wkn TEXT NOT NULL UNIQUE,
    isin TEXT UNIQUE,
    name TEXT,
    ticker TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER IF NOT EXISTS products_set_updated_at
AFTER UPDATE ON products
BEGIN
    UPDATE products
    SET updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.id;
END;

CREATE TABLE IF NOT EXISTS source_documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path TEXT NOT NULL,
    file_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_source_documents_hash ON source_documents(file_hash);

CREATE TABLE IF NOT EXISTS transactions (
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

CREATE INDEX IF NOT EXISTS idx_transactions_product_id ON transactions(product_id);
CREATE INDEX IF NOT EXISTS idx_transactions_source_document_id ON transactions(source_document_id);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type);

CREATE TABLE IF NOT EXISTS asset_values (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    value REAL NOT NULL CHECK (value >= 0),
    currency TEXT NOT NULL DEFAULT 'EUR',
    source TEXT NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_asset_values_product_time
    ON asset_values(product_id, recorded_at DESC);

CREATE TABLE IF NOT EXISTS holding_snapshots (
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

CREATE INDEX IF NOT EXISTS idx_holding_snapshots_product_date
    ON holding_snapshots(product_id, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_holding_snapshots_source_document_id
    ON holding_snapshots(source_document_id);

CREATE TABLE IF NOT EXISTS processed_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_document_id INTEGER NOT NULL UNIQUE,
    processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    parser_version TEXT NOT NULL DEFAULT 'v1',
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_processed_files_source_document_id
    ON processed_files(source_document_id);

CREATE TABLE IF NOT EXISTS portfolio_monthly_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    month_date TEXT NOT NULL UNIQUE,
    month_end_date TEXT NOT NULL,
    invested_amount_eur REAL NOT NULL DEFAULT 0,
    portfolio_value_eur REAL NOT NULL DEFAULT 0,
    portfolio_profit_eur REAL NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'computed',
    recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_portfolio_monthly_history_month_date
    ON portfolio_monthly_history(month_date);
