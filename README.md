# AnalyzeBankingDocuments

Automatically ingest transaction PDFs, persist normalized data in SQLite, and compute current portfolio profit using
live internet prices.

## What it creates

- `products`: financial products (includes `WKN`, optional `ISIN`, optional ticker)
- `transactions`: `BUY` / `SELL` / `ERTRAGSABRECHNUNG` with cost column and FK to products
- `holding_snapshots`: Depotauszug position snapshots per product/date
- `asset_values`: current market value snapshots per product (timestamp + value)
- `portfolio_monthly_history`: stored monthly portfolio snapshots (`invested_amount_eur`, `portfolio_value_eur`,
  `portfolio_profit_eur`)
- `v_current_profit`: overall current profit from all transactions
- `v_product_profit`: per-product profit breakdown
- `v_sold_transactions`: realized sale report with buy price, sell price, and profit per SELL transaction
- `v_ertragsabrechnungen_by_year`: Ertragsabrechnungen grouped by product and year
- `v_profit_by_year`: yearly profit summary (`ERTRAGSABRECHNUNG` net + realized sold profit)
- `v_return_on_equity_by_year`: yearly return on equity in percent (`total_profit_eur / avg invested_amount_eur * 100`)
- `v_current_positions_unrealized_profit`: debug view for open positions only (`invested_open_eur` vs
  `current_value_eur`)
- `v_portfolio_monthly_history`: ordered read view over stored monthly snapshots

SQL files:

- `sql/001_schema.sql`
- `sql/002_profit_views.sql`

## Usage

Install dependencies:

```bash
uv sync
```

Initialize database:

```bash
uv run python main.py --db-path banking.sqlite init-db
```

Put PDFs into `incoming_pdfs/` and ingest them:

```bash
uv run python main.py --db-path banking.sqlite ingest --pdf-dir incoming_pdfs
```

Choose parser bank explicitly when needed:

```bash
uv run python main.py --db-path banking.sqlite --parser-bank consors ingest --pdf-dir incoming_pdfs
```

Infer missing BUY transactions from Depotauszug snapshots (uses historical prices around snapshot date):

```bash
uv run python main.py --db-path banking.sqlite infer-buys
```

Fetch current values from the internet (Yahoo Finance):

```bash
uv run python main.py --db-path banking.sqlite update-values
```

Build/store monthly portfolio history snapshots:

```bash
uv run python main.py --db-path banking.sqlite build-monthly-history
```

Optional range:

```bash
uv run python main.py --db-path banking.sqlite build-monthly-history --start-month 2020-01 --end-month 2026-02
```

Draw a graph from `v_portfolio_monthly_history` (x-axis time, y-axis EUR, lines for `portfolio_value_eur`,
`portfolio_profit_eur`, and realized earnings):

```bash
uv run python main.py --db-path banking.sqlite plot-history --output-file portfolio_monthly_history.png
```

Open the same graph as an interactive plot (zoom/pan in the matplotlib window):

```bash
uv run python main.py --db-path banking.sqlite plot-history --interactive
```

Backfill month-end Yahoo values and then rebuild monthly history (script):

```bash
uv run python scripts/backfill_monthly_yahoo_history.py --db-path banking.sqlite --start-month 2021-01 --end-month 2026-02
```

Run everything once:

```bash
uv run python main.py --db-path banking.sqlite run-once --pdf-dir incoming_pdfs
```

`run-once` now performs: ingest -> infer missing buys from Depotauszug -> update current values -> report.
It also refreshes monthly history snapshots.

Run continuously (watches folder by polling):

```bash
uv run python main.py --db-path banking.sqlite monitor --pdf-dir incoming_pdfs --interval-seconds 60
```

Apply known portfolio data repairs (currently includes Atlassian alias duplicate neutralization and Amazon 1:20 split
adjustment):

```bash
uv run python main.py --db-path banking.sqlite repair-db
```

Print report:

```bash
uv run python main.py --db-path banking.sqlite report
```

Download documents from your current browser tab (Selenium attach mode):

1. Start Chrome with remote debugging enabled:

```bash
open -na "Google Chrome" --args --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-debug
```

2. In that Chrome window, log in to your bank and open the inbox page with the message list.
3. Run the downloader for your bank (`ing` or `consors`):

```bash
uv run python main.py download-docs --bank ing --debugger-address 127.0.0.1:9222 --download-dir incoming_pdfs
```

Consors example:

```bash
uv run python main.py download-docs --bank consors --debugger-address 127.0.0.1:9222 --download-dir incoming_pdfs
```

If you want to force a fresh Selenium pass (ignore previously downloaded row signatures):

```bash
uv run python main.py download-docs --debugger-address 127.0.0.1:9222 --download-dir incoming_pdfs --reset-state
```

## Notes

- Database writes are implemented with SQLAlchemy ORM mappings.
- Duplicate ingestion is prevented by hashing each PDF.
- Parser supports both ING and Consors document families (`Kauf`, `Verkauf`, `Ertragsabrechnung` /
  `Dividendengutschrift`, `Jahresdepotauszug`, `Quartalsdepotauszug`).
- Alias normalization maps known security ID changes to a canonical product (`A3DUN5` -> `A2ABYA`, `US0494681010` ->
  `GB00BZ09BD16`) to avoid duplicate holdings from inferred Depotauszug buys.
- Monthly history snapshots are computed as-of each month end: open-position invested amount (cost basis), portfolio
  value (latest EUR valuation up to month end, fallback to buy-cost if missing), and portfolio profit (
  `net_cashflow + portfolio_value`).
- Depotauszug documents are parsed into `holding_snapshots`; inferred historical BUY entries can be created from the
  earliest snapshot per product with `infer-buys`.
- Price updates are attempted for open positions (`buy_qty - sell_qty > 0`).
- All stored valuation prices are normalized to `EUR`; if Yahoo returns another currency (e.g. `USD`), FX conversion to
  EUR is applied before writing `asset_values` or inferred BUY prices.
- If Yahoo data is unavailable for an open position, `update-values` falls back to average EUR buy cost per unit for
  that product.
- Selenium download mode keeps control in your manual browser session and stores downloaded row signatures in
  `incoming_pdfs/.selenium_downloaded_rows.json`.
- Consors Selenium download mode stores signatures in `incoming_pdfs/.selenium_downloaded_rows_consors.json`.
- Selenium download mode watches both `incoming_pdfs/` and `~/Downloads/` for newly created PDFs and can fallback to
  cookie-based URL download if the UI opens a PDF URL instead of writing a file directly.
