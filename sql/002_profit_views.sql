DROP VIEW IF EXISTS v_open_positions;
DROP VIEW IF EXISTS v_product_profit;
DROP VIEW IF EXISTS v_current_profit;
DROP VIEW IF EXISTS v_sold_transactions;
DROP VIEW IF EXISTS v_ertragsabrechnungen_by_year;
DROP VIEW IF EXISTS v_profit_by_year;
DROP VIEW IF EXISTS v_return_on_equity_by_year;
DROP VIEW IF EXISTS v_current_positions_unrealized_profit;
DROP VIEW IF EXISTS v_portfolio_monthly_history;

CREATE VIEW v_open_positions AS
WITH tx AS (
    SELECT
        product_id,
        SUM(
            CASE
                WHEN type = 'BUY' THEN quantity
                WHEN type = 'SELL' THEN -quantity
                WHEN type = 'SPLIT' THEN quantity
                ELSE 0
            END
        ) AS quantity_open
    FROM transactions
    GROUP BY product_id
)
SELECT
    p.id AS product_id,
    p.wkn,
    p.isin,
    p.name,
    p.ticker,
    ROUND(tx.quantity_open, 8) AS quantity_open
FROM tx
JOIN products p ON p.id = tx.product_id
WHERE tx.quantity_open > 0;

CREATE VIEW v_product_profit AS
WITH tx AS (
    SELECT
        product_id,
        SUM(
            CASE
                WHEN type = 'BUY' THEN quantity
                WHEN type = 'SELL' THEN -quantity
                WHEN type = 'SPLIT' THEN quantity
                ELSE 0
            END
        ) AS quantity_open,
        SUM(
            CASE
                WHEN type = 'BUY' THEN gross_amount + costs
                WHEN type = 'SPLIT' THEN gross_amount
                ELSE 0
            END
        ) AS invested_eur,
        SUM(CASE WHEN type = 'SELL' THEN gross_amount - costs ELSE 0 END) AS returned_eur,
        SUM(
            CASE
                WHEN type = 'BUY' THEN -(gross_amount + costs)
                WHEN type = 'SELL' THEN (gross_amount - costs)
                WHEN type = 'ERTRAGSABRECHNUNG' THEN (gross_amount - costs)
                WHEN type = 'SPLIT' THEN -gross_amount
                ELSE 0
            END
        ) AS net_cashflow
    FROM transactions
    GROUP BY product_id
), latest_values AS (
    SELECT av.product_id, av.value, av.recorded_at
    FROM asset_values av
    JOIN (
        SELECT product_id, MAX(id) AS max_id
        FROM asset_values
        WHERE UPPER(currency) = 'EUR'
        GROUP BY product_id
    ) latest
    ON latest.max_id = av.id
)
SELECT
    p.id AS product_id,
    p.wkn,
    p.isin,
    p.name,
    p.ticker,
    ROUND(tx.quantity_open, 8) AS quantity_open,
    ROUND(tx.invested_eur, 2) AS invested_eur,
    ROUND(tx.returned_eur, 2) AS returned_eur,
    ROUND(tx.net_cashflow, 2) AS net_cashflow,
    ROUND(COALESCE(tx.quantity_open, 0) * COALESCE(lv.value, 0), 2) AS current_value,
    ROUND(COALESCE(tx.net_cashflow, 0) + (COALESCE(tx.quantity_open, 0) * COALESCE(lv.value, 0)), 2) AS profit,
    lv.recorded_at AS latest_value_timestamp
FROM tx
JOIN products p ON p.id = tx.product_id
LEFT JOIN latest_values lv ON lv.product_id = tx.product_id;

CREATE VIEW v_current_profit AS
SELECT
    ROUND(COALESCE(SUM(profit), 0), 2) AS total_profit,
    ROUND(COALESCE(SUM(current_value), 0), 2) AS current_portfolio_value,
    ROUND(COALESCE(SUM(net_cashflow), 0), 2) AS net_cashflow
FROM v_product_profit;

CREATE VIEW v_sold_transactions AS
WITH sell_tx AS (
    SELECT
        t.id,
        t.product_id,
        t.transaction_date,
        t.quantity,
        t.gross_amount,
        t.costs,
        t.currency
    FROM transactions t
    WHERE t.type = 'SELL'
), buy_basis AS (
    SELECT
        s.id AS sell_id,
        COALESCE(
            (
                SELECT SUM(
                    CASE
                        WHEN b.type = 'BUY' THEN b.gross_amount + b.costs
                        WHEN b.type = 'SPLIT' THEN b.gross_amount
                        ELSE 0
                    END
                )
                FROM transactions b
                WHERE b.product_id = s.product_id
                  AND b.type IN ('BUY', 'SPLIT')
                  AND (
                    b.transaction_date < s.transaction_date
                    OR (b.transaction_date = s.transaction_date AND b.id <= s.id)
                  )
            ),
            0
        ) AS buy_amount_eur,
        COALESCE(
            (
                SELECT SUM(
                    CASE
                        WHEN b.type = 'BUY' THEN b.quantity
                        WHEN b.type = 'SPLIT' THEN b.quantity
                        ELSE 0
                    END
                )
                FROM transactions b
                WHERE b.product_id = s.product_id
                  AND b.type IN ('BUY', 'SPLIT')
                  AND (
                    b.transaction_date < s.transaction_date
                    OR (b.transaction_date = s.transaction_date AND b.id <= s.id)
                  )
            ),
            0
        ) AS buy_quantity
    FROM sell_tx s
)
SELECT
    s.id AS sell_transaction_id,
    s.product_id,
    p.wkn,
    p.isin,
    p.name,
    p.ticker,
    s.transaction_date AS sell_date,
    ROUND(s.quantity, 8) AS quantity_sold,
    ROUND(
        CASE
            WHEN bb.buy_quantity > 0 THEN bb.buy_amount_eur / bb.buy_quantity
            ELSE 0
        END,
        8
    ) AS avg_buy_price_eur,
    ROUND(
        CASE
            WHEN s.quantity > 0 THEN (s.gross_amount - s.costs) / s.quantity
            ELSE 0
        END,
        8
    ) AS avg_sell_price_eur,
    ROUND(
        s.quantity
        * (
            CASE
                WHEN bb.buy_quantity > 0 THEN bb.buy_amount_eur / bb.buy_quantity
                ELSE 0
            END
        ),
        2
    ) AS buy_total_eur,
    ROUND(s.gross_amount - s.costs, 2) AS sell_total_eur,
    ROUND(
        (s.gross_amount - s.costs)
        - (
            s.quantity
            * (
                CASE
                    WHEN bb.buy_quantity > 0 THEN bb.buy_amount_eur / bb.buy_quantity
                    ELSE 0
                END
            )
        ),
        2
    ) AS profit_eur,
    ROUND(s.gross_amount, 2) AS sell_gross_amount_eur,
    ROUND(s.costs, 2) AS sell_costs_eur,
    s.currency AS sell_currency
FROM sell_tx s
JOIN buy_basis bb ON bb.sell_id = s.id
JOIN products p ON p.id = s.product_id
ORDER BY s.transaction_date, s.id;

CREATE VIEW v_ertragsabrechnungen_by_year AS
SELECT
    p.id AS product_id,
    p.wkn,
    p.isin,
    p.name,
    p.ticker,
    CAST(strftime('%Y', t.transaction_date) AS INTEGER) AS year,
    COUNT(*) AS entries_count,
    ROUND(SUM(t.gross_amount), 2) AS gross_amount_sum_eur,
    ROUND(SUM(t.costs), 2) AS costs_sum_eur,
    ROUND(SUM(t.gross_amount - t.costs), 2) AS net_amount_sum_eur
FROM transactions t
JOIN products p ON p.id = t.product_id
WHERE t.type = 'ERTRAGSABRECHNUNG'
GROUP BY
    p.id,
    p.wkn,
    p.isin,
    p.name,
    p.ticker,
    CAST(strftime('%Y', t.transaction_date) AS INTEGER)
ORDER BY
    year,
    p.wkn;

CREATE VIEW v_profit_by_year AS
WITH ertrag AS (
    SELECT
        year,
        ROUND(SUM(net_amount_sum_eur), 2) AS ertragsabrechnung_profit_eur
    FROM v_ertragsabrechnungen_by_year
    GROUP BY year
), sold AS (
    SELECT
        CAST(strftime('%Y', sell_date) AS INTEGER) AS year,
        ROUND(SUM(profit_eur), 2) AS sold_profit_eur
    FROM v_sold_transactions
    GROUP BY CAST(strftime('%Y', sell_date) AS INTEGER)
), years AS (
    SELECT year FROM ertrag
    UNION
    SELECT year FROM sold
)
SELECT
    y.year,
    ROUND(COALESCE(e.ertragsabrechnung_profit_eur, 0), 2) AS ertragsabrechnung_profit_eur,
    ROUND(COALESCE(s.sold_profit_eur, 0), 2) AS sold_profit_eur,
    ROUND(COALESCE(e.ertragsabrechnung_profit_eur, 0) + COALESCE(s.sold_profit_eur, 0), 2) AS total_profit_eur
FROM years y
LEFT JOIN ertrag e ON e.year = y.year
LEFT JOIN sold s ON s.year = y.year
ORDER BY y.year;

CREATE VIEW v_return_on_equity_by_year AS
WITH yearly_equity AS (
    SELECT
        CAST(strftime('%Y', month_date) AS INTEGER) AS year,
        ROUND(AVG(invested_amount_eur), 2) AS avg_invested_amount_eur
    FROM portfolio_monthly_history
    GROUP BY CAST(strftime('%Y', month_date) AS INTEGER)
)
SELECT
    p.year,
    p.ertragsabrechnung_profit_eur,
    p.sold_profit_eur,
    p.total_profit_eur,
    ROUND(COALESCE(y.avg_invested_amount_eur, 0), 2) AS avg_invested_amount_eur,
    CASE
        WHEN COALESCE(y.avg_invested_amount_eur, 0) > 0 THEN ROUND((p.total_profit_eur / y.avg_invested_amount_eur) * 100, 2)
        ELSE NULL
    END AS return_on_equity_percent
FROM v_profit_by_year p
LEFT JOIN yearly_equity y ON y.year = p.year
ORDER BY p.year;

CREATE VIEW v_current_positions_unrealized_profit AS
WITH qty AS (
    SELECT
        t.product_id,
        SUM(
            CASE
                WHEN t.type = 'BUY' THEN t.quantity
                WHEN t.type = 'SELL' THEN -t.quantity
                WHEN t.type = 'SPLIT' THEN t.quantity
                ELSE 0
            END
        ) AS quantity_open
    FROM transactions t
    GROUP BY t.product_id
), buy_basis AS (
    SELECT
        t.product_id,
        SUM(CASE WHEN t.type = 'BUY' THEN t.quantity WHEN t.type = 'SPLIT' THEN t.quantity ELSE 0 END) AS buy_quantity_total,
        SUM(CASE WHEN t.type = 'BUY' THEN t.gross_amount + t.costs WHEN t.type = 'SPLIT' THEN t.gross_amount ELSE 0 END) AS buy_amount_total_eur
    FROM transactions t
    GROUP BY t.product_id
), latest_values AS (
    SELECT av.product_id, av.value, av.recorded_at
    FROM asset_values av
    JOIN (
        SELECT product_id, MAX(id) AS max_id
        FROM asset_values
        WHERE UPPER(currency) = 'EUR'
        GROUP BY product_id
    ) latest ON latest.max_id = av.id
)
SELECT
    p.id AS product_id,
    p.wkn,
    p.isin,
    p.name,
    p.ticker,
    ROUND(q.quantity_open, 8) AS quantity_open,
    ROUND(
        CASE
            WHEN bb.buy_quantity_total > 0 THEN bb.buy_amount_total_eur / bb.buy_quantity_total
            ELSE 0
        END,
        8
    ) AS avg_buy_price_eur,
    ROUND(
        q.quantity_open
        * (
            CASE
                WHEN bb.buy_quantity_total > 0 THEN bb.buy_amount_total_eur / bb.buy_quantity_total
                ELSE 0
            END
        ),
        2
    ) AS invested_open_eur,
    ROUND(COALESCE(lv.value, 0), 8) AS current_price_eur,
    ROUND(q.quantity_open * COALESCE(lv.value, 0), 2) AS current_value_eur,
    ROUND(
        (q.quantity_open * COALESCE(lv.value, 0))
        - (
            q.quantity_open
            * (
                CASE
                    WHEN bb.buy_quantity_total > 0 THEN bb.buy_amount_total_eur / bb.buy_quantity_total
                    ELSE 0
                END
            )
        ),
        2
    ) AS unrealized_profit_eur,
    lv.recorded_at AS latest_value_timestamp
FROM qty q
JOIN products p ON p.id = q.product_id
LEFT JOIN buy_basis bb ON bb.product_id = q.product_id
LEFT JOIN latest_values lv ON lv.product_id = q.product_id
WHERE q.quantity_open > 0
ORDER BY unrealized_profit_eur DESC, p.wkn;

CREATE VIEW v_portfolio_monthly_history AS
SELECT
    month_date,
    month_end_date,
    ROUND(invested_amount_eur, 2) AS invested_amount_eur,
    ROUND(portfolio_value_eur, 2) AS portfolio_value_eur,
    ROUND(portfolio_profit_eur, 2) AS portfolio_profit_eur,
    source,
    recorded_at
FROM portfolio_monthly_history
ORDER BY month_date;
