BEGIN;

-- Monthly business performance.
-- Financial values retain their original precision.
CREATE OR REPLACE VIEW analytics.v_monthly_performance AS
SELECT
    year,
    month,
    year_month,
    transaction_count,
    sale_count,
    cancellation_count,
    return_count,
    review_count,
    identified_customer_count,
    sales_amount,
    cancellation_amount,
    return_amount,
    net_amount,
    LAG(net_amount) OVER (
        ORDER BY year, month
    ) AS previous_month_net_amount,
    net_amount
        - LAG(net_amount) OVER (
            ORDER BY year, month
        ) AS month_over_month_change,
    ROUND(
        (
            net_amount
            - LAG(net_amount) OVER (
                ORDER BY year, month
            )
        )
        / NULLIF(
            ABS(
                LAG(net_amount) OVER (
                    ORDER BY year, month
                )
            ),
            0
        ) * 100,
        2
    ) AS month_over_month_pct
FROM analytics.mart_monthly_sales;

-- Product performance using only recognized transactions.
-- Review rows are excluded from monetary aggregations.
CREATE OR REPLACE VIEW analytics.v_product_performance AS
SELECT
    p.product_key,
    p.stock_code,
    p.description,
    p.description_variants,
    COUNT(*) FILTER (
        WHERE f.transaction_type = 'sale'
    ) AS sale_lines,
    COUNT(*) FILTER (
        WHERE f.transaction_type = 'cancellation'
    ) AS cancellation_lines,
    COUNT(*) FILTER (
        WHERE f.transaction_type = 'return'
    ) AS return_lines,
    COUNT(*) FILTER (
        WHERE f.transaction_type = 'review'
    ) AS review_lines,
    COALESCE(
        SUM(f.net_transaction_amount),
        0::numeric
    ) AS net_amount
FROM analytics.dim_product p
LEFT JOIN analytics.fact_sales f
    ON f.product_key = p.product_key
GROUP BY
    p.product_key,
    p.stock_code,
    p.description,
    p.description_variants;

-- Country-level commercial performance.
CREATE OR REPLACE VIEW analytics.v_country_performance AS
SELECT
    c.country_key,
    c.country,
    COUNT(*) FILTER (
        WHERE f.transaction_type = 'sale'
    ) AS sale_lines,
    COUNT(*) FILTER (
        WHERE f.transaction_type = 'cancellation'
    ) AS cancellation_lines,
    COUNT(*) FILTER (
        WHERE f.transaction_type = 'return'
    ) AS return_lines,
    COUNT(*) FILTER (
        WHERE f.transaction_type = 'review'
    ) AS review_lines,
    COALESCE(
        SUM(f.net_transaction_amount),
        0::numeric
    ) AS net_amount
FROM analytics.dim_country c
LEFT JOIN analytics.fact_sales f
    ON f.country_key = c.country_key
GROUP BY
    c.country_key,
    c.country;

-- Customer-level activity.
-- Unknown customers remain visible rather than being silently discarded.
CREATE OR REPLACE VIEW analytics.v_customer_performance AS
SELECT
    c.customer_key,
    c.customer_id,
    COUNT(*) FILTER (
        WHERE f.transaction_type = 'sale'
    ) AS sale_lines,
    COUNT(*) FILTER (
        WHERE f.transaction_type = 'cancellation'
    ) AS cancellation_lines,
    COUNT(*) FILTER (
        WHERE f.transaction_type = 'return'
    ) AS return_lines,
    COUNT(*) FILTER (
        WHERE f.transaction_type = 'review'
    ) AS review_lines,
    COUNT(DISTINCT f.invoice) FILTER (
        WHERE f.transaction_type = 'sale'
    ) AS distinct_sale_invoices,
    COALESCE(
        SUM(f.net_transaction_amount),
        0::numeric
    ) AS net_amount
FROM analytics.dim_customer c
LEFT JOIN analytics.fact_sales f
    ON f.customer_key = c.customer_key
GROUP BY
    c.customer_key,
    c.customer_id;

-- Operational quality indicators.
CREATE OR REPLACE VIEW analytics.v_data_quality_summary AS
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (
        WHERE is_customer_missing
    ) AS missing_customer_rows,
    COUNT(*) FILTER (
        WHERE is_price_non_positive
    ) AS non_positive_price_rows,
    COUNT(*) FILTER (
        WHERE is_date_missing
    ) AS missing_date_rows,
    COUNT(*) FILTER (
        WHERE transaction_type = 'review'
    ) AS review_rows,
    COUNT(*) FILTER (
        WHERE net_transaction_amount IS NULL
    ) AS null_net_amount_rows,
    COUNT(*) - COUNT(DISTINCT row_fingerprint)
        AS repeated_fingerprint_rows
FROM analytics.fact_sales;

COMMIT;
