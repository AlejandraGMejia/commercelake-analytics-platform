-- CommerceLake business analysis.
-- All queries are read-only.

-- 1. Monthly performance and growth.
SELECT
    year_month,
    sale_count,
    cancellation_count,
    return_count,
    review_count,
    net_amount,
    month_over_month_change,
    month_over_month_pct
FROM analytics.v_monthly_performance
ORDER BY year, month;

-- 2. Top 15 products by net amount.
SELECT
    stock_code,
    description,
    sale_lines,
    cancellation_lines,
    return_lines,
    net_amount
FROM analytics.v_product_performance
ORDER BY net_amount DESC, product_key
LIMIT 15;

-- 3. Top 15 countries by net amount.
SELECT
    country,
    sale_lines,
    cancellation_lines,
    return_lines,
    net_amount
FROM analytics.v_country_performance
ORDER BY net_amount DESC, country_key
LIMIT 15;

-- 4. Top 15 identified customers by net amount.
SELECT
    customer_id,
    distinct_sale_invoices,
    sale_lines,
    cancellation_lines,
    return_lines,
    net_amount
FROM analytics.v_customer_performance
WHERE customer_id IS NOT NULL
ORDER BY net_amount DESC, customer_key
LIMIT 15;

-- 5. Transaction classification.
SELECT
    transaction_type,
    COUNT(*) AS transaction_lines,
    SUM(net_transaction_amount) AS net_amount
FROM analytics.fact_sales
GROUP BY transaction_type
ORDER BY transaction_type;

-- 6. Data quality overview.
SELECT *
FROM analytics.v_data_quality_summary;

-- 7. Monthly cancellation rate by line count.
SELECT
    year_month,
    sale_count,
    cancellation_count,
    ROUND(
        cancellation_count::numeric
        / NULLIF(
            sale_count + cancellation_count,
            0
        ) * 100,
        2
    ) AS cancellation_line_rate_pct
FROM analytics.mart_monthly_sales
ORDER BY year, month;
