BEGIN;

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key TEXT PRIMARY KEY,
    stock_code TEXT,
    description TEXT,
    description_variants BIGINT NOT NULL
        CHECK (description_variants >= 0)
);

CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key TEXT PRIMARY KEY,
    customer_id TEXT
);

CREATE TABLE IF NOT EXISTS analytics.dim_country (
    country_key TEXT PRIMARY KEY,
    country TEXT
);

CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key INTEGER PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    day INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
    quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    day_of_week INTEGER NOT NULL CHECK (day_of_week BETWEEN 1 AND 7)
);

CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    fact_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    invoice TEXT,
    stock_code TEXT,
    description TEXT,
    quantity BIGINT,
    invoice_date TIMESTAMP,
    unit_price NUMERIC(18,4),
    customer_id TEXT,
    country TEXT,
    source_sheet TEXT NOT NULL,
    source_row_number BIGINT NOT NULL,
    row_fingerprint TEXT NOT NULL,

    transaction_type TEXT NOT NULL
        CHECK (
            transaction_type IN (
                'sale',
                'cancellation',
                'return',
                'review'
            )
        ),

    gross_amount NUMERIC(38,4),

    is_customer_missing BOOLEAN NOT NULL,
    is_price_non_positive BOOLEAN NOT NULL,
    is_date_missing BOOLEAN NOT NULL,

    silver_processed_at TIMESTAMPTZ NOT NULL,

    product_key TEXT NOT NULL
        REFERENCES analytics.dim_product(product_key),

    customer_key TEXT NOT NULL
        REFERENCES analytics.dim_customer(customer_key),

    country_key TEXT NOT NULL
        REFERENCES analytics.dim_country(country_key),

    date_key INTEGER
        REFERENCES analytics.dim_date(date_key),

    net_transaction_amount NUMERIC(38,4),

    CONSTRAINT fact_review_net_amount_check
        CHECK (
            (
                transaction_type = 'review'
                AND net_transaction_amount IS NULL
            )
            OR
            (
                transaction_type <> 'review'
                AND net_transaction_amount IS NOT NULL
            )
        )
);

CREATE TABLE IF NOT EXISTS analytics.mart_monthly_sales (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    year_month TEXT NOT NULL UNIQUE,

    transaction_count BIGINT NOT NULL,
    sale_count BIGINT NOT NULL,
    cancellation_count BIGINT NOT NULL,
    return_count BIGINT NOT NULL,
    review_count BIGINT NOT NULL,
    identified_customer_count BIGINT NOT NULL,

    sales_amount NUMERIC(38,4) NOT NULL,
    cancellation_amount NUMERIC(38,4) NOT NULL,
    return_amount NUMERIC(38,4) NOT NULL,
    net_amount NUMERIC(38,4) NOT NULL,

    PRIMARY KEY (year, month),

    CONSTRAINT monthly_transaction_reconciliation
        CHECK (
            transaction_count =
                sale_count
                + cancellation_count
                + return_count
                + review_count
        ),

    CONSTRAINT monthly_amount_reconciliation
        CHECK (
            net_amount =
                sales_amount
                + cancellation_amount
                + return_amount
        )
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date
    ON analytics.fact_sales(date_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_product
    ON analytics.fact_sales(product_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer
    ON analytics.fact_sales(customer_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_country
    ON analytics.fact_sales(country_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_transaction_type
    ON analytics.fact_sales(transaction_type);

COMMIT;
