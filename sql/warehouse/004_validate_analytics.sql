-- CommerceLake warehouse and analytics validation.
-- Read-only checks. Any failed assertion stops execution.

BEGIN TRANSACTION READ ONLY;

DO $$
DECLARE
    actual BIGINT;
    actual_amount NUMERIC;
    expected_amount CONSTANT NUMERIC := 19445926.7080;
    item RECORD;
BEGIN
    -- Warehouse row counts.
    FOR item IN
        SELECT *
        FROM (
            VALUES
                ('dim_product', 5305::BIGINT),
                ('dim_customer', 5943::BIGINT),
                ('dim_country', 44::BIGINT),
                ('dim_date', 739::BIGINT),
                ('fact_sales', 1067371::BIGINT),
                ('mart_monthly_sales', 25::BIGINT)
        ) AS expected(table_name, row_count)
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM analytics.%I',
            item.table_name
        )
        INTO actual;

        IF actual <> item.row_count THEN
            RAISE EXCEPTION
                'Row count mismatch in %: expected %, got %',
                item.table_name, item.row_count, actual;
        END IF;
    END LOOP;

    -- Validate all financial aggregation paths.
    FOR item IN
        SELECT *
        FROM (
            VALUES
                ('fact_sales', 'net_transaction_amount'),
                ('mart_monthly_sales', 'net_amount'),
                ('v_monthly_performance', 'net_amount'),
                ('v_product_performance', 'net_amount'),
                ('v_country_performance', 'net_amount'),
                ('v_customer_performance', 'net_amount')
        ) AS expected(object_name, amount_column)
    LOOP
        EXECUTE format(
            'SELECT SUM(%I) FROM analytics.%I',
            item.amount_column,
            item.object_name
        )
        INTO actual_amount;

        IF actual_amount IS DISTINCT FROM expected_amount THEN
            RAISE EXCEPTION
                'Financial mismatch in %: expected %, got %',
                item.object_name, expected_amount, actual_amount;
        END IF;
    END LOOP;

    -- Validate transaction classification.
    SELECT COUNT(*) INTO actual
    FROM analytics.fact_sales
    WHERE transaction_type = 'sale';

    IF actual <> 1041670 THEN
        RAISE EXCEPTION 'Sale classification mismatch: %', actual;
    END IF;

    SELECT COUNT(*) INTO actual
    FROM analytics.fact_sales
    WHERE transaction_type = 'cancellation';

    IF actual <> 19494 THEN
        RAISE EXCEPTION 'Cancellation classification mismatch: %', actual;
    END IF;

    SELECT COUNT(*) INTO actual
    FROM analytics.fact_sales
    WHERE transaction_type = 'return';

    IF actual <> 3457 THEN
        RAISE EXCEPTION 'Return classification mismatch: %', actual;
    END IF;

    SELECT COUNT(*) INTO actual
    FROM analytics.fact_sales
    WHERE transaction_type = 'review';

    IF actual <> 2750 THEN
        RAISE EXCEPTION 'Review classification mismatch: %', actual;
    END IF;

    -- Validate missing financial amounts.
    SELECT COUNT(*) INTO actual
    FROM analytics.fact_sales
    WHERE net_transaction_amount IS NULL
      AND transaction_type <> 'review';

    IF actual <> 0 THEN
        RAISE EXCEPTION 'Unexpected null financial amounts: %', actual;
    END IF;

    -- Verify foreign-key relationships independently.
    SELECT COUNT(*) INTO actual
    FROM analytics.fact_sales f
    LEFT JOIN analytics.dim_product p
        ON f.product_key = p.product_key
    LEFT JOIN analytics.dim_customer c
        ON f.customer_key = c.customer_key
    LEFT JOIN analytics.dim_country co
        ON f.country_key = co.country_key
    LEFT JOIN analytics.dim_date d
        ON f.date_key = d.date_key
    WHERE p.product_key IS NULL
       OR c.customer_key IS NULL
       OR co.country_key IS NULL
       OR (f.date_key IS NOT NULL AND d.date_key IS NULL);

    IF actual <> 0 THEN
        RAISE EXCEPTION 'Orphan fact rows: %', actual;
    END IF;

    -- Verify the quality view.
    SELECT repeated_fingerprint_rows INTO actual
    FROM analytics.v_data_quality_summary;

    IF actual <> 34335 THEN
        RAISE EXCEPTION 'Fingerprint reconciliation mismatch: %', actual;
    END IF;

    RAISE NOTICE 'WAREHOUSE VALIDATION: ALL CHECKS PASSED';
END
$$;

COMMIT;
