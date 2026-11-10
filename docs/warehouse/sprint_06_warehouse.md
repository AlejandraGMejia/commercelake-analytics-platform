# Sprint 06 — PostgreSQL Analytical Warehouse

## Objective

Publish the validated Gold datasets into PostgreSQL and provide
a reproducible SQL analytics layer.

## Architecture

Raw Excel → Bronze Parquet → Silver Parquet → Gold Parquet
→ PostgreSQL Warehouse → Analytical SQL Views

PostgreSQL is the serving layer. Gold Parquet remains the
upstream analytical source.

## Warehouse schema

| Table | Rows | Purpose |
|---|---:|---|
| dim_product | 5,305 | Product attributes |
| dim_customer | 5,943 | Customer identifiers |
| dim_country | 44 | Country attributes |
| dim_date | 739 | Calendar attributes |
| fact_sales | 1,067,371 | Transaction-level facts |
| mart_monthly_sales | 25 | Monthly business metrics |

The fact table includes a PostgreSQL-generated identity
key and foreign keys to the analytical dimensions.

## Loading strategy

The Python loader:

1. Resolves the active Gold run.
2. Validates the manifest and Parquet inventory.
3. Loads datasets into temporary staging tables.
4. Reconciles row counts and financial amounts.
5. Verifies that the active Gold pointer is unchanged.
6. Publishes all warehouse tables transactionally.
7. Commits only after successful publication.

The staging fact table excludes the PostgreSQL-generated
fact_id column.

Publication replaces the complete warehouse snapshot;
incremental upserts are not implemented.

## Analytics layer

Five reusable PostgreSQL views:

- analytics.v_monthly_performance
- analytics.v_product_performance
- analytics.v_country_performance
- analytics.v_customer_performance
- analytics.v_data_quality_summary

Business queries are stored in:

sql/warehouse/003_business_queries.sql

## Financial semantics

Transactions are classified as sale, cancellation,
return, or review.

Review records have NULL net amounts and do not
contribute to financial aggregates.

Returns are retained as a separate category. Their
aggregate net amount is zero in this dataset version.

No source currency is assumed.

## Validation

The SQL regression suite is located at:

sql/warehouse/004_validate_analytics.sql

It verifies:

- Warehouse table row counts
- Financial reconciliation across facts, marts, and views
- Transaction classification
- Financial NULL handling
- Foreign-key relationships
- Repeated fingerprints

Validation runs inside a read-only transaction.

### Confirmed results

| Metric | Result |
|---|---:|
| Fact rows | 1,067,371 |
| Monthly periods | 25 |
| Net amount | 19,445,926.7080 |
| Orphan fact rows | 0 |
| Review rows | 2,750 |
| Repeated fingerprints | 34,335 |

All SQL assertions passed.

## Reproducibility

Run the SQL files in this order:

1. sql/warehouse/001_create_schema.sql
2. Load Gold data using the Python warehouse loader.
3. sql/warehouse/002_create_analytics_views.sql
4. sql/warehouse/003_business_queries.sql
5. sql/warehouse/004_validate_analytics.sql

Example SQL execution command:

    psql -h 127.0.0.1 \
      -U commercelake_app \
      -d commercelake_warehouse \
      -v ON_ERROR_STOP=1 \
      -f sql/warehouse/004_validate_analytics.sql

## Scope and limitations

- Validation expectations refer to the current Gold snapshot.
- Publication uses full-snapshot replacement.
- Views are standard PostgreSQL views, not materialized.
- Rankings may include postage-related codes.
- Customer identifiers are missing in some records.
- Repeated fingerprints are not necessarily data errors.
- December 2011 is an incomplete reporting month.
- Google Cloud deployment is not yet implemented.
