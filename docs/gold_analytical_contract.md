# Sprint 05 — Gold Analytical Contract

## Objective

Build a reproducible dimensional analytics layer from the
validated CommerceLake Silver dataset.

Gold must preserve transactional traceability, prevent
dimension-related row multiplication, and expose consistent
commercial metrics.

## Source

- Layer: Silver
- Dataset: Online Retail II
- Source records: 1,067,371
- Source files: 131 Parquet
- Source partitions: 25

Gold must consume the active, validated Silver run.

## Fact table: fact_sales

### Grain

One row per original Silver transaction line.

The fact table must not aggregate or deduplicate source rows.

### Required attributes

- invoice
- stock_code
- customer_id
- country
- invoice_date
- quantity
- unit_price
- transaction_type
- gross_amount
- source_sheet
- source_row_number
- row_fingerprint

Additional dimensional keys and metric fields may be added
without losing source traceability.

## Dimensions

### dim_product

Business key: stock_code

One row per distinct product code.

A deterministic canonical description must be selected when
multiple descriptions exist.

Observed profiling:
- 5,304 distinct product codes
- 1,214 codes with multiple descriptions
- 4,382 transaction lines with missing descriptions

### dim_customer

Business key: customer_id

One row per distinct identified customer.

Missing customer identifiers must map to a dedicated
unknown-member key without discarding transactions.

Observed profiling:
- 5,942 distinct identified customers
- 243,007 lines with missing customer IDs

### dim_country

Business key: country

Country must be associated with the transaction, not inferred
as a permanent customer attribute.

Observed profiling:
- 43 distinct countries
- 13 customers associated with multiple countries

### dim_date

Business key: calendar date

One row per calendar date within the supported analytical
period.

Transaction timestamps must remain available in fact_sales.

### Invoice handling

Invoice is a degenerate dimension in fact_sales.

Transaction type must not be inferred exclusively from
invoice-level classification.

Observed profiling:
- 53,628 distinct invoices
- 125 invoices with multiple transaction types

## Financial metric definitions

### Sale amount

Sum of gross_amount for transaction_type = sale.

### Cancellation amount

Sum of gross_amount for transaction_type = cancellation.

### Return amount

Sum of gross_amount for transaction_type = return.

### Review amount

Tracked separately and excluded from published financial KPIs
until the business rules are validated.

### Net transaction amount

Sale amount + cancellation amount + return amount.

This is a signed transactional measure, not a certified
accounting revenue measure.

## Required validations

1. Gold fact rows equal Silver rows.
2. Every fact row maps to valid dimensional members.
3. Dimension business keys are unique.
4. No source rows are lost or duplicated by dimensional joins.
5. Financial metrics reconcile with Silver classifications.
6. Source lineage remains available.
7. Repeated builds produce consistent business results.
8. Gold runs are versioned and published only after validation.

## Implementation principles

- Read source data in bounded batches.
- Preserve Decimal precision for monetary calculations.
- Avoid arbitrary selection of dimensional attributes.
- Keep transformation logic independently testable.
- Write partitioned Parquet datasets.
- Publish manifests and active pointers atomically.
- Do not overwrite Bronze or Silver data.
