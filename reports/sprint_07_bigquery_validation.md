# Sprint 07 — BigQuery Gold Validation

## Deployment

- Google Cloud project: `commercelake-analytics`
- BigQuery dataset: `commercelake_gold`
- Dataset location: `US`
- Source layer: validated local Gold Parquet
- Active Gold run: `591c8097f0554414be18648e3587d2b3`
- Published tables: 6
- Deployment method: BigQuery CLI, local Parquet load jobs

## Row-count reconciliation

| Table | Expected rows | BigQuery rows | Result |
|---|---:|---:|---|
| dim_country | 44 | 44 | PASS |
| dim_customer | 5,943 | 5,943 | PASS |
| dim_date | 739 | 739 | PASS |
| dim_product | 5,305 | 5,305 | PASS |
| fact_sales | 1,067,371 | 1,067,371 | PASS |
| mart_monthly_sales | 25 | 25 | PASS |

## Financial reconciliation

- Metric: `SUM(fact_sales.net_transaction_amount)`
- Expected value: `19445926.7080`
- BigQuery displayed value: `19445926.708`
- Numeric equality: PASS
- BigQuery financial type: `BIGNUMERIC`
- Currency: not independently verified

## Engineering decisions

1. Publish only the active Gold run.
2. Preserve financial precision using BigQuery BIGNUMERIC.
3. Consolidate 131 fact Parquet fragments into one temporary
   publication artifact without modifying source files.
4. Use replacement loads to avoid appending duplicate records.
5. Keep cloud tables unpartitioned during the initial deployment.
6. Avoid paid Google Cloud infrastructure and Cloud Storage buckets.

## Operational considerations

The BigQuery dataset has a 60-day default table expiration.
Tables must be recreated after expiration.

The deployment currently uses manually executed CLI commands.
Automated, tested deployment is a subsequent engineering task.

## Validation scope

Confirmed:
- Six BigQuery tables exist.
- All six row counts match the expected Gold counts.
- The net transaction amount matches the expected local total.
- Financial columns in fact_sales use BIGNUMERIC.

Not yet independently confirmed:
- Full row-level equality across systems.
- Referential integrity of every dimension key.
- Complete reconciliation of every financial metric.
- Automated deployment and recovery.

## Result

PASS — Initial Gold deployment and reconciliation completed.
