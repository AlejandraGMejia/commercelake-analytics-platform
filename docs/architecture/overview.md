# CommerceLake — Implemented Architecture

## System purpose

CommerceLake transforms UCI Online Retail II historical Excel data into validated, traceable analytical tables. It follows a local-first Bronze → Silver → Gold architecture, with PostgreSQL reporting and an initial Google BigQuery Gold deployment.

## End-to-end flow

```text
Online Retail II Excel (1,067,371 source lines)
                    |
                    v
            Python ingestion
                    |
                    v
          Bronze: source preservation
          and quality observations
                    |
                    v
          Silver: validated lines,
          classifications and lineage
                    |
                    v
          Gold: versioned Parquet
          dimensional model + mart
                    |
             +------+------+
             |             |
             v             v
        PostgreSQL     Google BigQuery
        SQL reporting  Six Gold tables
```

## Bronze

Bronze preserves source information and exposes data-quality observations without silently dropping transactions. Source profiling includes missing values, repeated records, and cancellation-related patterns.

## Silver

Silver produces validated, classified transaction lines while retaining source lineage. It distinguishes sales, cancellations, returns, and review records so downstream measures use explicit business rules.

## Gold

Gold materializes the analytical model in Parquet, with versioned runs and an active-run reference. The official Gold run is `591c8097f0554414be18648e3587d2b3`.

Published model:

| Table | Rows |
|---|---:|
| `fact_sales` | 1,067,371 |
| `dim_product` | 5,305 |
| `dim_customer` | 5,943 |
| `dim_country` | 44 |
| `dim_date` | 739 |
| `mart_monthly_sales` | 25 |

The fact preserves one record per validated Silver line. Gold contains 131 fact Parquet fragments and 136 Parquet files overall. Financial calculations preserve decimal precision; the Gold fact and monthly mart net totals reconcile to `19,445,926.7080`.

## PostgreSQL reporting

The relational analytical warehouse provides **five reusable SQL views** and **seven business queries** for commercial analysis. This is a SQL reporting layer, not a separate orchestration engine.

## Google BigQuery deployment

- Project: `commercelake-analytics`
- Dataset: `commercelake_gold`
- Location: `US`
- Published Gold tables: six
- Initial loading: local Parquet via BigQuery CLI; no GCS bucket required

The six published table row counts reconcile with local Gold. The BigQuery fact net amount is numerically equivalent to the local Gold total. Financial columns use `BIGNUMERIC`. A full independent row-level comparison has not been established.

## Staging publication strategies

The CLI supports two distinct staging strategies:

- **`parquet_insert`**: legacy Parquet/SQL-insert staging route.
- **`csv_direct`**: opt-in route designed to export the fact to CSV and load into typed BigQuery staging tables, avoiding problematic Parquet DATETIME inference in that path.

The `csv_direct` path has passed automated tests and a **local manifest dry-run** covering the six tables and 136 Gold files. **No live BigQuery CSV load has been verified.** Local dry-run does not execute cloud commands.

Execution is gated by an explicit confirmation phrase and includes checks such as staging-table existence and schema/row-count validation. A maximum-bytes-billed query setting is not a general spending cap. Cloud billing is disabled in the current environment; no new cloud writes are required to validate the repository locally.

## Design decisions

1. **Preserve original lines:** repeated source records are not automatically deleted.
2. **Make classifications explicit:** review transactions remain visible but are excluded from published net financial totals.
3. **Preserve missing-key transactions:** unknown dimension members avoid unintended fact loss.
4. **Use decimal financial measures:** avoid floating-point drift in reconciliation.
5. **Version Gold runs:** manifests and active-run selection improve traceability.
6. **Separate local validation from cloud execution:** tests and dry-runs should not mutate official cloud tables.

## Not implemented / limitations

This version does **not** implement Apache Airflow, dbt, Terraform, Power BI, automated CI/CD, streaming, or a fully automated production cloud deployment. Data is historical (December 2009–December 2011), December 2011 is partial, and the source currency is unverified. BigQuery Sandbox table expiration may affect future access.

## Related documentation

- [Gold analytical model](../data/analytical_model.md)
- [Gold analytical contract](../gold_analytical_contract.md)
- [BigQuery validation](../../reports/sprint_07_bigquery_validation.md)
