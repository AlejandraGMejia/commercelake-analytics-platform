# CommerceLake Analytics Platform

**Retail data engineering and analytics | Python · PyArrow · Parquet · PostgreSQL · BigQuery**

CommerceLake is a local-first, end-to-end analytics platform built on the UCI Online Retail II dataset. It preserves historical retail transactions, validates and classifies them, publishes a dimensional Gold model, and supports SQL analytics in PostgreSQL and Google BigQuery.

## Project at a glance

| Verified metric | Result |
|---|---:|
| Source transaction lines | 1,067,371 |
| Gold fact rows | 1,067,371 |
| Historical monthly periods | 25 |
| Gold Parquet files | 136 (including 131 fact fragments) |
| Published BigQuery tables | 6 |
| Automated tests | 691 passed |
| Reconciled net transaction amount | 19,445,926.7080 |

**Financial interpretation:** The net transaction amount is a signed transactional measure in source monetary units. The currency has not been independently verified, and this measure is **not certified accounting revenue or profit**.

## Business problem

Historical retail transactions include cancellations, returns, missing customer identifiers, repeated lines, and inconsistent descriptions. Aggregating these records without explicit rules can distort commercial reporting. CommerceLake retains source traceability, classifies transactions, tracks quality findings, and reconciles analytical measures.

## Implemented architecture

```text
UCI Online Retail II (Excel)
             |
             v
      Python ingestion
             |
             v
    Bronze: source records
             |
             v
 Silver: validated, classified lines
             |
             v
 Gold: dimensional model + monthly mart
             |
       +-----+-----+
       |           |
       v           v
  PostgreSQL   Google BigQuery
  SQL views    Six Gold tables
  and queries  (initial deployment)
```

The platform uses Python, PyArrow, Apache Parquet, SQL, PostgreSQL, Google BigQuery, pytest, Ruff, and Git. **dbt, Apache Airflow, Terraform, Power BI, and automated CI/CD are not implemented** in this version.

## Gold analytical model

| Table | Rows |
|---|---:|
| `fact_sales` | 1,067,371 |
| `dim_product` | 5,305 |
| `dim_customer` | 5,943 |
| `dim_country` | 44 |
| `dim_date` | 739 |
| `mart_monthly_sales` | 25 |

The fact grain is **one row per validated Silver source line**. Gold does not automatically deduplicate the original transaction lines. Unknown dimension members preserve records with missing identifiers.

### Transaction classification

| Type | Lines |
|---|---:|
| Sale | 1,041,670 |
| Cancellation | 19,494 |
| Return | 3,457 |
| Review | 2,750 |
| **Total** | **1,067,371** |

Review lines remain available for analysis but are excluded from the published net financial total.

### Data quality observations

| Finding | Lines |
|---|---:|
| Missing customer ID | 243,007 |
| Exact duplicates within source worksheets | 12,133 |
| Nonpositive prices | 6,207 |
| Missing descriptions | 4,382 |

These findings can overlap. An exact duplicate is a review signal, not automatic proof of an invalid transaction.

## Reconciliation and cloud publication

The Gold fact and monthly mart reconcile to **19,445,926.7080** in net transaction amount. The initial BigQuery deployment published six tables in project `commercelake-analytics`, dataset `commercelake_gold`, location `US`. Their row counts reconcile with local Gold; the BigQuery fact amount is numerically equivalent. Financial columns use `BIGNUMERIC`. Independent, full row-by-row cross-system equality has not been established.

The initial deployment used local Parquet and the BigQuery CLI, without Google Cloud Storage buckets. A newer staging workflow supports a legacy `parquet_insert` strategy and an opt-in `csv_direct` strategy. **CSV direct staging passed automated tests and a local dry-run; a live CSV cloud load has not been verified.** Neither this local validation nor the query-budget setting should be interpreted as a comprehensive cloud cost safeguard.

## Selected analytical findings

- November 2011 had the highest observed monthly net amount: **1,461,756.2500**.
- The United Kingdom contributed approximately **85.1%** of the observed net amount.
- Some product-code rankings include postage or service charges.
- December 2011 is a partial reporting month and should not be compared directly with complete months.

These are descriptive findings, not forecasts or causal claims.

## Validation

In the project environment:

```bash
python -m pytest -q
python -m ruff check .
```

Latest verified local result: **691 passed; Ruff clean**. Reproducing the full pipeline requires the source dataset and the expected local processing artifacts.

## Documentation

- [Implemented architecture](docs/architecture/overview.md)
- [Gold analytical model](docs/data/analytical_model.md)
- [Gold analytical contract](docs/gold_analytical_contract.md)
- [Data quality findings](docs/data/data_quality_findings.md)
- [Gold analytics report](reports/gold_analytics_report.md)
- [Business findings](reports/sprint_06_business_findings.md)
- [BigQuery validation](reports/sprint_07_bigquery_validation.md)

## Scope and limitations

The source covers historical transactions from December 2009 through December 2011. This is a batch, full-snapshot implementation, not a real-time production service. Customer IDs can be missing, descriptions may vary, source currency is unverified, and December 2011 is incomplete. BigQuery Sandbox tables may be subject to expiration. The newer CSV staging path remains unverified through a live load, and cloud orchestration/CI/CD are outside the implemented scope.

**Portfolio focus:** reproducible pipelines, data quality, dimensional modeling, analytical SQL, financial reconciliation, and careful cloud publication.
