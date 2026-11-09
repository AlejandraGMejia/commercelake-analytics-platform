# Bronze Data Quality & Observability

## Overview

CommerceLake implements contract-driven quality validation and observability for the Bronze layer of the UCI Online Retail II dataset.

The objective is to preserve source fidelity, detect structural defects, and measure anomalies before downstream transformations.

- Dataset: UCI Online Retail II
- Layer: Bronze
- Format: Apache Parquet
- Source records: 1,067,371
- Partitions: 25 year-month partitions
- Active run: 43ffa04f507145139c1e08962d040a62

## Architecture

The Bronze quality workflow consists of five components:

1. Versioned ingestion into partitioned Parquet files.
2. Active-run resolution through current.json.
3. YAML-based data quality contracts.
4. Batch-based anomaly observability.
5. JSON reports containing validation evidence.

The quality contract is located at:

config/data_quality/bronze_contract.yaml

## Critical Quality Rules

| Rule | Description |
|---|---|
| BRZ001 | Parquet schema compatibility |
| BRZ002 | File inventory and integrity |
| BRZ003 | Row count and partition reconciliation |
| BRZ004 | Source provenance validation |
| BRZ005 | Source fingerprint presence |

Critical validation verifies:

- Expected schema compatibility.
- Existence and readability of declared Parquet files.
- Manifest and physical row-count reconciliation.
- Partition-level reconciliation.
- Source sheet and row-number provenance.
- Presence of source fingerprints.
- Complete source ingestion.

Mandatory contract checks cannot be disabled.

## Business Observability

| Rule | Metric | Severity | Count | Percentage |
|---|---|---|---:|---:|
| BRZ101 | Missing customer ID | Info | 243,007 | 22.7669% |
| BRZ102 | Negative quantity | Info | 22,950 | 2.1501% |
| BRZ103 | Cancelled invoice | Info | 19,494 | 1.8264% |
| BRZ104 | Non-positive price | Warning | 6,207 | 0.5815% |
| BRZ105 | Repeated fingerprint | Warning | 34,335 | 3.2168% |

Observations can overlap and should not be added together.

Repeated fingerprints identify repeated normalized record content, not necessarily incorrect transactions.

## Data Preservation Policy

Bronze preserves all source records.

The quality workflow does not:

- Delete source records.
- Deduplicate records.
- Remove cancellations.
- Impute missing customer identifiers.
- Modify transaction values.

Business cleaning and standardization belong to the Silver layer.

## Reliability and Security

The implementation includes:

- Active run identifier validation.
- Manifest reference verification.
- Safe file-path resolution.
- Duplicate inventory detection.
- Physical Parquet validation.
- Source provenance checks.
- Row-count reconciliation.
- Automated adversarial tests.

Parquet files are processed in batches to limit memory usage.

Observability uses temporary SQLite storage for fingerprint tracking rather than retaining every fingerprint in Python memory.

## Reproducibility

Run Bronze validation:

    .venv/bin/python -m scripts.validate_bronze

Run Bronze observability:

    .venv/bin/python -m scripts.observe_bronze

Run static checks:

    .venv/bin/python -m ruff check .

Run automated tests:

    .venv/bin/python -m pytest -q

## Validation Results

The active Bronze dataset contains 1,067,371 source records.

The local automated test suite completed successfully:

- 103 tests passed.
- Zero Ruff lint errors.

Generated reports:

- reports/bronze_quality_report.json
- reports/bronze_observability_report.json

## Analytical Implications

Missing customer identifiers affect customer-level analytics.

Negative quantities and cancelled invoices require explicit return and cancellation semantics.

Non-positive prices require careful financial treatment.

Repeated fingerprints require investigation before deduplication decisions.

These findings inform downstream Silver transformations without altering Bronze.

## Next Steps

Sprint 04 will introduce the Silver layer:

- Standardized transaction fields.
- Explicit cancellation and return classification.
- Business quality flags.
- Documented transformation policies.
- Bronze-to-Silver reconciliation.

## Scope

The reported results describe the locally validated Bronze dataset.

Cloud deployment, production orchestration, and continuous monitoring are outside the completed Sprint 03 scope.
