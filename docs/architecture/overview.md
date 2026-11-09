# Architecture Overview

## Proposed architecture

1. Source systems: API, CSV/JSON, PostgreSQL
2. Ingestion: Python
3. Raw storage: Google Cloud Storage
4. Analytical warehouse: BigQuery
5. Transformation: dbt Core
6. Orchestration: Apache Airflow
7. Infrastructure: Terraform
8. CI/CD: GitHub Actions
9. Analytics: Power BI

## Design principles

- Local-first development
- Free-tier-conscious deployment
- Incremental ingestion where supported
- Idempotent pipeline execution
- Separation of raw and analytical layers
- Data quality testing
- Least-privilege access

## Pending decisions

- Source datasets
- GCP project and region
- Storage formats
- Incremental loading strategy
- Data retention policy
- Cloud deployment scope
- Cost and quota safeguards
