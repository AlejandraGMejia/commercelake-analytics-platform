# CommerceLake Analytics Platform

End-to-end cloud data platform for retail analytics.

## Project objective

Design and implement a reproducible retail analytics
platform using modern data engineering practices.

## Planned technology stack

- Python and SQL
- Google Cloud Storage
- Google BigQuery
- dbt Core
- Apache Airflow
- Terraform
- GitHub Actions
- Power BI

## Architecture

Data Sources -> Ingestion -> Cloud Storage
-> BigQuery -> dbt -> Analytics

## Engineering principles

- Modular architecture
- Automated testing
- Data quality validation
- Incremental and idempotent processing
- Secure credential management
- Cost-conscious cloud infrastructure
- Reproducible development

## Project status

Sprint 01 - Architecture and project foundation.

## Local setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
pytest -q
ruff check .
```

## Cloud cost policy

Cloud resources will be provisioned only after
reviewing current pricing, quotas, IAM permissions,
and billing safeguards.

## License

To be determined before public release.
