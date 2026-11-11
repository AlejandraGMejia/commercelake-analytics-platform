"""Build explicit, isolated BigQuery CSV staging load commands."""

from pathlib import Path

from commercelake.cloud.bigquery_publisher import (
    DEFAULT_LOCATION,
    validate_identifiers,
)
from commercelake.cloud.bigquery_staging import staging_table_name


def build_fact_sales_csv_load_command(
    *,
    project_id: str,
    dataset: str,
    run_id: str,
    source_path: Path,
    location: str = DEFAULT_LOCATION,
) -> list[str]:
    """Build a non-replacing CSV load command for fact_sales staging."""
    validate_identifiers(project_id, dataset)

    if location != DEFAULT_LOCATION:
        raise ValueError("Unsupported BigQuery location")

    stage = staging_table_name("fact_sales", run_id)
    source = Path(source_path).resolve()

    if source.suffix.lower() != ".csv":
        raise ValueError("BigQuery CSV source must have a .csv extension")

    if not source.is_file():
        raise FileNotFoundError(f"Missing BigQuery CSV source: {source}")

    return [
        "bq",
        f"--location={location}",
        "load",
        "--source_format=CSV",
        "--noreplace",
        "--skip_leading_rows=0",
        "--allow_quoted_newlines=true",
        "--max_bad_records=0",
        f"{project_id}:{dataset}.{stage}",
        str(source),
    ]
