"""Build safe BigQuery staging table creation commands."""

import json
from pathlib import Path

from commercelake.cloud.bigquery_ingestion_schema import (
    expected_ingestion_schema,
)
from commercelake.cloud.bigquery_ingestion_staging import (
    ingestion_staging_table_name,
)
from commercelake.cloud.bigquery_publisher import (
    DEFAULT_LOCATION,
    validate_identifiers,
)
from commercelake.cloud.bigquery_schema_validation import (
    expected_bigquery_schema,
)
from commercelake.cloud.bigquery_staging import staging_table_name

DEFAULT_STAGING_EXPIRATION = 86400


def build_staging_create_command(
    project_id: str,
    dataset: str,
    table: str,
    run_id: str,
    schema_path: Path,
    location: str = DEFAULT_LOCATION,
    expiration_seconds: int = DEFAULT_STAGING_EXPIRATION,
    *,
    ingestion: bool = False,
) -> list[str]:
    """Build a creation command with an exact validated schema."""
    validate_identifiers(project_id, dataset)

    if location != DEFAULT_LOCATION:
        raise ValueError("Unsupported BigQuery location")

    if not isinstance(ingestion, bool):
        raise TypeError("ingestion must be a boolean")

    if ingestion and table != "fact_sales":
        raise ValueError("Ingestion staging is only supported for fact_sales")

    if ingestion:
        stage = ingestion_staging_table_name(table, run_id)
        expected_schema = expected_ingestion_schema(table)
    else:
        stage = staging_table_name(table, run_id)
        expected_schema = expected_bigquery_schema(table)

    schema_path = Path(schema_path)

    if schema_path.suffix.lower() != ".json":
        raise ValueError("BigQuery schema must be a JSON file")

    if not schema_path.is_file():
        raise FileNotFoundError(
            f"Missing BigQuery schema JSON: {schema_path}"
        )

    if (
        isinstance(expiration_seconds, bool)
        or not isinstance(expiration_seconds, int)
        or expiration_seconds <= 0
    ):
        raise ValueError("Invalid staging expiration")

    try:
        schema = json.loads(
            schema_path.read_text(encoding="utf-8")
        )
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise ValueError("Invalid BigQuery schema JSON") from exc

    if schema != expected_schema:
        raise ValueError(
            f"BigQuery schema does not match "
            f"{'ingestion' if ingestion else 'Gold'} contract: {table}"
        )

    return [
        "bq",
        f"--location={location}",
        "mk",
        "--table",
        f"--expiration={expiration_seconds}",
        f"--schema={schema_path}",
        f"{project_id}:{dataset}.{stage}",
    ]
