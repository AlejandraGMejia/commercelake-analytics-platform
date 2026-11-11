"""Build safe BigQuery staging commands for Gold publication."""

import re
from pathlib import Path

from commercelake.cloud.bigquery_cli import (
    ALLOWED_TABLES,
    DECIMAL_TABLES,
    DECIMAL_TARGET_TYPES,
)
from commercelake.cloud.bigquery_publisher import (
    DEFAULT_LOCATION,
    validate_identifiers,
)

RUN_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")


def staging_table_name(table: str, run_id: str) -> str:
    """Generate a deterministic staging table name for a Gold run."""
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Unsupported CommerceLake table: {table}")

    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError("Invalid Gold run ID")

    return f"_stage_{table}_{run_id[:8]}"


def build_staging_load_command(
    project_id: str,
    dataset: str,
    table: str,
    run_id: str,
    source_path: Path,
    location: str = DEFAULT_LOCATION,
) -> list[str]:
    """Build a staging load command without executing it."""
    validate_identifiers(project_id, dataset)

    if location != DEFAULT_LOCATION:
        raise ValueError("Unsupported BigQuery location")

    stage_table = staging_table_name(table, run_id)
    source_path = Path(source_path)

    if source_path.suffix.lower() != ".parquet":
        raise ValueError("BigQuery source must be a Parquet file")

    command = [
        "bq",
        f"--location={location}",
        "load",
        "--source_format=PARQUET",
    ]

    if table in DECIMAL_TABLES:
        command.extend(DECIMAL_TARGET_TYPES)

    command.extend(
        [
            "--replace",
            f"{project_id}:{dataset}.{stage_table}",
            str(source_path),
        ]
    )

    return command
