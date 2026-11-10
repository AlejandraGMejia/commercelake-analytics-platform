"""Safe construction and execution of BigQuery CLI commands."""

import subprocess
from pathlib import Path

from commercelake.cloud.bigquery_publisher import (
    DEFAULT_LOCATION,
    validate_identifiers,
)
from commercelake.warehouse.loader import LOAD_ORDER

ALLOWED_TABLES = frozenset(LOAD_ORDER)

DECIMAL_TABLES = frozenset(
    {
        "fact_sales",
        "mart_monthly_sales",
    }
)


def build_load_command(
    project_id: str,
    dataset: str,
    table: str,
    source_path: Path,
    location: str = DEFAULT_LOCATION,
) -> list[str]:
    """Build a BigQuery Parquet load command without executing it."""
    validate_identifiers(project_id, dataset)

    if location != DEFAULT_LOCATION:
        raise ValueError("Unsupported BigQuery location")

    if table not in ALLOWED_TABLES:
        raise ValueError(f"Unsupported CommerceLake table: {table}")

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
        command.append("--decimal_target_types=BIGNUMERIC")

    command.extend(
        [
            "--replace",
            f"{project_id}:{dataset}.{table}",
            str(source_path),
        ]
    )

    return command


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    """Execute a CLI command, raising on nonzero exit status."""
    if not command or command[0] != "bq":
        raise ValueError("Only BigQuery CLI commands are permitted")

    return subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
