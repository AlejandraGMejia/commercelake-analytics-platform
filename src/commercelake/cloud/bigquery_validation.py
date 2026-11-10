"""BigQuery table metadata validation for Gold publication."""

import json
import re

from commercelake.cloud.bigquery_publisher import (
    DEFAULT_LOCATION,
    validate_identifiers,
)

TABLE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ROW_COUNT_PATTERN = re.compile(r"^[0-9]+$")


def build_show_command(
    project_id: str,
    dataset: str,
    table: str,
    location: str = DEFAULT_LOCATION,
) -> list[str]:
    """Build a BigQuery metadata command without executing it."""
    validate_identifiers(project_id, dataset)

    if location != DEFAULT_LOCATION:
        raise ValueError("Unsupported BigQuery location")

    if not TABLE_PATTERN.fullmatch(table):
        raise ValueError("Invalid BigQuery table name")

    return [
        "bq",
        f"--location={location}",
        "show",
        "--format=prettyjson",
        f"{project_id}:{dataset}.{table}",
    ]


def parse_table_row_count(output: str) -> int:
    """Extract a nonnegative integer row count from BigQuery metadata."""
    try:
        metadata = json.loads(output)
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid BigQuery metadata JSON") from exc

    if not isinstance(metadata, dict):
        raise TypeError("BigQuery metadata must be a JSON object")

    if "numRows" not in metadata:
        raise ValueError("BigQuery metadata missing numRows")

    raw_rows = metadata["numRows"]

    if isinstance(raw_rows, bool):
        raise TypeError("Invalid BigQuery row count type")

    if isinstance(raw_rows, int):
        rows = raw_rows
    elif isinstance(raw_rows, str) and ROW_COUNT_PATTERN.fullmatch(raw_rows):
        rows = int(raw_rows)
    else:
        raise ValueError("Invalid BigQuery row count")

    if rows < 0:
        raise ValueError("BigQuery row count cannot be negative")

    return rows


def validate_table_row_count(
    table: str,
    expected_rows: int,
    metadata_output: str,
) -> int:
    """Raise when BigQuery table rows differ from Gold inventory."""
    actual_rows = parse_table_row_count(metadata_output)

    if actual_rows != expected_rows:
        raise ValueError(
            f"BigQuery row count mismatch for {table}: "
            f"expected {expected_rows}, got {actual_rows}"
        )

    return actual_rows
