"""Export authoritative Gold schemas for BigQuery table creation."""

import json
from pathlib import Path

from commercelake.cloud.bigquery_schema_validation import (
    expected_bigquery_schema,
)
from commercelake.warehouse.loader import LOAD_ORDER


def export_bigquery_schema(table: str, destination: Path) -> Path:
    """Write the complete Gold BigQuery schema as a JSON array."""
    if table not in LOAD_ORDER:
        raise ValueError(f"Unsupported CommerceLake table: {table}")

    destination = Path(destination)

    if destination.suffix.lower() != ".json":
        raise ValueError("BigQuery schema destination must be a JSON file")

    if not destination.parent.is_dir():
        raise FileNotFoundError(
            f"BigQuery schema directory does not exist: {destination.parent}"
        )

    fields = expected_bigquery_schema(table)

    destination.write_text(
        json.dumps(fields, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    return destination
