"""Export Gold fact_sales Parquet to a BigQuery-compatible CSV candidate."""

import csv
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from commercelake.warehouse.loader import TABLE_SCHEMAS


def serialize_bigquery_csv_value(value, field_type: str) -> str:
    """Serialize one Arrow/Python value without losing decimal precision."""
    if value is None:
        return ""

    if isinstance(value, str) and value == "":
        raise ValueError(
            "Empty strings cannot be safely distinguished from NULL in CSV"
        )

    if field_type == "DATETIME":
        if not isinstance(value, datetime) or value.tzinfo is not None:
            raise ValueError("DATETIME requires a timezone-naive datetime")
        return value.isoformat(sep=" ", timespec="microseconds")

    if field_type == "TIMESTAMP":
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise ValueError("TIMESTAMP requires a timezone-aware datetime")
        utc_value = value.astimezone(UTC)
        return utc_value.isoformat(sep=" ", timespec="microseconds")

    if isinstance(value, Decimal):
        return format(value, "f")

    if isinstance(value, bool):
        return "true" if value else "false"

    return str(value)


def export_fact_sales_csv(
    parquet_files: list[Path],
    output_path: Path,
    *,
    batch_size: int = 10_000,
) -> int:
    """Export validated fact_sales Parquet files in bounded batches.

    This function only writes a local CSV file. It never contacts BigQuery.
    """
    if not parquet_files:
        raise ValueError("At least one Parquet file is required")

    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    expected_schema = TABLE_SCHEMAS["fact_sales"]
    columns = expected_schema.names

    from commercelake.cloud.bigquery_schema_validation import (
        expected_bigquery_schema,
    )

    bigquery_fields = expected_bigquery_schema("fact_sales")
    field_types = {field["name"]: field["type"] for field in bigquery_fields}

    output_path = Path(output_path)
    if output_path.exists():
        raise FileExistsError(f"Refusing to overwrite existing CSV: {output_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    expected_rows = 0

    try:
        with output_path.open("x", newline="", encoding="utf-8") as output:
            writer = csv.writer(output, lineterminator="\n")

            for source_path in parquet_files:
                source_path = Path(source_path)
                parquet = pq.ParquetFile(source_path)

                if not parquet.schema_arrow.equals(
                    expected_schema,
                    check_metadata=False,
                ):
                    raise ValueError(f"Unexpected fact_sales schema: {source_path}")

                expected_rows += parquet.metadata.num_rows

                for batch in parquet.iter_batches(batch_size=batch_size):
                    for record in batch.to_pylist():
                        writer.writerow(
                            [
                                serialize_bigquery_csv_value(
                                    record[column],
                                    field_types[column],
                                )
                                for column in columns
                            ]
                        )
                        total_rows += 1

        if total_rows != expected_rows:
            raise ValueError(
                f"CSV row count mismatch: expected {expected_rows}, got {total_rows}"
            )

    except Exception:
        output_path.unlink(missing_ok=True)
        raise

    return total_rows
