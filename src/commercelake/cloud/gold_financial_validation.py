"""Validate financial consistency between Gold facts and monthly mart."""

from pathlib import Path

import pyarrow.parquet as pq

from commercelake.gold.monthly_mart import MonthlySalesCollector
from commercelake.warehouse.loader import TABLE_SCHEMAS


def validate_gold_financials(
    fact_files: list[Path],
    mart_file: Path,
    *,
    batch_size: int = 16384,
) -> dict[str, int]:
    """Rebuild monthly metrics and compare every mart value.

    Fact files are processed in bounded batches. No cloud calls are made.
    """
    if not fact_files:
        raise ValueError("At least one fact_sales file is required")

    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    collector = MonthlySalesCollector()

    for path in fact_files:
        parquet = pq.ParquetFile(path)

        if not parquet.schema_arrow.equals(
            TABLE_SCHEMAS["fact_sales"],
            check_metadata=False,
        ):
            raise ValueError(f"Invalid fact_sales schema: {path}")

        for batch in parquet.iter_batches(batch_size=batch_size):
            collector.consume(batch)

    mart = pq.read_table(mart_file)

    if not mart.schema.equals(
        TABLE_SCHEMAS["mart_monthly_sales"],
        check_metadata=False,
    ):
        raise ValueError("Invalid mart_monthly_sales schema")

    collector.validate(mart)

    expected = collector.build()

    if expected.num_rows != mart.num_rows:
        raise ValueError("Monthly period count mismatch")

    expected_rows = {row["year_month"]: row for row in expected.to_pylist()}

    observed_rows = {}

    for row in mart.to_pylist():
        month = row["year_month"]

        if month in observed_rows:
            raise ValueError(f"Duplicate monthly period: {month}")

        observed_rows[month] = row

    if set(expected_rows) != set(observed_rows):
        raise ValueError("Monthly periods do not match fact_sales")

    for month, expected_row in expected_rows.items():
        observed_row = observed_rows[month]

        for field in expected.schema.names:
            if expected_row[field] != observed_row[field]:
                raise ValueError(
                    "Monthly reconciliation mismatch: "
                    f"{month}, field={field}, "
                    f"expected={expected_row[field]}, "
                    f"actual={observed_row[field]}"
                )

    return {
        "fact_rows": collector.total_rows,
        "monthly_periods": expected.num_rows,
    }
