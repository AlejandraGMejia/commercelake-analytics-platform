"""Prepare fact_sales CSV from the authoritative Gold inventory."""

from pathlib import Path

from commercelake.cloud.bigquery_csv_adapter import export_fact_sales_csv
from commercelake.cloud.bigquery_publisher import PublicationPlan
from commercelake.warehouse.loader import safe_file


def prepare_fact_sales_csv(
    plan: PublicationPlan,
    output_path: Path,
    *,
    batch_size: int = 10_000,
) -> int:
    """Export and validate fact_sales before any cloud operation."""
    inventory = plan.files_by_table.get("fact_sales")

    if not inventory:
        raise ValueError("Missing fact_sales Gold inventory")

    parquet_files = []
    expected_rows = 0

    for relative_path, rows in inventory:
        if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
            raise ValueError("Invalid fact_sales inventory row count")

        source = safe_file(plan.run_directory, relative_path)

        if source.suffix.lower() != ".parquet":
            raise ValueError("Invalid fact_sales Parquet source")

        parquet_files.append(source)
        expected_rows += rows

    if len(set(parquet_files)) != len(parquet_files):
        raise ValueError("Duplicate fact_sales Parquet source")

    output_path = Path(output_path)

    exported_rows = export_fact_sales_csv(
        parquet_files,
        output_path,
        batch_size=batch_size,
    )

    if exported_rows != expected_rows:
        output_path.unlink(missing_ok=True)
        raise ValueError(
            "fact_sales CSV row count mismatch: "
            f"expected {expected_rows}, got {exported_rows}"
        )

    return exported_rows
