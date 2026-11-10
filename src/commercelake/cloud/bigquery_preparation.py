"""Prepare and validate Gold Parquet files for BigQuery staging."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from commercelake.cloud.bigquery_publisher import PublicationPlan
from commercelake.cloud.parquet_consolidator import consolidate_parquet
from commercelake.warehouse.loader import (
    LOAD_ORDER,
    TABLE_SCHEMAS,
    safe_file,
)


def validate_prepared_file(
    path: Path,
    expected_rows: int,
    expected_schema: pa.Schema | None = None,
) -> None:
    """Validate Parquet existence, row count and optional Arrow schema."""
    path = Path(path)

    if expected_rows < 0:
        raise ValueError("Expected row count cannot be negative")

    if not path.is_file():
        raise FileNotFoundError(f"Missing prepared Parquet: {path}")

    parquet = pq.ParquetFile(path)

    if parquet.metadata.num_rows != expected_rows:
        raise ValueError(
            f"Prepared Parquet row count mismatch: {path}; "
            f"expected {expected_rows}, "
            f"got {parquet.metadata.num_rows}"
        )

    if expected_schema is not None:
        actual_schema = parquet.schema_arrow

        if not actual_schema.equals(
            expected_schema,
            check_metadata=False,
        ):
            raise ValueError(f"Prepared Parquet schema mismatch: {path}")


@contextmanager
def prepare_gold_files(
    plan: PublicationPlan,
    temporary_root: Path | None = None,
) -> Iterator[dict[str, Path]]:
    """Prepare six validated Gold tables and clean temporary files."""
    if set(plan.files_by_table) != set(LOAD_ORDER):
        raise ValueError("Publication plan must contain all six Gold tables")

    # Preflight: reject invalid sources before creating output.
    for table in LOAD_ORDER:
        files = plan.files_by_table[table]

        if not files:
            raise ValueError(f"Empty Gold inventory for {table}")

        expected_schema = TABLE_SCHEMAS[table]

        for relative_path, expected_rows in files:
            source = safe_file(plan.run_directory, relative_path)

            validate_prepared_file(
                path=source,
                expected_rows=expected_rows,
                expected_schema=expected_schema,
            )

    if temporary_root is not None:
        root = Path(temporary_root).resolve()
        root.mkdir(parents=True, exist_ok=True)

        temporary_directory = TemporaryDirectory(
            prefix="commercelake_bq_",
            dir=root,
        )
    else:
        temporary_directory = TemporaryDirectory(
            prefix="commercelake_bq_",
        )

    with temporary_directory as directory:
        work_directory = Path(directory)
        prepared: dict[str, Path] = {}

        for table in LOAD_ORDER:
            files = plan.files_by_table[table]
            expected_rows = sum(rows for _, rows in files)
            expected_schema = TABLE_SCHEMAS[table]

            if len(files) == 1:
                relative_path, _ = files[0]

                output = safe_file(
                    plan.run_directory,
                    relative_path,
                )
            else:
                output = work_directory / f"{table}.parquet"

                consolidate_parquet(
                    run_directory=plan.run_directory,
                    files=files,
                    output_path=output,
                )

            validate_prepared_file(
                path=output,
                expected_rows=expected_rows,
                expected_schema=expected_schema,
            )

            prepared[table] = output

        yield prepared
