"""Memory-efficient and safe consolidation of Gold Parquet files."""

from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq

from commercelake.warehouse.loader import safe_file

DEFAULT_BATCH_SIZE = 16_384


def consolidate_parquet(
    run_directory: Path,
    files: list[tuple[str, int]],
    output_path: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """Atomically consolidate Gold Parquet files using bounded-memory batches."""
    if batch_size <= 0:
        raise ValueError("Batch size must be positive")

    if not files:
        raise ValueError("Cannot consolidate an empty Parquet inventory")

    run_directory = Path(run_directory).resolve()
    output_path = Path(output_path).resolve()

    if output_path.is_relative_to(run_directory):
        raise ValueError("Output path cannot be inside the Gold run")

    source_paths = [
        (safe_file(run_directory, relative_path), expected_rows)
        for relative_path, expected_rows in files
    ]

    if any(path == output_path for path, _ in source_paths):
        raise ValueError("Output path cannot overwrite a source file")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = output_path.with_name(f".{output_path.name}.{uuid4().hex}.tmp")

    expected_schema = None
    total_rows = 0

    try:
        writer = None

        try:
            for source_path, declared_rows in source_paths:
                parquet = pq.ParquetFile(source_path)

                if parquet.metadata.num_rows != declared_rows:
                    raise ValueError(f"Source row count mismatch: {source_path}")

                if expected_schema is None:
                    expected_schema = parquet.schema_arrow
                    writer = pq.ParquetWriter(
                        temporary_path,
                        expected_schema,
                        compression="snappy",
                    )
                elif not parquet.schema_arrow.equals(
                    expected_schema,
                    check_metadata=False,
                ):
                    raise ValueError(f"Source schema mismatch: {source_path}")

                for batch in parquet.iter_batches(batch_size=batch_size):
                    writer.write_batch(batch)
                    total_rows += batch.num_rows

        finally:
            if writer is not None:
                writer.close()

        expected_total = sum(rows for _, rows in source_paths)

        if total_rows != expected_total:
            raise ValueError(
                "Consolidated row count mismatch: "
                f"expected {expected_total}, got {total_rows}"
            )

        consolidated = pq.ParquetFile(temporary_path)

        if consolidated.metadata.num_rows != expected_total:
            raise ValueError("Consolidated Parquet metadata mismatch")

        if not consolidated.schema_arrow.equals(
            expected_schema,
            check_metadata=False,
        ):
            raise ValueError("Consolidated Parquet schema mismatch")

        temporary_path.replace(output_path)

        return total_rows

    finally:
        temporary_path.unlink(missing_ok=True)
