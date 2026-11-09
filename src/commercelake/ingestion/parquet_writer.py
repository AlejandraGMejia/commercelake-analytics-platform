"""Validated, atomic Parquet file writing."""

import os
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from commercelake.ingestion.schema import RETAIL_SCHEMA


def write_parquet(rows, destination):
    """Write normalized records to a verified Parquet file."""
    destination = Path(destination)

    if not rows:
        raise ValueError("Cannot write an empty Parquet batch")

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    table = pa.Table.from_pylist(
        rows,
        schema=RETAIL_SCHEMA,
    )

    if table.num_rows != len(rows):
        raise ValueError("Arrow row count mismatch")

    temporary_path = None

    try:
        with tempfile.NamedTemporaryFile(
            prefix=".parquet_",
            suffix=".tmp",
            dir=destination.parent,
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)

        pq.write_table(
            table,
            temporary_path,
            compression="zstd",
        )

        metadata = pq.read_metadata(temporary_path)

        if metadata.num_rows != len(rows):
            raise ValueError("Written Parquet row count mismatch")

        written_schema = pq.read_schema(temporary_path)

        if not written_schema.equals(RETAIL_SCHEMA):
            raise ValueError("Written Parquet schema mismatch")

        os.replace(
            temporary_path,
            destination,
        )

    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    return {
        "path": str(destination),
        "row_count": len(rows),
        "size_bytes": destination.stat().st_size,
        "compression": "zstd",
    }
