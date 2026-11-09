"""Incremental, versioned ingestion of Online Retail II."""

import hashlib
import json
import os
import shutil
import tempfile
import uuid
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

from openpyxl import load_workbook

from commercelake.ingestion.parquet_writer import write_parquet
from commercelake.ingestion.schema import (
    RETAIL_SCHEMA,
    normalize_retail_row,
)

SOURCE_COLUMNS = (
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer ID",
    "Country",
)

DEFAULT_SOURCE = Path("data/raw/online_retail_ii/online_retail_II.xlsx")
DEFAULT_OUTPUT = Path("data/processed/retail")


def file_sha256(path):
    """Compute a source checksum using bounded memory."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def partition_name(record):
    """Return a deterministic partition from invoice date."""
    invoice_date = record["invoice_date"]
    if invoice_date is None:
        return "year=unknown/month=unknown"
    return f"year={invoice_date.year:04d}/month={invoice_date.month:02d}"


def ingest(
    source=DEFAULT_SOURCE, output=DEFAULT_OUTPUT, batch_size=10000, max_rows=None
):
    """Create a complete immutable Parquet ingestion version."""
    source = Path(source).resolve()
    output = Path(output).resolve()

    if not source.is_file():
        raise FileNotFoundError(source)
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if max_rows is not None and max_rows < 1:
        raise ValueError("max_rows must be positive")

    output.mkdir(parents=True, exist_ok=True)
    runs = output / "runs"
    runs.mkdir(exist_ok=True)

    run_id = uuid.uuid4().hex
    staging = Path(tempfile.mkdtemp(prefix=".staging_", dir=output))
    destination = runs / run_id

    counts = Counter()
    parts = Counter()
    files = []
    buffers = defaultdict(list)
    workbook = None

    def flush():
        """Write pending rows without retaining old batches."""
        for partition, records in list(buffers.items()):
            if not records:
                continue

            index = parts[partition]
            relative = f"{partition}/part-{index:05d}.parquet"
            result = write_parquet(records, staging / relative)
            files.append(
                {
                    "path": relative,
                    "rows": result["row_count"],
                    "bytes": result["size_bytes"],
                }
            )
            parts[partition] += 1
            records.clear()

    try:
        workbook = load_workbook(source, read_only=True, data_only=True)

        stop = False
        for sheet in workbook.worksheets:
            iterator = sheet.iter_rows(values_only=True)
            headers = tuple(next(iterator))

            if headers != SOURCE_COLUMNS:
                raise ValueError(
                    f"Unexpected source columns in {sheet.title}: {headers}"
                )

            print(f"Reading {sheet.title}", flush=True)

            for excel_row, row in enumerate(iterator, start=2):
                record = normalize_retail_row(row, sheet.title, excel_row)
                partition = partition_name(record)
                buffers[partition].append(record)

                counts["total_rows"] += 1
                counts[f"sheet:{sheet.title}"] += 1
                counts[f"partition:{partition}"] += 1

                if counts["total_rows"] % batch_size == 0:
                    flush()
                    print(
                        f"Processed {counts['total_rows']:,} rows",
                        flush=True,
                    )

                if max_rows is not None and counts["total_rows"] >= max_rows:
                    stop = True
                    break

            if stop:
                break

        flush()

        if counts["total_rows"] == 0:
            raise ValueError("Source contains no data rows")

        if sum(item["rows"] for item in files) != counts["total_rows"]:
            raise ValueError("Manifest row reconciliation failed")

        manifest = {
            "run_id": run_id,
            "created_at_utc": datetime.now(UTC).isoformat(),
            "source": str(source),
            "source_sha256": file_sha256(source),
            "source_sheets": {
                key.removeprefix("sheet:"): value
                for key, value in counts.items()
                if key.startswith("sheet:")
            },
            "total_rows": counts["total_rows"],
            "partitions": {
                key.removeprefix("partition:"): value
                for key, value in counts.items()
                if key.startswith("partition:")
            },
            "schema": str(RETAIL_SCHEMA),
            "batch_size": batch_size,
            "max_rows": max_rows,
            "complete_source": max_rows is None,
            "files": files,
        }

        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n",
            encoding="utf-8",
        )

        # A run becomes immutable after this rename.
        staging.rename(destination)

        # Partial runs are retained but never activated.
        if max_rows is None:
            pointer = output / "current.json"
            pointer_temp = output / f".current_{run_id}.tmp"
            try:
                pointer_temp.write_text(
                    json.dumps(
                        {
                            "run_id": run_id,
                            "manifest": (f"runs/{run_id}/manifest.json"),
                        },
                        indent=2,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                os.replace(pointer_temp, pointer)
            finally:
                pointer_temp.unlink(missing_ok=True)

        return manifest

    finally:
        if workbook is not None:
            workbook.close()
        if staging.exists():
            shutil.rmtree(staging)
