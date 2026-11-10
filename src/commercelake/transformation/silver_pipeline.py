"""Versioned Bronze-to-Silver Parquet transformation pipeline."""

import json
import os
import shutil
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq

from commercelake.ingestion.schema import RETAIL_SCHEMA
from commercelake.transformation.silver_schema import SILVER_SCHEMA
from commercelake.transformation.silver_transformer import transform_retail_batch


def read_json(path):
    """Read a JSON object from disk."""
    with path.open(encoding="utf-8") as stream:
        document = json.load(stream)

    if not isinstance(document, dict):
        raise TypeError(f"Expected a JSON object: {path}")

    return document


def safe_relative_path(root, relative):
    """Resolve a path without allowing traversal outside its root."""
    if not isinstance(relative, str) or not relative:
        raise ValueError("Invalid relative path")

    candidate = Path(relative)

    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError(f"Unsafe relative path: {relative}")

    resolved = (root / candidate).resolve()

    if not resolved.is_relative_to(root.resolve()):
        raise ValueError(f"Path escapes its root: {relative}")

    return resolved


def load_active_bronze(bronze_root):
    """Resolve the active Bronze manifest and validate its inventory."""
    bronze_root = Path(bronze_root).resolve()
    pointer = read_json(bronze_root / "current.json")

    run_id = pointer.get("run_id")

    if (
        not isinstance(run_id, str)
        or not run_id
        or run_id in {".", ".."}
        or "/" in run_id
        or "\\" in run_id
    ):
        raise ValueError("Invalid Bronze run ID")

    expected_manifest = f"runs/{run_id}/manifest.json"

    if pointer.get("manifest") != expected_manifest:
        raise ValueError("Bronze manifest reference mismatch")

    run_root = safe_relative_path(bronze_root, f"runs/{run_id}")
    manifest = read_json(run_root / "manifest.json")

    if manifest.get("run_id") != run_id:
        raise ValueError("Bronze run ID mismatch")

    if manifest.get("complete_source") is not True:
        raise ValueError("Bronze source is incomplete")

    files = manifest.get("files")

    if not isinstance(files, list) or not files:
        raise ValueError("Bronze file inventory is missing")

    expected_total = manifest.get("total_rows")

    if type(expected_total) is not int or expected_total < 1:
        raise ValueError("Invalid Bronze total row count")

    seen = set()
    inventory_total = 0

    for item in files:
        if not isinstance(item, dict):
            raise TypeError("Invalid Bronze inventory entry")

        relative = item.get("path")
        path = safe_relative_path(run_root, relative)

        if path.suffix != ".parquet":
            raise ValueError(f"Unexpected file type: {relative}")

        if relative in seen:
            raise ValueError(f"Duplicate Bronze inventory entry: {relative}")

        seen.add(relative)

        if not path.is_file():
            raise FileNotFoundError(path)

        expected_rows = item.get("rows")

        if type(expected_rows) is not int or expected_rows < 1:
            raise ValueError(f"Invalid row count: {relative}")

        inventory_total += expected_rows

    if inventory_total != expected_total:
        raise ValueError("Bronze inventory total does not match manifest")

    return run_id, run_root, manifest


def publish_pointer(root, run_id):
    """Atomically publish a completed Silver run."""
    pointer = {
        "run_id": run_id,
        "manifest": f"runs/{run_id}/manifest.json",
    }

    temporary = root / f".current-{run_id}.tmp"

    try:
        temporary.write_text(
            json.dumps(pointer, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, root / "current.json")
    finally:
        temporary.unlink(missing_ok=True)


def build_silver(
    bronze_root="data/processed/retail",
    silver_root="data/processed/retail_silver",
    batch_size=10000,
):
    """Create and publish a fully reconciled Silver dataset."""
    if type(batch_size) is not int or batch_size < 1:
        raise ValueError("batch_size must be a positive integer")

    bronze_root = Path(bronze_root).resolve()
    silver_root = Path(silver_root).resolve()

    if (
        silver_root == bronze_root
        or silver_root.is_relative_to(bronze_root)
        or bronze_root.is_relative_to(silver_root)
    ):
        raise ValueError("Bronze and Silver storage must be separate")

    bronze_run_id, bronze_run_root, bronze_manifest = load_active_bronze(bronze_root)

    run_id = uuid4().hex
    run_root = silver_root / "runs" / run_id

    if run_root.exists():
        raise FileExistsError(run_root)

    run_root.mkdir(parents=True)

    processed_at = datetime.now(UTC)
    inventory = []
    partitions = Counter()
    total_rows = 0

    try:
        for item in bronze_manifest["files"]:
            relative = item["path"]
            source_path = safe_relative_path(bronze_run_root, relative)
            target_path = safe_relative_path(run_root, relative)

            source = pq.ParquetFile(source_path)

            if not source.schema_arrow.equals(
                RETAIL_SCHEMA,
                check_metadata=False,
            ):
                raise ValueError(f"Bronze schema mismatch: {relative}")

            if source.metadata.num_rows != item["rows"]:
                raise ValueError(f"Bronze row count mismatch: {relative}")

            target_path.parent.mkdir(parents=True, exist_ok=True)

            writer = None
            file_rows = 0

            try:
                for batch in source.iter_batches(batch_size=batch_size):
                    silver_batch = transform_retail_batch(
                        batch,
                        processed_at=processed_at,
                    )

                    if not silver_batch.schema.equals(
                        SILVER_SCHEMA,
                        check_metadata=False,
                    ):
                        raise ValueError("Unexpected Silver schema")

                    if writer is None:
                        writer = pq.ParquetWriter(
                            target_path,
                            SILVER_SCHEMA,
                            compression="snappy",
                        )

                    writer.write_table(silver_batch)
                    file_rows += silver_batch.num_rows
            finally:
                if writer is not None:
                    writer.close()

            if file_rows != item["rows"]:
                raise ValueError(f"Silver row count mismatch: {relative}")

            written = pq.ParquetFile(target_path)

            if not written.schema_arrow.equals(
                SILVER_SCHEMA,
                check_metadata=False,
            ):
                raise ValueError(f"Written Silver schema mismatch: {relative}")

            if written.metadata.num_rows != file_rows:
                raise ValueError(f"Written Silver count mismatch: {relative}")

            partition = "/".join(Path(relative).parts[:-1])
            partitions[partition] += file_rows
            total_rows += file_rows

            inventory.append(
                {
                    "path": relative,
                    "rows": file_rows,
                    "bytes": target_path.stat().st_size,
                }
            )

        if total_rows != bronze_manifest["total_rows"]:
            raise ValueError("Bronze-to-Silver total reconciliation failed")

        if dict(partitions) != bronze_manifest.get("partitions"):
            raise ValueError("Bronze-to-Silver partition reconciliation failed")

        manifest = {
            "run_id": run_id,
            "layer": "silver",
            "source_bronze_run_id": bronze_run_id,
            "processed_at": processed_at.isoformat(),
            "complete_source": True,
            "total_rows": total_rows,
            "files": inventory,
            "partitions": dict(partitions),
        }

        manifest_path = run_root / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        publish_pointer(silver_root, run_id)

        return manifest

    except Exception:
        shutil.rmtree(run_root)
        raise
