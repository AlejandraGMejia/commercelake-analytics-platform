"""Validate versioned Bronze Parquet data against its quality contract."""

import json
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import yaml

from commercelake.ingestion.schema import RETAIL_SCHEMA


def load_contract(path):
    """Load and validate the mandatory Bronze quality contract."""
    with Path(path).open(encoding="utf-8") as stream:
        contract = yaml.safe_load(stream)

    if not isinstance(contract, dict):
        raise TypeError("Bronze contract must be a mapping")

    required = {
        "validation",
        "critical_rules",
        "observability_rules",
        "preservation_policy",
    }

    if not required.issubset(contract):
        raise ValueError("Bronze contract is incomplete")

    settings = contract["validation"]

    if not isinstance(settings, dict):
        raise TypeError("Validation settings must be a mapping")

    expected = settings.get("expected_total_rows")

    if isinstance(expected, bool) or not isinstance(expected, int):
        raise TypeError("Expected row count must be an integer")

    if expected < 1:
        raise ValueError("Expected row count must be positive")

    mandatory_settings = (
        "require_complete_source",
        "require_manifest",
        "require_all_files",
        "require_partition_reconciliation",
    )

    for setting in mandatory_settings:
        value = settings.get(setting)

        if value is not True:
            raise ValueError(f"{setting} must be explicitly enabled")

    return contract


def count_invalid_values(batch, column):
    """Count missing or invalid provenance values."""
    values = batch.column(column)
    invalid = pc.is_null(values)

    if column in {"source_sheet", "row_fingerprint"}:
        empty = pc.equal(values, "")
        invalid = pc.or_(invalid, pc.fill_null(empty, False))

    if column == "source_row_number":
        invalid = pc.or_(
            invalid,
            pc.fill_null(pc.less(values, 2), False),
        )

    return pc.sum(pc.cast(invalid, pa.int64())).as_py() or 0


def validate_bronze(
    output="data/processed/retail",
    contract_path="config/data_quality/bronze_contract.yaml",
):
    """Validate the active Bronze run using bounded-memory reads."""
    output = Path(output).resolve()
    contract = load_contract(contract_path)
    settings = contract["validation"]

    pointer_path = output / "current.json"

    if not pointer_path.is_file():
        raise FileNotFoundError(f"Active Bronze pointer not found: {pointer_path}")

    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))

    run_id = pointer["run_id"]

    if not isinstance(run_id, str):
        raise TypeError("Active run ID must be a string")

    if not run_id or "/" in run_id or "\\" in run_id or run_id in {".", ".."}:
        raise ValueError("Invalid active run ID")

    expected_manifest = f"runs/{run_id}/manifest.json"

    if pointer.get("manifest") != expected_manifest:
        raise ValueError("Active manifest path mismatch")

    run_dir = (output / "runs" / run_id).resolve()

    if not run_dir.is_relative_to(output):
        raise ValueError("Unsafe run directory")

    manifest_path = run_dir / "manifest.json"

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    failures = []
    metrics = Counter()
    partitions = Counter()
    seen_files = set()

    if manifest.get("run_id") != run_id:
        failures.append("BRZ003: Manifest run ID mismatch")

    if (
        settings.get("require_complete_source", True)
        and manifest.get("complete_source") is not True
    ):
        failures.append("BRZ003: Incomplete source")

    files = manifest.get("files", [])

    if not isinstance(files, list) or not files:
        failures.append("BRZ002: Missing file inventory")
        files = []

    for item in files:
        relative = item["path"]

        if relative in seen_files:
            failures.append(f"BRZ002: Duplicate file inventory entry: {relative}")
            continue

        seen_files.add(relative)

        path = (run_dir / relative).resolve()

        if not path.is_relative_to(run_dir):
            failures.append(f"BRZ002: Unsafe file path: {relative}")
            continue

        if not path.is_file():
            failures.append(f"BRZ002: Missing file: {relative}")
            continue

        try:
            parquet = pq.ParquetFile(path)
        except (OSError, pa.ArrowException) as exc:
            failures.append(f"BRZ002: Unreadable file: {relative}: {exc}")
            continue

        if not parquet.schema_arrow.equals(
            RETAIL_SCHEMA,
            check_metadata=False,
        ):
            failures.append(f"BRZ001: Schema mismatch: {relative}")
            continue

        file_rows = parquet.metadata.num_rows

        if file_rows != item["rows"]:
            failures.append(f"BRZ003: File row count mismatch: {relative}")

        partition = "/".join(Path(relative).parts[:-1])

        read_rows = 0

        try:
            for batch in parquet.iter_batches(
                batch_size=10000,
                columns=[
                    "source_sheet",
                    "source_row_number",
                    "row_fingerprint",
                ],
            ):
                read_rows += batch.num_rows

                for column in (
                    "source_sheet",
                    "source_row_number",
                    "row_fingerprint",
                ):
                    metrics[f"invalid_{column}"] += count_invalid_values(batch, column)

        except (OSError, pa.ArrowException) as exc:
            failures.append(f"BRZ002: Data read failure: {relative}: {exc}")
            continue

        if read_rows != file_rows:
            failures.append(f"BRZ003: Physical row count mismatch: {relative}")
            continue

        metrics["files_checked"] += 1
        metrics["rows_checked"] += read_rows
        partitions[partition] += read_rows

    if metrics["files_checked"] != len(files):
        failures.append("BRZ002: Not all declared files were validated")

    if metrics["rows_checked"] != settings["expected_total_rows"]:
        failures.append("BRZ003: Unexpected total row count")

    if metrics["rows_checked"] != manifest.get("total_rows"):
        failures.append("BRZ003: Manifest row count mismatch")

    if settings.get("require_partition_reconciliation", True) and dict(
        partitions
    ) != manifest.get("partitions"):
        failures.append("BRZ003: Partition reconciliation failed")

    if metrics["invalid_source_sheet"] or metrics["invalid_source_row_number"]:
        failures.append("BRZ004: Invalid source provenance")

    if metrics["invalid_row_fingerprint"]:
        failures.append("BRZ005: Missing source fingerprints")

    return {
        "status": "passed" if not failures else "failed",
        "run_id": run_id,
        "contract_version": contract.get("contract_version"),
        "metrics": dict(metrics),
        "failures": failures,
    }
