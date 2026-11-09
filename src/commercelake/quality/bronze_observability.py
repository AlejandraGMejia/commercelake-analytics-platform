"""Business-level data quality observability for Bronze Parquet."""

import json
import sqlite3
import tempfile
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq

BATCH_SIZE = 10000

METRIC_RULES = {
    "BRZ101": "missing_customer_id",
    "BRZ102": "negative_quantity",
    "BRZ103": "cancelled_invoice",
    "BRZ104": "non_positive_price",
    "BRZ105": "repeated_fingerprint",
}

OBSERVATION_COLUMNS = [
    "invoice",
    "quantity",
    "unit_price",
    "customer_id",
    "row_fingerprint",
]


def load_active_run(output):
    """Resolve and validate the active Bronze run."""
    output = Path(output).resolve()

    pointer = json.loads((output / "current.json").read_text(encoding="utf-8"))

    if not isinstance(pointer, dict):
        raise TypeError("Invalid active run pointer")

    run_id = pointer.get("run_id")

    if (
        not isinstance(run_id, str)
        or not run_id
        or "/" in run_id
        or "\\" in run_id
        or run_id in {".", ".."}
    ):
        raise ValueError("Invalid active run ID")

    expected = f"runs/{run_id}/manifest.json"

    if pointer.get("manifest") != expected:
        raise ValueError("Active manifest path mismatch")

    run_dir = (output / "runs" / run_id).resolve()

    if not run_dir.is_relative_to(output):
        raise ValueError("Unsafe run directory")

    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))

    if not isinstance(manifest, dict):
        raise TypeError("Invalid Bronze manifest")

    if manifest.get("run_id") != run_id:
        raise ValueError("Manifest run ID mismatch")

    if manifest.get("complete_source") is not True:
        raise ValueError("Active Bronze run is incomplete")

    return run_id, run_dir, manifest


def validate_inventory(run_dir, manifest):
    """Validate file inventory before reading Parquet data."""
    files = manifest.get("files")

    if not isinstance(files, list) or not files:
        raise ValueError("Invalid or empty file inventory")

    expected_total = manifest.get("total_rows")

    if (
        isinstance(expected_total, bool)
        or not isinstance(expected_total, int)
        or expected_total < 0
    ):
        raise ValueError("Invalid manifest total row count")

    seen_paths = set()
    validated_files = []

    for item in files:
        if not isinstance(item, dict):
            raise TypeError("Invalid file inventory entry")

        relative = item.get("path")
        expected_rows = item.get("rows")

        if not isinstance(relative, str) or not relative:
            raise ValueError("Invalid Parquet file path")

        if (
            isinstance(expected_rows, bool)
            or not isinstance(expected_rows, int)
            or expected_rows < 0
        ):
            raise ValueError(f"Invalid row count for file: {relative}")

        path = (run_dir / relative).resolve()

        if not path.is_relative_to(run_dir):
            raise ValueError(f"Unsafe Parquet path: {relative}")

        if path in seen_paths:
            raise ValueError(f"Duplicate file inventory entry: {relative}")

        seen_paths.add(path)

        if not path.is_file():
            raise FileNotFoundError(path)

        validated_files.append((path, expected_rows))

    return validated_files


def percentage(count, total):
    """Calculate a percentage rounded to four decimals."""
    if total == 0:
        return 0.0

    return round(count * 100 / total, 4)


def count_business_anomalies(data, metrics):
    """Count commercial anomalies in a single Arrow batch."""
    rows = len(data["invoice"])
    fingerprints = []

    for index in range(rows):
        invoice = data["invoice"][index]
        quantity = data["quantity"][index]
        price = data["unit_price"][index]
        customer = data["customer_id"][index]
        fingerprint = data["row_fingerprint"][index]

        if customer is None or not str(customer).strip():
            metrics["missing_customer_id"] += 1

        if quantity is not None and quantity < 0:
            metrics["negative_quantity"] += 1

        if invoice is not None and str(invoice).upper().startswith("C"):
            metrics["cancelled_invoice"] += 1

        if price is not None and price <= 0:
            metrics["non_positive_price"] += 1

        if fingerprint:
            fingerprints.append((fingerprint,))

    return fingerprints


def observe_bronze(output="data/processed/retail"):
    """Calculate Bronze quality metrics with bounded memory."""
    run_id, run_dir, manifest = load_active_run(output)

    files = validate_inventory(run_dir, manifest)

    metrics = Counter(
        {
            "total_rows": 0,
            **{name: 0 for name in METRIC_RULES.values()},
        }
    )

    with tempfile.TemporaryDirectory(prefix="commercelake_quality_") as temporary:
        database = Path(temporary) / "fingerprints.sqlite"

        with sqlite3.connect(database) as connection:
            connection.execute(
                """
                CREATE TABLE fingerprints (
                    fingerprint TEXT PRIMARY KEY
                )
                """
            )

            for path, expected_rows in files:
                parquet = pq.ParquetFile(path)
                file_rows = 0

                if parquet.metadata.num_rows != expected_rows:
                    raise ValueError(f"File row count mismatch: {path.name}")

                for batch in parquet.iter_batches(
                    batch_size=BATCH_SIZE,
                    columns=OBSERVATION_COLUMNS,
                ):
                    data = batch.to_pydict()
                    file_rows += batch.num_rows
                    metrics["total_rows"] += batch.num_rows

                    fingerprints = count_business_anomalies(
                        data,
                        metrics,
                    )

                    before = connection.total_changes

                    connection.executemany(
                        """
                        INSERT OR IGNORE INTO fingerprints
                        (fingerprint) VALUES (?)
                        """,
                        fingerprints,
                    )

                    inserted = connection.total_changes - before

                    metrics["repeated_fingerprint"] += len(fingerprints) - inserted

                if file_rows != expected_rows:
                    raise ValueError(f"Physical row count mismatch: {path.name}")

            connection.commit()

    total = metrics["total_rows"]

    if total != manifest["total_rows"]:
        raise ValueError("Observed row count does not match the manifest")

    observations = {}

    for rule_id, name in METRIC_RULES.items():
        count = metrics[name]

        observations[rule_id] = {
            "metric": name,
            "count": count,
            "percentage": percentage(count, total),
            "severity": ("warning" if rule_id in {"BRZ104", "BRZ105"} else "info"),
        }

    return {
        "run_id": run_id,
        "layer": "bronze",
        "total_rows": total,
        "observations": observations,
        "preservation": {
            "source_rows_modified": False,
            "source_rows_deleted": False,
        },
    }
