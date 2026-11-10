"""Integration tests for the versioned Silver pipeline."""

import json
from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from commercelake.ingestion.schema import RETAIL_SCHEMA
from commercelake.transformation.silver_pipeline import (
    build_silver,
    load_active_bronze,
    safe_relative_path,
)

BRONZE_RUN_ID = "bronze-test-run"
FILE_PATH = "year=2010/month=01/part-00000.parquet"
PARTITION = "year=2010/month=01"


def write_json(path, document):
    """Write a JSON fixture."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(document, indent=2),
        encoding="utf-8",
    )


def make_bronze_dataset(tmp_path):
    """Create a complete, isolated Bronze dataset."""
    bronze_root = tmp_path / "bronze"
    run_root = bronze_root / "runs" / BRONZE_RUN_ID
    parquet_path = run_root / FILE_PATH

    parquet_path.parent.mkdir(parents=True)

    rows = [
        {
            "invoice": "10001",
            "stock_code": "SKU001",
            "description": "Product",
            "quantity": 2,
            "invoice_date": datetime(2010, 1, 1),
            "unit_price": Decimal("5.0000"),
            "customer_id": "12345",
            "country": "United Kingdom",
            "source_sheet": "Sheet1",
            "source_row_number": 2,
            "row_fingerprint": "fingerprint-1",
        },
        {
            "invoice": "C10002",
            "stock_code": "SKU002",
            "description": "Returned product",
            "quantity": -1,
            "invoice_date": datetime(2010, 1, 2),
            "unit_price": Decimal("7.5000"),
            "customer_id": None,
            "country": "United Kingdom",
            "source_sheet": "Sheet1",
            "source_row_number": 3,
            "row_fingerprint": "fingerprint-2",
        },
    ]

    table = pa.Table.from_pylist(rows, schema=RETAIL_SCHEMA)
    pq.write_table(table, parquet_path)

    manifest = {
        "run_id": BRONZE_RUN_ID,
        "complete_source": True,
        "total_rows": 2,
        "files": [
            {
                "path": FILE_PATH,
                "rows": 2,
                "bytes": parquet_path.stat().st_size,
            }
        ],
        "partitions": {PARTITION: 2},
    }

    write_json(run_root / "manifest.json", manifest)

    write_json(
        bronze_root / "current.json",
        {
            "run_id": BRONZE_RUN_ID,
            "manifest": f"runs/{BRONZE_RUN_ID}/manifest.json",
        },
    )

    return bronze_root


def test_build_silver_preserves_rows(tmp_path):
    bronze = make_bronze_dataset(tmp_path)
    silver = tmp_path / "silver"

    manifest = build_silver(
        bronze_root=bronze,
        silver_root=silver,
        batch_size=1,
    )

    assert manifest["total_rows"] == 2
    assert manifest["source_bronze_run_id"] == BRONZE_RUN_ID
    assert manifest["complete_source"] is True
    assert manifest["partitions"] == {PARTITION: 2}

    pointer = json.loads((silver / "current.json").read_text(encoding="utf-8"))

    assert pointer["run_id"] == manifest["run_id"]

    output_path = silver / "runs" / manifest["run_id"] / FILE_PATH

    result = pq.read_table(output_path)
    records = result.to_pylist()

    assert len(records) == 2
    assert records[0]["transaction_type"] == "sale"
    assert records[1]["transaction_type"] == "cancellation"
    assert records[0]["gross_amount"] == Decimal("10.0000")
    assert records[1]["gross_amount"] == Decimal("-7.5000")
    assert records[1]["is_customer_missing"] is True

    assert [row["row_fingerprint"] for row in records] == [
        "fingerprint-1",
        "fingerprint-2",
    ]


def test_rejects_unsafe_paths(tmp_path):
    root = tmp_path / "root"
    root.mkdir()

    with pytest.raises(ValueError, match="Unsafe relative path"):
        safe_relative_path(root, "../../outside.parquet")


def test_rejects_incomplete_bronze(tmp_path):
    bronze = make_bronze_dataset(tmp_path)

    manifest_path = bronze / "runs" / BRONZE_RUN_ID / "manifest.json"

    document = json.loads(manifest_path.read_text(encoding="utf-8"))
    document["complete_source"] = False
    write_json(manifest_path, document)

    with pytest.raises(ValueError, match="incomplete"):
        load_active_bronze(bronze)


def test_rejects_inventory_mismatch(tmp_path):
    bronze = make_bronze_dataset(tmp_path)

    manifest_path = bronze / "runs" / BRONZE_RUN_ID / "manifest.json"

    document = json.loads(manifest_path.read_text(encoding="utf-8"))
    document["total_rows"] = 3
    write_json(manifest_path, document)

    with pytest.raises(ValueError, match="inventory total"):
        load_active_bronze(bronze)


def test_failure_does_not_publish_silver(tmp_path):
    bronze = make_bronze_dataset(tmp_path)
    silver = tmp_path / "silver"

    parquet_path = bronze / "runs" / BRONZE_RUN_ID / FILE_PATH

    parquet_path.write_bytes(b"corrupted parquet data")

    with pytest.raises(pa.ArrowInvalid):
        build_silver(
            bronze_root=bronze,
            silver_root=silver,
        )

    assert not (silver / "current.json").exists()

    runs = silver / "runs"
    assert not runs.exists() or not list(runs.iterdir())


def test_rejects_overlapping_storage(tmp_path):
    bronze = make_bronze_dataset(tmp_path)

    with pytest.raises(ValueError, match="storage must be separate"):
        build_silver(
            bronze_root=bronze,
            silver_root=bronze,
        )


def test_rejects_invalid_batch_size(tmp_path):
    with pytest.raises(ValueError, match="batch_size"):
        build_silver(
            bronze_root=tmp_path / "bronze",
            silver_root=tmp_path / "silver",
            batch_size=0,
        )


def test_rejects_duplicate_inventory(tmp_path):
    bronze = make_bronze_dataset(tmp_path)

    manifest_path = bronze / "runs" / BRONZE_RUN_ID / "manifest.json"

    document = json.loads(manifest_path.read_text(encoding="utf-8"))
    document["files"].append(document["files"][0])
    document["total_rows"] = 4
    write_json(manifest_path, document)

    with pytest.raises(ValueError, match="Duplicate"):
        load_active_bronze(bronze)
