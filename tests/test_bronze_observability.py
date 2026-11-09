"""Tests for Bronze business quality observability."""

import json
from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from commercelake.ingestion.schema import RETAIL_SCHEMA
from commercelake.quality.bronze_observability import (
    observe_bronze,
    percentage,
)

RUN_ID = "test_observability"
PARTITION = "year=2020/month=01"


def make_record(
    fingerprint,
    *,
    invoice="12345",
    quantity=1,
    price="10.0000",
    customer="100",
):
    """Build one valid Bronze record."""
    return {
        "invoice": invoice,
        "stock_code": "ABC",
        "description": "Sample product",
        "quantity": quantity,
        "invoice_date": datetime(2020, 1, 1),
        "unit_price": Decimal(price),
        "customer_id": customer,
        "country": "United Kingdom",
        "source_sheet": "Year 2010-2011",
        "source_row_number": 2,
        "row_fingerprint": fingerprint,
    }


def make_dataset(tmp_path, records_by_file):
    """Create an isolated versioned Bronze run."""
    output = tmp_path / "retail"
    run_dir = output / "runs" / RUN_ID
    inventory = []
    total = 0

    for index, records in enumerate(records_by_file):
        relative = f"{PARTITION}/part-{index:05d}.parquet"
        path = run_dir / relative
        path.parent.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pylist(
            records,
            schema=RETAIL_SCHEMA,
        )
        pq.write_table(table, path)

        inventory.append({"path": relative, "rows": len(records)})
        total += len(records)

    manifest = {
        "run_id": RUN_ID,
        "complete_source": True,
        "total_rows": total,
        "partitions": {PARTITION: total},
        "files": inventory,
    }

    (run_dir / "manifest.json").write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    (output / "current.json").write_text(
        json.dumps(
            {
                "run_id": RUN_ID,
                "manifest": (f"runs/{RUN_ID}/manifest.json"),
            }
        ),
        encoding="utf-8",
    )

    return output


def test_all_business_quality_metrics(tmp_path):
    """Count anomalies and cross-file repetitions."""
    records = [
        make_record("fingerprint-a"),
        make_record(
            "fingerprint-b",
            invoice="C12345",
            quantity=-2,
            price="0",
            customer=None,
        ),
        make_record(
            "fingerprint-c",
            price="-1",
            customer="",
        ),
    ]

    repeated = [
        make_record("fingerprint-a"),
        make_record("fingerprint-a"),
    ]

    output = make_dataset(
        tmp_path,
        [records, repeated],
    )

    result = observe_bronze(output)
    observations = result["observations"]

    assert result["total_rows"] == 5
    assert observations["BRZ101"]["count"] == 2
    assert observations["BRZ102"]["count"] == 1
    assert observations["BRZ103"]["count"] == 1
    assert observations["BRZ104"]["count"] == 2
    assert observations["BRZ105"]["count"] == 2

    assert observations["BRZ101"]["percentage"] == 40.0
    assert observations["BRZ105"]["percentage"] == 40.0
    assert observations["BRZ104"]["severity"] == "warning"

    assert result["preservation"] == {
        "source_rows_modified": False,
        "source_rows_deleted": False,
    }


def test_clean_dataset(tmp_path):
    """A clean dataset produces zero anomaly counts."""
    output = make_dataset(
        tmp_path,
        [[make_record("unique-a")]],
    )

    result = observe_bronze(output)

    assert result["total_rows"] == 1
    assert all(
        observation["count"] == 0 for observation in result["observations"].values()
    )


def test_manifest_row_count_mismatch(tmp_path):
    """Reject a mismatch between observed and declared rows."""
    output = make_dataset(
        tmp_path,
        [[make_record("unique-a")]],
    )
    path = output / "runs" / RUN_ID / "manifest.json"

    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["total_rows"] = 99
    path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="row count"):
        observe_bronze(output)


def test_incomplete_run(tmp_path):
    """Reject incomplete source runs."""
    output = make_dataset(
        tmp_path,
        [[make_record("unique-a")]],
    )
    path = output / "runs" / RUN_ID / "manifest.json"

    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["complete_source"] = False
    path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="incomplete"):
        observe_bronze(output)


def test_missing_parquet_file(tmp_path):
    """Reject missing files in the manifest inventory."""
    output = make_dataset(
        tmp_path,
        [[make_record("unique-a")]],
    )
    path = output / "runs" / RUN_ID / PARTITION / "part-00000.parquet"
    path.unlink()

    with pytest.raises(FileNotFoundError):
        observe_bronze(output)


def test_percentage_zero_denominator():
    """Handle empty totals without division errors."""
    assert percentage(0, 0) == 0.0


def test_percentage_rounding():
    """Report percentages with four decimal places."""
    assert percentage(1, 3) == 33.3333
