"""Integration tests for the versioned Gold pipeline."""

import json
from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from commercelake.gold.gold_pipeline import build_gold
from commercelake.transformation.silver_schema import SILVER_SCHEMA


def write_json(path, payload):
    """Write a small test manifest or active pointer."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )


def make_row(
    invoice,
    kind,
    quantity,
    amount,
    customer,
    source_row,
):
    """Build a realistic Silver record."""
    return {
        "invoice": invoice,
        "stock_code": "SKU001",
        "description": "Test product",
        "quantity": quantity,
        "invoice_date": datetime(2010, 1, 2, 10, 30),
        "unit_price": Decimal("10.0000"),
        "customer_id": customer,
        "country": "United Kingdom",
        "source_sheet": "Year 2009-2010",
        "source_row_number": source_row,
        "row_fingerprint": f"fingerprint-{source_row}",
        "transaction_type": kind,
        "gross_amount": Decimal(amount),
        "is_customer_missing": customer is None,
        "is_price_non_positive": False,
        "is_date_missing": False,
        "silver_processed_at": datetime(2026, 1, 1, tzinfo=UTC),
    }


def create_silver_source(root):
    """Create an isolated, versioned Silver source."""
    run_id = "test-silver-run"
    run_directory = root / "runs" / run_id
    relative_path = "year=2010/month=01/part-00000.parquet"
    parquet_path = run_directory / relative_path

    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    rows = [
        make_row("INV001", "sale", 2, "20.0000", "12345", 1),
        make_row("CINV001", "cancellation", -1, "-10.0000", "12345", 2),
        make_row("INV002", "review", 1, "0.0000", None, 3),
    ]

    table = pa.Table.from_pylist(rows, schema=SILVER_SCHEMA)
    pq.write_table(table, parquet_path)

    manifest = {
        "run_id": run_id,
        "layer": "silver",
        "complete_source": True,
        "total_rows": len(rows),
        "files": [
            {
                "path": relative_path,
                "rows": len(rows),
                "bytes": parquet_path.stat().st_size,
            }
        ],
        "partitions": {"year=2010/month=01": len(rows)},
    }

    write_json(run_directory / "manifest.json", manifest)
    write_json(
        root / "current.json",
        {"run_id": run_id, "layer": "silver"},
    )

    return run_directory, parquet_path


def test_gold_build_publishes_complete_version(tmp_path):
    """A valid source produces a complete, published Gold run."""
    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    create_silver_source(silver_root)

    result = build_gold(
        silver_root=silver_root,
        gold_root=gold_root,
        batch_size=2,
    )

    pointer = json.loads((gold_root / "current.json").read_text(encoding="utf-8"))

    assert pointer["run_id"] == result["run_id"]
    assert result["source_silver_run_id"] == "test-silver-run"
    assert result["fact_sales"]["total_rows"] == 3
    assert result["fact_sales"]["net_transaction_amount"] == "10.0000"

    assert result["fact_sales"]["transaction_counts"] == {
        "cancellation": 1,
        "review": 1,
        "sale": 1,
    }

    assert result["dimensions"]["dim_product"]["rows"] == 2
    assert result["dimensions"]["dim_customer"]["rows"] == 2
    assert result["dimensions"]["dim_country"]["rows"] == 2
    assert result["dimensions"]["dim_date"]["rows"] == 1

    run_directory = gold_root / "runs" / result["run_id"]

    for item in result["fact_sales"]["files"]:
        assert (run_directory / item["path"]).is_file()

    for item in result["dimensions"].values():
        assert (run_directory / item["path"]).is_file()

    assert (run_directory / "manifest.json").is_file()


def test_gold_fact_preserves_lineage_and_decimal_amounts(tmp_path):
    """The persisted fact retains source lineage and exact amounts."""
    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    create_silver_source(silver_root)

    result = build_gold(
        silver_root=silver_root,
        gold_root=gold_root,
        batch_size=2,
    )

    run_directory = gold_root / "runs" / result["run_id"]

    rows = []

    for item in result["fact_sales"]["files"]:
        rows.extend(pq.read_table(run_directory / item["path"]).to_pylist())

    assert len(rows) == 3

    rows.sort(key=lambda row: row["source_row_number"])

    assert [row["row_fingerprint"] for row in rows] == [
        "fingerprint-1",
        "fingerprint-2",
        "fingerprint-3",
    ]

    assert [row["net_transaction_amount"] for row in rows] == [
        Decimal("20.0000"),
        Decimal("-10.0000"),
        None,
    ]

    assert rows[2]["customer_key"] == "CUSTOMER:UNKNOWN"


def test_gold_second_build_creates_independent_version(tmp_path):
    """Rebuilding produces an independent version with equal metrics."""
    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    create_silver_source(silver_root)

    first = build_gold(silver_root, gold_root, batch_size=2)
    second = build_gold(silver_root, gold_root, batch_size=2)

    assert first["run_id"] != second["run_id"]
    assert first["fact_sales"] == second["fact_sales"]

    assert first["dimensions"] == second["dimensions"]

    assert (gold_root / "runs" / first["run_id"] / "manifest.json").is_file()

    pointer = json.loads((gold_root / "current.json").read_text(encoding="utf-8"))

    assert pointer["run_id"] == second["run_id"]


def test_invalid_silver_inventory_does_not_publish(tmp_path):
    """A corrupted source manifest cannot become active Gold."""
    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    source_directory, _ = create_silver_source(silver_root)

    manifest_path = source_directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    manifest["total_rows"] = 999
    write_json(manifest_path, manifest)

    with pytest.raises(ValueError, match="inventory total"):
        build_gold(silver_root, gold_root, batch_size=2)

    assert not (gold_root / "current.json").exists()
    assert not (gold_root / "runs").exists()


def test_failed_build_preserves_previous_active_run(tmp_path):
    """A failed rebuild cannot replace a valid active pointer."""
    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    source_directory, _ = create_silver_source(silver_root)

    first = build_gold(silver_root, gold_root, batch_size=2)

    manifest_path = source_directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["partitions"]["year=2010/month=01"] = 999
    write_json(manifest_path, manifest)

    with pytest.raises(ValueError, match="partition totals"):
        build_gold(silver_root, gold_root, batch_size=2)

    pointer = json.loads((gold_root / "current.json").read_text(encoding="utf-8"))

    assert pointer["run_id"] == first["run_id"]
    assert (gold_root / "runs" / first["run_id"] / "manifest.json").is_file()


def test_gold_rejects_unsafe_manifest_path(tmp_path):
    """Source paths cannot escape the Silver run directory."""
    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    source_directory, _ = create_silver_source(silver_root)

    manifest_path = source_directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    manifest["files"][0]["path"] = "../outside.parquet"
    write_json(manifest_path, manifest)

    with pytest.raises(ValueError, match="Unsafe Silver file path"):
        build_gold(silver_root, gold_root, batch_size=2)

    assert not (gold_root / "current.json").exists()


def test_gold_rejects_invalid_batch_size(tmp_path):
    """Batch size must be positive."""
    with pytest.raises(ValueError, match="positive"):
        build_gold(
            silver_root=tmp_path / "silver",
            gold_root=tmp_path / "gold",
            batch_size=0,
        )
