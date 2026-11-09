"""Integration tests for the retail ingestion pipeline."""

import json
from datetime import datetime

import pyarrow.parquet as pq
import pytest
from openpyxl import Workbook

from commercelake.ingestion.pipeline import ingest

HEADERS = [
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer ID",
    "Country",
]


def create_workbook(path, rows, headers=None):
    """Create a small Excel fixture for integration tests."""
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Year 2010-2011"
    sheet.append(headers if headers is not None else HEADERS)

    for row in rows:
        sheet.append(row)

    workbook.save(path)
    workbook.close()
    return path


def sample_rows():
    """Return representative retail records."""
    return [
        [
            "536365",
            "85123A",
            "PRODUCT A",
            6,
            datetime(2010, 12, 1, 8, 26),
            2.55,
            17850,
            "United Kingdom",
        ],
        [
            "536366",
            "71053",
            "PRODUCT B",
            3,
            datetime(2011, 1, 3, 9, 0),
            3.75,
            None,
            "United Kingdom",
        ],
        [
            "C536367",
            "85123A",
            "PRODUCT A",
            -1,
            datetime(2011, 1, 4, 10, 0),
            2.55,
            17850,
            "United Kingdom",
        ],
    ]


def active_manifest(output):
    """Read the manifest referenced by the active pointer."""
    pointer = json.loads((output / "current.json").read_text(encoding="utf-8"))

    manifest = json.loads((output / pointer["manifest"]).read_text(encoding="utf-8"))

    return pointer, manifest


def test_ingestion_creates_partitioned_parquet(tmp_path):
    source = create_workbook(
        tmp_path / "source.xlsx",
        sample_rows(),
    )
    output = tmp_path / "retail"

    manifest = ingest(
        source=source,
        output=output,
        batch_size=2,
    )

    assert manifest["total_rows"] == 3
    assert manifest["complete_source"] is True
    assert manifest["partitions"] == {
        "year=2010/month=12": 1,
        "year=2011/month=01": 2,
    }

    pointer, active = active_manifest(output)
    assert pointer["run_id"] == manifest["run_id"]
    assert active["total_rows"] == 3

    total = 0

    for item in manifest["files"]:
        path = output / "runs" / manifest["run_id"] / item["path"]

        assert path.exists()
        total += pq.read_metadata(path).num_rows

    assert total == 3


def test_repeated_ingestion_creates_new_version(tmp_path):
    source = create_workbook(
        tmp_path / "source.xlsx",
        sample_rows(),
    )
    output = tmp_path / "retail"

    first = ingest(source=source, output=output)
    second = ingest(source=source, output=output)

    assert first["run_id"] != second["run_id"]

    assert (output / "runs" / first["run_id"]).exists()

    assert (output / "runs" / second["run_id"]).exists()

    pointer, active = active_manifest(output)

    assert pointer["run_id"] == second["run_id"]
    assert active["total_rows"] == 3


def test_invalid_headers_do_not_publish(tmp_path):
    source = create_workbook(
        tmp_path / "invalid.xlsx",
        sample_rows(),
        headers=["Wrong"] * 8,
    )
    output = tmp_path / "retail"

    with pytest.raises(ValueError):
        ingest(source=source, output=output)

    assert not (output / "current.json").exists()
    assert not list(output.glob(".staging_*"))


def test_failed_run_preserves_previous_version(tmp_path):
    source = create_workbook(
        tmp_path / "source.xlsx",
        sample_rows(),
    )
    output = tmp_path / "retail"

    successful = ingest(source=source, output=output)

    create_workbook(
        source,
        sample_rows(),
        headers=["Wrong"] * 8,
    )

    with pytest.raises(ValueError):
        ingest(source=source, output=output)

    pointer, active = active_manifest(output)

    assert pointer["run_id"] == successful["run_id"]
    assert active["total_rows"] == 3


def test_partial_run_is_marked_in_manifest(tmp_path):
    source = create_workbook(
        tmp_path / "source.xlsx",
        sample_rows(),
    )

    manifest = ingest(
        source=source,
        output=tmp_path / "retail",
        batch_size=2,
        max_rows=2,
    )

    assert manifest["total_rows"] == 2
    assert manifest["complete_source"] is False
    assert manifest["max_rows"] == 2


def test_missing_source_is_rejected(tmp_path):
    with pytest.raises(FileNotFoundError):
        ingest(
            source=tmp_path / "missing.xlsx",
            output=tmp_path / "retail",
        )


def test_invalid_batch_size_is_rejected(tmp_path):
    source = create_workbook(
        tmp_path / "source.xlsx",
        sample_rows(),
    )

    with pytest.raises(ValueError):
        ingest(
            source=source,
            output=tmp_path / "retail",
            batch_size=0,
        )


def test_negative_quantity_is_preserved(tmp_path):
    source = create_workbook(
        tmp_path / "source.xlsx",
        sample_rows(),
    )

    manifest = ingest(
        source=source,
        output=tmp_path / "retail",
    )

    values = []

    for item in manifest["files"]:
        path = tmp_path / "retail" / "runs" / manifest["run_id"] / item["path"]
        table = pq.read_table(path)
        values.extend(table.column("quantity").to_pylist())

    assert sorted(values) == [-1, 3, 6]


def test_missing_customer_is_preserved(tmp_path):
    source = create_workbook(
        tmp_path / "source.xlsx",
        sample_rows(),
    )

    manifest = ingest(
        source=source,
        output=tmp_path / "retail",
    )

    customers = []

    for item in manifest["files"]:
        path = tmp_path / "retail" / "runs" / manifest["run_id"] / item["path"]
        table = pq.read_table(path)
        customers.extend(table.column("customer_id").to_pylist())

    assert customers.count(None) == 1


def test_partial_run_does_not_replace_complete_version(
    tmp_path,
):
    source = create_workbook(
        tmp_path / "source.xlsx",
        sample_rows(),
    )
    output = tmp_path / "retail"

    complete = ingest(
        source=source,
        output=output,
    )

    partial = ingest(
        source=source,
        output=output,
        max_rows=1,
    )

    pointer, active = active_manifest(output)

    assert pointer["run_id"] == complete["run_id"]
    assert active["total_rows"] == 3
    assert partial["total_rows"] == 1
    assert (output / "runs" / partial["run_id"] / "manifest.json").exists()


def test_partial_run_without_complete_version(
    tmp_path,
):
    source = create_workbook(
        tmp_path / "source.xlsx",
        sample_rows(),
    )
    output = tmp_path / "retail"

    partial = ingest(
        source=source,
        output=output,
        max_rows=1,
    )

    assert partial["complete_source"] is False
    assert not (output / "current.json").exists()
    assert (output / "runs" / partial["run_id"] / "manifest.json").exists()
