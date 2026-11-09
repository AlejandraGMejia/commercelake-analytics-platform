"""Tests for atomic Parquet writing."""

from datetime import datetime
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from commercelake.ingestion.parquet_writer import write_parquet


@pytest.fixture
def sample_records():
    return [
        {
            "invoice": "536365",
            "stock_code": "85123A",
            "description": "WHITE HANGING HEART",
            "quantity": 6,
            "invoice_date": datetime(2010, 12, 1, 8, 26),
            "unit_price": Decimal("2.5500"),
            "customer_id": "17850",
            "country": "United Kingdom",
            "source_sheet": "Year 2010-2011",
            "source_row_number": 2,
            "row_fingerprint": "a" * 64,
        },
        {
            "invoice": "C536366",
            "stock_code": "85123A",
            "description": "WHITE HANGING HEART",
            "quantity": -1,
            "invoice_date": datetime(2010, 12, 1, 9, 0),
            "unit_price": Decimal("2.5500"),
            "customer_id": None,
            "country": "United Kingdom",
            "source_sheet": "Year 2010-2011",
            "source_row_number": 3,
            "row_fingerprint": "b" * 64,
        },
    ]


def test_write_parquet_creates_file(
    tmp_path,
    sample_records,
):
    destination = tmp_path / "retail.parquet"

    result = write_parquet(
        sample_records,
        destination,
    )

    assert destination.exists()
    assert result["row_count"] == 2
    assert result["size_bytes"] > 0


def test_written_parquet_can_be_read(
    tmp_path,
    sample_records,
):
    destination = tmp_path / "retail.parquet"

    write_parquet(sample_records, destination)

    table = pq.read_table(destination)

    assert table.num_rows == 2
    assert table.column("quantity").to_pylist() == [
        6,
        -1,
    ]


def test_parquet_preserves_decimal(
    tmp_path,
    sample_records,
):
    destination = tmp_path / "retail.parquet"

    write_parquet(sample_records, destination)

    table = pq.read_table(destination)

    assert table.column("unit_price").to_pylist() == [
        Decimal("2.5500"),
        Decimal("2.5500"),
    ]


def test_parquet_preserves_null_customer(
    tmp_path,
    sample_records,
):
    destination = tmp_path / "retail.parquet"

    write_parquet(sample_records, destination)

    table = pq.read_table(destination)

    assert table.column("customer_id").to_pylist() == [
        "17850",
        None,
    ]


def test_empty_batch_rejected(tmp_path):
    with pytest.raises(ValueError):
        write_parquet(
            [],
            tmp_path / "empty.parquet",
        )


def test_existing_file_can_be_replaced(
    tmp_path,
    sample_records,
):
    destination = tmp_path / "retail.parquet"

    write_parquet(sample_records, destination)
    write_parquet(
        sample_records[:1],
        destination,
    )

    table = pq.read_table(destination)

    assert table.num_rows == 1


def test_temporary_file_is_removed(
    tmp_path,
    sample_records,
):
    destination = tmp_path / "retail.parquet"

    write_parquet(sample_records, destination)

    assert not list(tmp_path.glob(".parquet_*.tmp"))
