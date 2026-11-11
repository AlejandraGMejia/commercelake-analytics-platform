"""Tests for local BigQuery CSV serialization."""

import csv
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from commercelake.cloud.bigquery_csv_adapter import (
    export_fact_sales_csv,
    serialize_bigquery_csv_value,
)
from commercelake.warehouse.loader import TABLE_SCHEMAS


def test_datetime_preserves_microseconds():
    value = datetime(2009, 12, 1, 8, 15, 30, 123456)

    assert (
        serialize_bigquery_csv_value(value, "DATETIME") == "2009-12-01 08:15:30.123456"
    )


def test_datetime_rejects_timezone():
    with pytest.raises(ValueError, match="timezone-naive"):
        serialize_bigquery_csv_value(
            datetime(2009, 12, 1, tzinfo=UTC),
            "DATETIME",
        )


def test_timestamp_normalizes_to_utc():
    value = datetime(
        2010,
        6,
        15,
        14,
        45,
        tzinfo=timezone(timedelta(hours=-6)),
    )

    assert (
        serialize_bigquery_csv_value(value, "TIMESTAMP")
        == "2010-06-15 20:45:00.000000+00:00"
    )


def test_timestamp_rejects_naive_datetime():
    with pytest.raises(ValueError, match="timezone-aware"):
        serialize_bigquery_csv_value(
            datetime(2010, 6, 15),
            "TIMESTAMP",
        )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("6.9500"), "6.9500"),
        (Decimal("83.4000"), "83.4000"),
        (Decimal("-12.3400"), "-12.3400"),
        (True, "true"),
        (False, "false"),
        (None, ""),
    ],
)
def test_scalar_serialization(value, expected):
    assert serialize_bigquery_csv_value(value, "STRING") == expected


def test_empty_string_is_rejected():
    with pytest.raises(ValueError, match="Empty strings"):
        serialize_bigquery_csv_value("", "STRING")


def test_export_rejects_empty_inventory(tmp_path):
    with pytest.raises(ValueError, match="At least one"):
        export_fact_sales_csv([], tmp_path / "fact_sales.csv")


def test_export_rejects_existing_destination(tmp_path):
    destination = tmp_path / "fact_sales.csv"
    destination.write_text("existing content", encoding="utf-8")

    with pytest.raises(FileExistsError):
        export_fact_sales_csv(
            [tmp_path / "missing.parquet"],
            destination,
        )

    assert destination.read_text(encoding="utf-8") == "existing content"


def test_export_preserves_rows_and_column_order(tmp_path):
    schema = TABLE_SCHEMAS["fact_sales"]

    values = {field.name: [None] for field in schema}

    values.update(
        {
            "invoice": ["INV-001"],
            "stock_code": ["SKU-001"],
            "description": ["Test product"],
            "quantity": [2],
            "invoice_date": [datetime(2009, 12, 1, 8, 15)],
            "unit_price": [Decimal("6.9500")],
            "customer_id": ["12345"],
            "country": ["United Kingdom"],
            "source_sheet": ["2009-2010"],
            "source_row_number": [1],
            "row_fingerprint": ["test-fingerprint"],
            "transaction_type": ["sale"],
            "gross_amount": [Decimal("13.9000")],
            "is_customer_missing": [False],
            "is_price_non_positive": [False],
            "is_date_missing": [False],
            "silver_processed_at": [datetime(2026, 1, 1, tzinfo=UTC)],
            "product_key": ["product-1"],
            "customer_key": ["customer-1"],
            "country_key": ["country-1"],
            "date_key": [20091201],
            "net_transaction_amount": [Decimal("13.9000")],
        }
    )

    table = pa.Table.from_pydict(values, schema=schema)

    source = tmp_path / "source.parquet"
    destination = tmp_path / "fact_sales.csv"

    pq.write_table(table, source)

    exported = export_fact_sales_csv(
        [source],
        destination,
        batch_size=1,
    )

    assert exported == 1

    with destination.open(newline="", encoding="utf-8") as file:
        rows = list(csv.reader(file))

    assert len(rows) == 1
    assert len(rows[0]) == len(schema.names)

    record = dict(zip(schema.names, rows[0], strict=True))

    assert record["invoice"] == "INV-001"
    assert record["invoice_date"] == "2009-12-01 08:15:00.000000"
    assert record["unit_price"] == "6.9500"
    assert record["gross_amount"] == "13.9000"
    assert record["net_transaction_amount"] == "13.9000"
    assert record["is_customer_missing"] == "false"
    assert record["silver_processed_at"].endswith("+00:00")
