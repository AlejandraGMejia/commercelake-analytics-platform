"""Tests for retail ingestion schema and normalization."""

from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from commercelake.ingestion.schema import (
    RETAIL_SCHEMA,
    normalize_integer,
    normalize_price,
    normalize_retail_row,
    rows_to_table,
)


@pytest.fixture
def sample_row():
    return (
        "536365",
        "85123A",
        "WHITE HANGING HEART",
        6,
        datetime(2010, 12, 1, 8, 26),
        2.55,
        17850.0,
        "United Kingdom",
    )


def test_schema_has_expected_columns():
    assert len(RETAIL_SCHEMA) == 11


def test_schema_uses_decimal_price():
    assert RETAIL_SCHEMA.field("unit_price").type == (pa.decimal128(18, 4))


def test_schema_uses_timestamp():
    assert RETAIL_SCHEMA.field("invoice_date").type == (pa.timestamp("us"))


def test_normalize_retail_row(sample_row):
    result = normalize_retail_row(
        sample_row,
        "Year 2010-2011",
        2,
    )

    assert result["invoice"] == "536365"
    assert result["quantity"] == 6
    assert result["unit_price"] == Decimal("2.5500")
    assert result["customer_id"] == "17850.0"
    assert result["source_row_number"] == 2


def test_fingerprint_present(sample_row):
    result = normalize_retail_row(
        sample_row,
        "Year 2010-2011",
        2,
    )

    assert len(result["row_fingerprint"]) == 64


def test_normalize_integer_rejects_fraction():
    with pytest.raises(ValueError):
        normalize_integer(2.5)


def test_normalize_price_rejects_extra_precision():
    with pytest.raises(ValueError):
        normalize_price("2.12345")


def test_normalize_price_preserves_precision():
    assert normalize_price("2.5500") == Decimal("2.5500")


def test_rows_to_arrow_table(sample_row):
    record = normalize_retail_row(
        sample_row,
        "Year 2010-2011",
        2,
    )

    table = rows_to_table([record])

    assert table.num_rows == 1
    assert table.schema == RETAIL_SCHEMA


def test_invalid_column_count():
    with pytest.raises(ValueError):
        normalize_retail_row(
            ("123", "ABC"),
            "Year 2010-2011",
            2,
        )
