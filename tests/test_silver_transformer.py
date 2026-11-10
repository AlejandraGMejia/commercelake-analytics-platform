"""Tests for deterministic Silver retail transformations."""

from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from commercelake.ingestion.schema import RETAIL_SCHEMA
from commercelake.transformation.silver_schema import SILVER_SCHEMA
from commercelake.transformation.silver_transformer import (
    calculate_gross_amount,
    classify_transaction,
    transform_retail_batch,
)

PROCESSED_AT = datetime(2026, 1, 1, tzinfo=UTC)


def make_row(**overrides):
    """Build one valid Bronze record."""
    row = {
        "invoice": "10001",
        "stock_code": "SKU001",
        "description": "Test product",
        "quantity": 2,
        "invoice_date": datetime(2010, 1, 1, 12, 0),
        "unit_price": Decimal("10.2500"),
        "customer_id": "12345",
        "country": "United Kingdom",
        "source_sheet": "Year 2009-2010",
        "source_row_number": 2,
        "row_fingerprint": "test-fingerprint",
    }
    row.update(overrides)
    return row


def make_table(*rows):
    """Create a typed Bronze table."""
    return pa.Table.from_pylist(list(rows), schema=RETAIL_SCHEMA)


@pytest.mark.parametrize(
    ("invoice", "quantity", "price", "expected"),
    [
        ("10001", 2, Decimal("10.0000"), "sale"),
        ("C10001", -2, Decimal("10.0000"), "cancellation"),
        ("c10001", 2, Decimal("10.0000"), "cancellation"),
        ("10002", -1, Decimal("10.0000"), "return"),
        ("10003", 0, Decimal("10.0000"), "review"),
        ("10004", 1, Decimal("0.0000"), "review"),
        ("10005", 1, None, "review"),
        (None, 1, Decimal("10.0000"), "sale"),
        ("10006", None, Decimal("10.0000"), "review"),
    ],
)
def test_transaction_classification(invoice, quantity, price, expected):
    assert classify_transaction(invoice, quantity, price) == expected


def test_exact_decimal_calculation():
    amount = calculate_gross_amount(3, Decimal("0.1000"))
    assert amount == Decimal("0.3000")


def test_negative_amount_preserves_sign():
    amount = calculate_gross_amount(-2, Decimal("12.5000"))
    assert amount == Decimal("-25.0000")


def test_missing_amount_components():
    assert calculate_gross_amount(None, Decimal("10.0000")) is None
    assert calculate_gross_amount(2, None) is None


def test_silver_schema():
    assert SILVER_SCHEMA.field("gross_amount").type == pa.decimal128(38, 4)
    assert SILVER_SCHEMA.field("transaction_type").nullable is False


def test_sale_transformation():
    source = make_table(make_row())

    result = transform_retail_batch(source, PROCESSED_AT)
    record = result.to_pylist()[0]

    assert result.schema.equals(SILVER_SCHEMA)
    assert record["transaction_type"] == "sale"
    assert record["gross_amount"] == Decimal("20.5000")
    assert record["is_customer_missing"] is False
    assert record["is_price_non_positive"] is False
    assert record["is_date_missing"] is False
    assert record["silver_processed_at"] == PROCESSED_AT


def test_cancellation_priority():
    source = make_table(make_row(invoice="C10001", quantity=-3))

    result = transform_retail_batch(source, PROCESSED_AT)

    assert result.column("transaction_type")[0].as_py() == "cancellation"


def test_missing_customer_and_date():
    source = make_table(make_row(customer_id=None, invoice_date=None))

    result = transform_retail_batch(source, PROCESSED_AT)
    record = result.to_pylist()[0]

    assert record["is_customer_missing"] is True
    assert record["is_date_missing"] is True


def test_blank_customer_identifier():
    source = make_table(make_row(customer_id="   "))

    result = transform_retail_batch(source, PROCESSED_AT)

    assert result.column("is_customer_missing")[0].as_py() is True


def test_non_positive_price():
    source = make_table(make_row(unit_price=Decimal("0.0000")))

    result = transform_retail_batch(source, PROCESSED_AT)
    record = result.to_pylist()[0]

    assert record["is_price_non_positive"] is True
    assert record["transaction_type"] == "review"


def test_preserves_all_rows_and_source_columns():
    source = make_table(
        make_row(invoice="10001"),
        make_row(invoice="C10002", quantity=-2),
        make_row(invoice="10003", customer_id=None),
    )

    result = transform_retail_batch(source, PROCESSED_AT)

    assert result.num_rows == source.num_rows

    for field in RETAIL_SCHEMA:
        assert result.column(field.name).to_pylist() == (
            source.column(field.name).to_pylist()
        )


def test_accepts_record_batch():
    source = make_table(make_row())
    batch = source.to_batches()[0]

    result = transform_retail_batch(batch, PROCESSED_AT)

    assert result.num_rows == 1


def test_rejects_incorrect_schema():
    source = pa.table({"invoice": ["10001"]})

    with pytest.raises(ValueError, match="Unexpected Bronze schema"):
        transform_retail_batch(source, PROCESSED_AT)


def test_rejects_invalid_input_type():
    with pytest.raises(TypeError, match="Expected a PyArrow"):
        transform_retail_batch([make_row()], PROCESSED_AT)


def test_rejects_naive_processing_datetime():
    source = make_table(make_row())

    with pytest.raises(ValueError, match="timezone-aware"):
        transform_retail_batch(
            source,
            datetime(2026, 1, 1),
        )


def test_deterministic_transformation():
    source = make_table(make_row())

    first = transform_retail_batch(source, PROCESSED_AT)
    second = transform_retail_batch(source, PROCESSED_AT)

    assert first.equals(second)


def test_empty_batch():
    source = make_table()

    result = transform_retail_batch(source, PROCESSED_AT)

    assert result.num_rows == 0
    assert result.schema.equals(SILVER_SCHEMA)
