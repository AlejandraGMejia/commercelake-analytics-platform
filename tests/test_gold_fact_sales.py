"""Tests for Gold sales fact transformation."""

from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from commercelake.gold.dimensions import DimensionCollector
from commercelake.gold.fact_sales import (
    FACT_SALES_SCHEMA,
    date_key,
    net_transaction_amount,
    transform_fact_batch,
    validate_fact_dimensions,
)
from commercelake.transformation.silver_schema import SILVER_SCHEMA


def make_row(
    kind="sale",
    amount="25.0000",
    customer="12345",
    invoice_date=None,
):
    """Create one representative Silver transaction."""
    return {
        "invoice": "INV001",
        "stock_code": "SKU001",
        "description": "Test product",
        "quantity": 2,
        "invoice_date": (
            datetime(2010, 1, 2, 10, 30) if invoice_date is None else invoice_date
        ),
        "unit_price": Decimal("12.5000"),
        "customer_id": customer,
        "country": "United Kingdom",
        "source_sheet": "Year 2009-2010",
        "source_row_number": 42,
        "row_fingerprint": "test-fingerprint",
        "transaction_type": kind,
        "gross_amount": (None if amount is None else Decimal(amount)),
        "is_customer_missing": customer is None,
        "is_price_non_positive": False,
        "is_date_missing": False,
        "silver_processed_at": datetime(2026, 1, 1, tzinfo=UTC),
    }


def make_batch(rows):
    """Build a Silver batch with its official schema."""
    table = pa.Table.from_pylist(rows, schema=SILVER_SCHEMA)
    return table.to_batches()[0]


def test_fact_preserves_schema_and_rows():
    batch = make_batch([make_row(), make_row()])
    fact = transform_fact_batch(batch)

    assert fact.schema == FACT_SALES_SCHEMA
    assert fact.num_rows == 2
    assert fact.column("source_row_number").to_pylist() == [42, 42]
    assert fact.column("row_fingerprint").to_pylist() == [
        "test-fingerprint",
        "test-fingerprint",
    ]


@pytest.mark.parametrize(
    ("kind", "amount", "expected"),
    [
        ("sale", "25.0000", Decimal("25.0000")),
        ("cancellation", "-25.0000", Decimal("-25.0000")),
        ("return", "-12.5000", Decimal("-12.5000")),
        ("review", "25.0000", None),
    ],
)
def test_transaction_financial_semantics(kind, amount, expected):
    batch = make_batch([make_row(kind=kind, amount=amount)])
    fact = transform_fact_batch(batch)

    assert fact.column("net_transaction_amount")[0].as_py() == expected


def test_missing_customer_uses_unknown_key():
    fact = transform_fact_batch(make_batch([make_row(customer=None)]))

    assert fact.column("customer_key")[0].as_py() == ("CUSTOMER:UNKNOWN")


def test_dimension_keys_and_date_are_correct():
    fact = transform_fact_batch(make_batch([make_row()]))

    assert fact.column("product_key")[0].as_py() == ("PRODUCT:VALUE:SKU001")
    assert fact.column("customer_key")[0].as_py() == ("CUSTOMER:VALUE:12345")
    assert fact.column("country_key")[0].as_py() == ("COUNTRY:VALUE:United Kingdom")
    assert fact.column("date_key")[0].as_py() == 20100102


def test_financial_amount_preserves_decimal_precision():
    amount = Decimal("123456789.1234")

    assert net_transaction_amount("sale", amount) == amount


def test_review_is_not_counted_as_revenue():
    assert net_transaction_amount("review", Decimal("100.0000")) is None


def test_unsupported_transaction_type_is_rejected():
    with pytest.raises(ValueError, match="Unsupported"):
        net_transaction_amount("invalid", Decimal("1.0000"))


def test_invalid_silver_schema_is_rejected():
    invalid = pa.record_batch(
        [pa.array(["SKU001"])],
        names=["stock_code"],
    )

    with pytest.raises(ValueError, match="schema"):
        transform_fact_batch(invalid)


def test_date_key_handles_missing_date():
    assert date_key(None) is None


def test_date_key_rejects_invalid_type():
    with pytest.raises(TypeError, match="datetime"):
        date_key("2010-01-02")


def test_fact_dimension_integrity():
    silver = make_batch([make_row()])
    collector = DimensionCollector()
    collector.consume(silver)

    fact = transform_fact_batch(silver)
    dimensions = collector.build_all()

    assert validate_fact_dimensions(fact, dimensions) is True


def test_missing_dimension_is_rejected():
    silver = make_batch([make_row()])
    collector = DimensionCollector()
    collector.consume(silver)

    dimensions = collector.build_all()
    del dimensions["dim_product"]

    with pytest.raises(ValueError, match="Missing dimension"):
        validate_fact_dimensions(
            transform_fact_batch(silver),
            dimensions,
        )


def test_unmatched_foreign_key_is_rejected():
    source = make_batch([make_row()])
    fact = transform_fact_batch(source)

    empty_dimensions = DimensionCollector().build_all()

    with pytest.raises(ValueError, match="Invalid foreign keys"):
        validate_fact_dimensions(fact, empty_dimensions)


def test_missing_date_fails_referential_validation():
    row = make_row()
    row["invoice_date"] = None
    row["is_date_missing"] = True

    silver = make_batch([row])
    collector = DimensionCollector()
    collector.consume(silver)

    fact = transform_fact_batch(silver)

    with pytest.raises(ValueError, match="Invalid foreign keys"):
        validate_fact_dimensions(fact, collector.build_all())
