"""Tests for the streaming Gold monthly sales mart."""

from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from commercelake.gold.fact_sales import transform_fact_batch
from commercelake.gold.monthly_mart import (
    MONTHLY_MART_SCHEMA,
    MonthlySalesCollector,
)
from commercelake.transformation.silver_schema import SILVER_SCHEMA


def make_row(
    kind="sale",
    amount="20.0000",
    customer="12345",
    month=1,
):
    """Create a representative Silver transaction."""
    return {
        "invoice": "INV001",
        "stock_code": "SKU001",
        "description": "Test product",
        "quantity": 2,
        "invoice_date": datetime(2010, month, 2),
        "unit_price": Decimal("10.0000"),
        "customer_id": customer,
        "country": "United Kingdom",
        "source_sheet": "Year 2009-2010",
        "source_row_number": 1,
        "row_fingerprint": "fingerprint",
        "transaction_type": kind,
        "gross_amount": Decimal(amount),
        "is_customer_missing": customer is None,
        "is_price_non_positive": False,
        "is_date_missing": False,
        "silver_processed_at": datetime(2026, 1, 1, tzinfo=UTC),
    }


def fact_batch(rows):
    """Convert test Silver rows into a Gold fact batch."""
    silver = pa.Table.from_pylist(
        rows,
        schema=SILVER_SCHEMA,
    ).to_batches()[0]

    return transform_fact_batch(silver)


def test_monthly_mart_aggregates_financial_components():
    collector = MonthlySalesCollector()

    collector.consume(
        fact_batch(
            [
                make_row("sale", "20.0000"),
                make_row("sale", "15.5000"),
                make_row("cancellation", "-10.0000"),
                make_row("return", "-5.0000"),
                make_row("review", "0.0000"),
            ]
        )
    )

    table = collector.build()
    row = table.to_pylist()[0]

    assert table.schema == MONTHLY_MART_SCHEMA
    assert row["transaction_count"] == 5
    assert row["sale_count"] == 2
    assert row["cancellation_count"] == 1
    assert row["return_count"] == 1
    assert row["review_count"] == 1
    assert row["sales_amount"] == Decimal("35.5000")
    assert row["cancellation_amount"] == Decimal("-10.0000")
    assert row["return_amount"] == Decimal("-5.0000")
    assert row["net_amount"] == Decimal("20.5000")
    assert collector.validate(table) is True


def test_multiple_months_are_sorted():
    collector = MonthlySalesCollector()

    collector.consume(
        fact_batch(
            [
                make_row(month=3),
                make_row(month=1),
                make_row(month=2),
            ]
        )
    )

    assert collector.build().column("year_month").to_pylist() == [
        "2010-01",
        "2010-02",
        "2010-03",
    ]


def test_distinct_customers_exclude_unknown():
    collector = MonthlySalesCollector()

    collector.consume(
        fact_batch(
            [
                make_row(customer="12345"),
                make_row(customer="12345"),
                make_row(customer="67890"),
                make_row(customer=None),
            ]
        )
    )

    row = collector.build().to_pylist()[0]

    assert row["identified_customer_count"] == 2
    assert row["transaction_count"] == 4


def test_batches_accumulate_into_same_month():
    collector = MonthlySalesCollector()

    collector.consume(fact_batch([make_row(amount="10.0000")]))
    collector.consume(fact_batch([make_row(amount="15.0000")]))

    table = collector.build()

    assert table.num_rows == 1
    assert table.column("net_amount")[0].as_py() == Decimal("25.0000")
    assert collector.validate(table)


def test_empty_collector_has_valid_schema():
    collector = MonthlySalesCollector()
    table = collector.build()

    assert table.schema == MONTHLY_MART_SCHEMA
    assert table.num_rows == 0
    assert collector.validate(table)


def test_review_does_not_change_net_amount():
    collector = MonthlySalesCollector()

    collector.consume(fact_batch([make_row("review", "999.0000")]))

    row = collector.build().to_pylist()[0]

    assert row["net_amount"] == Decimal("0.0000")
    assert row["review_count"] == 1


def test_missing_date_is_rejected():
    collector = MonthlySalesCollector()
    batch = fact_batch([make_row()])

    index = batch.schema.get_field_index("date_key")
    columns = list(batch.columns)
    columns[index] = pa.array([None], type=pa.int32())

    invalid = pa.RecordBatch.from_arrays(
        columns,
        schema=batch.schema,
    )

    with pytest.raises(ValueError, match="date_key"):
        collector.consume(invalid)


def test_invalid_financial_reconciliation_is_detected():
    collector = MonthlySalesCollector()
    collector.consume(fact_batch([make_row()]))

    table = collector.build()
    rows = table.to_pylist()
    rows[0]["net_amount"] = Decimal("999.0000")

    corrupted = pa.Table.from_pylist(
        rows,
        schema=MONTHLY_MART_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="financial reconciliation",
    ):
        collector.validate(corrupted)
