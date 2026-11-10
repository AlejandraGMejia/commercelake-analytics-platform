"""Tests for local Gold financial reconciliation."""

from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from gold_test_data import sample_value

from commercelake.cloud.gold_financial_validation import (
    validate_gold_financials,
)
from commercelake.gold.monthly_mart import MonthlySalesCollector
from commercelake.warehouse.loader import TABLE_SCHEMAS

FACT_SCHEMA = TABLE_SCHEMAS["fact_sales"]
MART_SCHEMA = TABLE_SCHEMAS["mart_monthly_sales"]


def make_fact_table():
    """Build two months with sales, cancellation, and review."""
    transactions = [
        {
            "date_key": 20200115,
            "transaction_type": "sale",
            "net_transaction_amount": Decimal("10.0000"),
            "customer_id": "customer_a",
        },
        {
            "date_key": 20200116,
            "transaction_type": "cancellation",
            "net_transaction_amount": Decimal("-2.0000"),
            "customer_id": "customer_a",
        },
        {
            "date_key": 20200210,
            "transaction_type": "sale",
            "net_transaction_amount": Decimal("20.0000"),
            "customer_id": "customer_b",
        },
        {
            "date_key": 20200211,
            "transaction_type": "review",
            "net_transaction_amount": None,
            "customer_id": None,
        },
    ]

    arrays = []

    for field in FACT_SCHEMA:
        values = []

        for index, transaction in enumerate(transactions):
            if field.name in transaction:
                value = transaction[field.name]
            elif field.nullable:
                value = None
            else:
                value = sample_value(field.type, index)

            values.append(value)

        arrays.append(pa.array(values, type=field.type))

    return pa.Table.from_arrays(arrays, schema=FACT_SCHEMA)


def make_fixture(tmp_path):
    """Persist valid facts and the mart derived from them."""
    facts = make_fact_table()

    fact_path = tmp_path / "fact_sales.parquet"
    mart_path = tmp_path / "mart_monthly_sales.parquet"

    pq.write_table(facts, fact_path)

    collector = MonthlySalesCollector()

    for batch in facts.to_batches():
        collector.consume(batch)

    mart = collector.build()
    collector.validate(mart)

    pq.write_table(mart, mart_path)

    return fact_path, mart_path


def modify_mart(mart_path, modifier):
    """Modify persisted mart rows while preserving its schema."""
    rows = pq.read_table(mart_path).to_pylist()
    modifier(rows)

    modified = pa.Table.from_pylist(
        rows,
        schema=MART_SCHEMA,
    )

    pq.write_table(modified, mart_path)


def test_valid_financial_reconciliation(tmp_path):
    fact_path, mart_path = make_fixture(tmp_path)

    result = validate_gold_financials(
        [fact_path],
        mart_path,
        batch_size=2,
    )

    assert result == {
        "fact_rows": 4,
        "monthly_periods": 2,
    }


def test_rejects_empty_fact_inventory(tmp_path):
    _, mart_path = make_fixture(tmp_path)

    with pytest.raises(ValueError, match="At least one"):
        validate_gold_financials([], mart_path)


@pytest.mark.parametrize("batch_size", [0, -1])
def test_rejects_invalid_batch_size(tmp_path, batch_size):
    fact_path, mart_path = make_fixture(tmp_path)

    with pytest.raises(ValueError, match="batch_size"):
        validate_gold_financials(
            [fact_path],
            mart_path,
            batch_size=batch_size,
        )


def test_rejects_missing_fact_file(tmp_path):
    _, mart_path = make_fixture(tmp_path)

    with pytest.raises(FileNotFoundError):
        validate_gold_financials(
            [tmp_path / "missing.parquet"],
            mart_path,
        )


def test_rejects_invalid_fact_schema(tmp_path):
    fact_path, mart_path = make_fixture(tmp_path)

    facts = pq.read_table(fact_path)
    facts = facts.drop(["net_transaction_amount"])

    pq.write_table(facts, fact_path)

    with pytest.raises(ValueError, match="Invalid fact_sales schema"):
        validate_gold_financials(
            [fact_path],
            mart_path,
        )


def test_rejects_invalid_mart_schema(tmp_path):
    fact_path, mart_path = make_fixture(tmp_path)

    mart = pq.read_table(mart_path)
    mart = mart.drop(["net_amount"])

    pq.write_table(mart, mart_path)

    with pytest.raises(ValueError, match="Invalid mart_monthly_sales schema"):
        validate_gold_financials(
            [fact_path],
            mart_path,
        )


def test_rejects_financial_difference(tmp_path):
    fact_path, mart_path = make_fixture(tmp_path)

    def change_amount(rows):
        rows[0]["sales_amount"] += Decimal("1.0000")
        rows[0]["net_amount"] += Decimal("1.0000")

    modify_mart(mart_path, change_amount)

    with pytest.raises(ValueError, match="financial reconciliation"):
        validate_gold_financials(
            [fact_path],
            mart_path,
        )


def test_rejects_offsetting_monthly_errors(tmp_path):
    """A correct grand total must not conceal wrong monthly totals."""
    fact_path, mart_path = make_fixture(tmp_path)

    def offset_amounts(rows):
        rows[0]["sales_amount"] += Decimal("1.0000")
        rows[0]["net_amount"] += Decimal("1.0000")

        rows[1]["sales_amount"] -= Decimal("1.0000")
        rows[1]["net_amount"] -= Decimal("1.0000")

    modify_mart(mart_path, offset_amounts)

    with pytest.raises(ValueError, match="Monthly reconciliation mismatch"):
        validate_gold_financials(
            [fact_path],
            mart_path,
        )


def test_rejects_wrong_unique_customer_count(tmp_path):
    fact_path, mart_path = make_fixture(tmp_path)

    def change_customers(rows):
        rows[0]["identified_customer_count"] += 1

    modify_mart(mart_path, change_customers)

    with pytest.raises(ValueError, match="Monthly reconciliation mismatch"):
        validate_gold_financials(
            [fact_path],
            mart_path,
        )


def test_rejects_wrong_monthly_classification(tmp_path):
    fact_path, mart_path = make_fixture(tmp_path)

    def change_classification(rows):
        rows[0]["sale_count"] += 1
        rows[0]["cancellation_count"] -= 1

    modify_mart(mart_path, change_classification)

    with pytest.raises(ValueError, match="Monthly reconciliation mismatch"):
        validate_gold_financials(
            [fact_path],
            mart_path,
        )


def test_rejects_missing_month(tmp_path):
    fact_path, mart_path = make_fixture(tmp_path)

    def remove_month(rows):
        rows.pop()

    modify_mart(mart_path, remove_month)

    with pytest.raises(ValueError, match="Monthly transaction count mismatch"):
        validate_gold_financials(
            [fact_path],
            mart_path,
        )


def test_rejects_duplicate_month(tmp_path):
    fact_path, mart_path = make_fixture(tmp_path)

    def duplicate_month(rows):
        rows.append(rows[0].copy())

    modify_mart(mart_path, duplicate_month)

    with pytest.raises(ValueError, match="Monthly transaction count mismatch"):
        validate_gold_financials(
            [fact_path],
            mart_path,
        )
