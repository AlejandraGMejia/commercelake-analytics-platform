"""Tests for exporting authoritative BigQuery Gold schemas."""

import json

import pytest

from commercelake.cloud.bigquery_schema_export import export_bigquery_schema
from commercelake.cloud.bigquery_schema_validation import (
    expected_bigquery_schema,
)
from commercelake.warehouse.loader import LOAD_ORDER


@pytest.mark.parametrize("table", LOAD_ORDER)
def test_exports_complete_gold_schema(tmp_path, table):
    destination = tmp_path / f"{table}.json"

    result = export_bigquery_schema(table, destination)

    assert result == destination
    assert json.loads(destination.read_text(encoding="utf-8")) == (
        expected_bigquery_schema(table)
    )


def test_fact_sales_preserves_datetime_and_decimal_types(tmp_path):
    destination = tmp_path / "fact_sales.json"

    export_bigquery_schema("fact_sales", destination)

    fields = json.loads(destination.read_text(encoding="utf-8"))
    types = {field["name"]: field["type"] for field in fields}

    assert types["invoice_date"] == "DATETIME"
    assert types["unit_price"] == "NUMERIC"
    assert types["net_transaction_amount"] == "BIGNUMERIC"
    assert types["gross_amount"] == "BIGNUMERIC"


def test_rejects_unknown_table(tmp_path):
    with pytest.raises(ValueError, match="Unsupported CommerceLake table"):
        export_bigquery_schema("unknown_table", tmp_path / "unknown.json")


def test_rejects_non_json_destination(tmp_path):
    with pytest.raises(ValueError, match="JSON"):
        export_bigquery_schema("fact_sales", tmp_path / "fact_sales.txt")


def test_rejects_missing_destination_directory(tmp_path):
    with pytest.raises(FileNotFoundError, match="does not exist"):
        export_bigquery_schema(
            "fact_sales",
            tmp_path / "missing" / "fact_sales.json",
        )
