"""Tests for BigQuery Parquet ingestion schemas."""

import pytest

from commercelake.cloud.bigquery_ingestion_schema import (
    expected_ingestion_schema,
)
from commercelake.cloud.bigquery_schema_validation import (
    expected_bigquery_schema,
)
from commercelake.warehouse.loader import LOAD_ORDER


def test_fact_sales_ingestion_uses_timestamp():
    ingestion = expected_ingestion_schema("fact_sales")
    gold = expected_bigquery_schema("fact_sales")

    ingestion_fields = {
        field["name"]: field
        for field in ingestion
    }
    gold_fields = {
        field["name"]: field
        for field in gold
    }

    assert ingestion_fields["invoice_date"]["type"] == "TIMESTAMP"
    assert gold_fields["invoice_date"]["type"] == "DATETIME"

    expected = [
        {
            **field,
            "type": (
                "TIMESTAMP"
                if field["name"] == "invoice_date"
                else field["type"]
            ),
        }
        for field in gold
    ]

    assert ingestion == expected


@pytest.mark.parametrize(
    "table",
    [table for table in LOAD_ORDER if table != "fact_sales"],
)
def test_other_tables_keep_gold_schema(table):
    assert expected_ingestion_schema(table) == (
        expected_bigquery_schema(table)
    )


def test_ingestion_does_not_mutate_gold_contract():
    before = expected_bigquery_schema("fact_sales")

    ingestion = expected_ingestion_schema("fact_sales")
    ingestion[0]["type"] = "BOOLEAN"

    assert expected_bigquery_schema("fact_sales") == before


def test_unknown_table_is_rejected():
    with pytest.raises(ValueError, match="Unknown Gold table"):
        expected_ingestion_schema("unknown_table")
