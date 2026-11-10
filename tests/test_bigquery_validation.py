"""Tests for BigQuery table metadata reconciliation."""

import json

import pytest

from commercelake.cloud.bigquery_validation import (
    build_show_command,
    parse_table_row_count,
    validate_table_row_count,
)

PROJECT = "commercelake-analytics"
DATASET = "commercelake_gold"


def test_build_show_command():
    command = build_show_command(
        PROJECT,
        DATASET,
        "_stage_fact_sales_591c8097",
    )

    assert command == [
        "bq",
        "--location=US",
        "show",
        "--format=prettyjson",
        ("commercelake-analytics:commercelake_gold._stage_fact_sales_591c8097"),
    ]


@pytest.mark.parametrize(
    ("raw_rows", "expected"),
    [
        ("0", 0),
        ("100", 100),
        ("1067371", 1_067_371),
        (25, 25),
    ],
)
def test_parse_valid_row_counts(raw_rows, expected):
    metadata = json.dumps({"numRows": raw_rows})

    assert parse_table_row_count(metadata) == expected


@pytest.mark.parametrize(
    "metadata",
    [
        "not-json",
        "[]",
        "{}",
        '{"numRows": null}',
        '{"numRows": true}',
        '{"numRows": -1}',
        '{"numRows": "invalid"}',
        '{"numRows": "12.5"}',
        '{"numRows": 12.5}',
    ],
)
def test_rejects_invalid_metadata(metadata):
    with pytest.raises((ValueError, TypeError)):
        parse_table_row_count(metadata)


@pytest.mark.parametrize(
    "table",
    [
        "",
        "fact_sales;DROP",
        "fact-sales",
        "table name",
    ],
)
def test_rejects_invalid_table_names(table):
    with pytest.raises(ValueError, match="Invalid BigQuery table name"):
        build_show_command(
            PROJECT,
            DATASET,
            table,
        )


def test_validates_matching_row_count():
    metadata = json.dumps({"numRows": "5943"})

    actual = validate_table_row_count(
        "dim_customer",
        5943,
        metadata,
    )

    assert actual == 5943


def test_rejects_mismatched_row_count():
    metadata = json.dumps({"numRows": "100"})

    with pytest.raises(ValueError, match="row count mismatch"):
        validate_table_row_count(
            "fact_sales",
            1_067_371,
            metadata,
        )


def test_rejects_unsupported_location():
    with pytest.raises(ValueError, match="location"):
        build_show_command(
            PROJECT,
            DATASET,
            "fact_sales",
            location="EU",
        )
