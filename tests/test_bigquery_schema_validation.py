"""Tests for BigQuery schema validation against Gold contracts."""

import json

import pyarrow as pa
import pytest

from commercelake.cloud.bigquery_schema_validation import (
    arrow_type_to_bigquery,
    expected_bigquery_schema,
    parse_bigquery_schema,
    validate_bigquery_schema,
)
from commercelake.warehouse.loader import LOAD_ORDER, TABLE_SCHEMAS


def metadata_for(table):
    """Construct simulated BigQuery metadata from the Gold contract."""
    return json.dumps(
        {
            "schema": {
                "fields": expected_bigquery_schema(table),
            }
        }
    )


@pytest.mark.parametrize(
    ("arrow_type", "expected"),
    [
        (pa.string(), "STRING"),
        (pa.int32(), "INTEGER"),
        (pa.int64(), "INTEGER"),
        (pa.bool_(), "BOOLEAN"),
        (pa.float64(), "FLOAT"),
        (pa.date32(), "DATE"),
        (pa.timestamp("us"), "DATETIME"),
        (pa.timestamp("us", tz="UTC"), "TIMESTAMP"),
        (pa.decimal128(18, 4), "NUMERIC"),
        (pa.decimal128(38, 4), "BIGNUMERIC"),
    ],
)
def test_arrow_type_mapping(arrow_type, expected):
    assert arrow_type_to_bigquery(arrow_type) == expected


def test_rejects_unsupported_arrow_type():
    with pytest.raises(ValueError, match="Unsupported Arrow type"):
        arrow_type_to_bigquery(pa.list_(pa.string()))


def test_all_gold_schemas_have_supported_types():
    for table in LOAD_ORDER:
        expected = expected_bigquery_schema(table)
        assert len(expected) == len(TABLE_SCHEMAS[table])


@pytest.mark.parametrize("table", LOAD_ORDER)
def test_accepts_matching_gold_schema(table):
    actual = validate_bigquery_schema(table, metadata_for(table))
    assert actual == expected_bigquery_schema(table)


def test_rejects_missing_column():
    fields = expected_bigquery_schema("dim_product")
    fields.pop()

    metadata = json.dumps({"schema": {"fields": fields}})

    with pytest.raises(ValueError, match="schema mismatch"):
        validate_bigquery_schema("dim_product", metadata)


def test_rejects_extra_column():
    fields = expected_bigquery_schema("dim_product")
    fields.append({"name": "unexpected", "type": "STRING", "mode": "NULLABLE"})

    metadata = json.dumps({"schema": {"fields": fields}})

    with pytest.raises(ValueError, match="schema mismatch"):
        validate_bigquery_schema("dim_product", metadata)


def test_rejects_wrong_column_type():
    fields = expected_bigquery_schema("fact_sales")
    fields[-1]["type"] = "STRING"

    metadata = json.dumps({"schema": {"fields": fields}})

    with pytest.raises(ValueError, match="schema mismatch"):
        validate_bigquery_schema("fact_sales", metadata)


def test_rejects_wrong_nullability():
    fields = expected_bigquery_schema("dim_product")
    fields[0]["mode"] = "NULLABLE"

    metadata = json.dumps({"schema": {"fields": fields}})

    with pytest.raises(ValueError, match="schema mismatch"):
        validate_bigquery_schema("dim_product", metadata)


def test_rejects_wrong_column_order():
    fields = expected_bigquery_schema("dim_product")
    fields[0], fields[1] = fields[1], fields[0]

    metadata = json.dumps({"schema": {"fields": fields}})

    with pytest.raises(ValueError, match="schema mismatch"):
        validate_bigquery_schema("dim_product", metadata)


def test_rejects_duplicate_columns():
    fields = [
        {"name": "id", "type": "INTEGER"},
        {"name": "id", "type": "INTEGER"},
    ]

    with pytest.raises(ValueError, match="Duplicate"):
        parse_bigquery_schema(json.dumps({"schema": {"fields": fields}}))


@pytest.mark.parametrize(
    "metadata",
    [
        "not-json",
        "[]",
        "{}",
        '{"schema": null}',
        '{"schema": {"fields": null}}',
        '{"schema": {"fields": [null]}}',
    ],
)
def test_rejects_invalid_metadata(metadata):
    with pytest.raises((ValueError, TypeError)):
        parse_bigquery_schema(metadata)


def test_rejects_nested_fields():
    metadata = json.dumps(
        {
            "schema": {
                "fields": [
                    {
                        "name": "nested",
                        "type": "RECORD",
                        "fields": [],
                    }
                ]
            }
        }
    )

    with pytest.raises(ValueError, match="Nested"):
        parse_bigquery_schema(metadata)


def test_normalizes_bigquery_type_aliases():
    metadata = json.dumps(
        {
            "schema": {
                "fields": [
                    {"name": "count", "type": "INT64"},
                    {"name": "amount", "type": "BIGDECIMAL"},
                ]
            }
        }
    )

    assert parse_bigquery_schema(metadata) == [
        {"name": "count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "amount", "type": "BIGNUMERIC", "mode": "NULLABLE"},
    ]


def test_rejects_unknown_gold_table():
    with pytest.raises(ValueError, match="Unknown Gold table"):
        expected_bigquery_schema("unknown_table")
