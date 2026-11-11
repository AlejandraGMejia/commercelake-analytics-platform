"""Tests for safe BigQuery staging table creation commands."""

import json

import pytest

from commercelake.cloud.bigquery_schema_validation import (
    expected_bigquery_schema,
)
from commercelake.cloud.bigquery_staging_create import (
    DEFAULT_STAGING_EXPIRATION,
    build_staging_create_command,
)
from commercelake.warehouse.loader import LOAD_ORDER

PROJECT = "commercelake-analytics"
DATASET = "commercelake_gold"
RUN_ID = "591c8097f0554414be18648e3587d2b3"


def write_schema(tmp_path, table, schema=None):
    """Write a schema fixture for a specific Gold table."""
    path = tmp_path / f"{table}.json"

    if schema is None:
        schema = expected_bigquery_schema(table)

    path.write_text(
        json.dumps(schema),
        encoding="utf-8",
    )
    return path


@pytest.mark.parametrize("table", LOAD_ORDER)
def test_builds_staging_create_command(tmp_path, table):
    schema_path = write_schema(tmp_path, table)

    command = build_staging_create_command(
        PROJECT,
        DATASET,
        table,
        RUN_ID,
        schema_path,
    )

    assert command[0] == "bq"
    assert "mk" in command
    assert "--table" in command
    assert f"--expiration={DEFAULT_STAGING_EXPIRATION}" in command
    assert f"--schema={schema_path}" in command
    assert f"{PROJECT}:{DATASET}._stage_{table}_{RUN_ID[:8]}" in command


def test_rejects_missing_schema_file(tmp_path):
    with pytest.raises(FileNotFoundError, match="Missing BigQuery schema"):
        build_staging_create_command(
            PROJECT,
            DATASET,
            "fact_sales",
            RUN_ID,
            tmp_path / "missing.json",
        )


def test_rejects_non_json_schema(tmp_path):
    schema_path = tmp_path / "schema.txt"
    schema_path.write_text("[]", encoding="utf-8")

    with pytest.raises(ValueError, match="JSON"):
        build_staging_create_command(
            PROJECT,
            DATASET,
            "fact_sales",
            RUN_ID,
            schema_path,
        )


@pytest.mark.parametrize(
    "expiration",
    [0, -1, True, 1.5, "86400"],
)
def test_rejects_invalid_expiration(tmp_path, expiration):
    schema_path = write_schema(tmp_path, "fact_sales")

    with pytest.raises(ValueError, match="expiration"):
        build_staging_create_command(
            PROJECT,
            DATASET,
            "fact_sales",
            RUN_ID,
            schema_path,
            expiration_seconds=expiration,
        )


def test_rejects_unsupported_location(tmp_path):
    schema_path = write_schema(tmp_path, "fact_sales")

    with pytest.raises(ValueError, match="location"):
        build_staging_create_command(
            PROJECT,
            DATASET,
            "fact_sales",
            RUN_ID,
            schema_path,
            location="EU",
        )


def test_rejects_invalid_run_id(tmp_path):
    schema_path = write_schema(tmp_path, "fact_sales")

    with pytest.raises(ValueError, match="Invalid Gold run ID"):
        build_staging_create_command(
            PROJECT,
            DATASET,
            "fact_sales",
            "invalid",
            schema_path,
        )


@pytest.mark.parametrize(
    "invalid_schema",
    [
        [],
        [{"name": "invoice_date", "type": "TIMESTAMP"}],
        {"fields": []},
    ],
)
def test_rejects_schema_not_matching_gold_contract(
    tmp_path,
    invalid_schema,
):
    schema_path = write_schema(
        tmp_path,
        "fact_sales",
        invalid_schema,
    )

    with pytest.raises(ValueError, match="Gold contract"):
        build_staging_create_command(
            PROJECT,
            DATASET,
            "fact_sales",
            RUN_ID,
            schema_path,
        )


def test_rejects_malformed_json(tmp_path):
    schema_path = tmp_path / "fact_sales.json"
    schema_path.write_text("{invalid", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid BigQuery schema JSON"):
        build_staging_create_command(
            PROJECT,
            DATASET,
            "fact_sales",
            RUN_ID,
            schema_path,
        )


def test_rejects_incorrect_decimal_type(tmp_path):
    schema = expected_bigquery_schema("fact_sales")

    for field in schema:
        if field["name"] == "unit_price":
            field["type"] = "BIGNUMERIC"

    schema_path = write_schema(tmp_path, "fact_sales", schema)

    with pytest.raises(ValueError, match="Gold contract"):
        build_staging_create_command(
            PROJECT,
            DATASET,
            "fact_sales",
            RUN_ID,
            schema_path,
        )
