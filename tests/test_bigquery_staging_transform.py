"""Tests for explicit fact_sales staging transformations."""

import pytest

from commercelake.cloud.bigquery_schema_validation import (
    expected_bigquery_schema,
)
from commercelake.cloud.bigquery_staging_transform import (
    build_fact_sales_transform_dry_run_command,
    build_fact_sales_transform_query,
)

PROJECT = "commercelake-analytics"
DATASET = "commercelake_gold"
RUN_ID = "591c8097f0554414be18648e3587d2b3"
SOURCE = "_ingest_fact_sales_591c8097"


def build_query(**overrides):
    options = {
        "project_id": PROJECT,
        "dataset": DATASET,
        "run_id": RUN_ID,
        "source_table": SOURCE,
    }
    options.update(overrides)

    return build_fact_sales_transform_query(**options)


def test_transform_preserves_gold_column_order():
    query = build_query()
    fields = expected_bigquery_schema("fact_sales")

    projection = query.split("\nFROM ", 1)[0]
    expressions = projection.removeprefix("SELECT\n").split(",\n")

    assert len(expressions) == len(fields)

    for expression, field in zip(expressions, fields, strict=True):
        name = field["name"]

        if name == "invoice_date":
            assert expression.strip() == (
                "DATETIME(`invoice_date`, 'UTC') AS `invoice_date`"
            )
        else:
            assert expression.strip() == f"`{name}`"


def test_transform_uses_exact_ingestion_source():
    query = build_query()

    assert query.endswith(
        f"FROM `{PROJECT}.{DATASET}.{SOURCE}`"
    )


def test_transform_is_read_only():
    query = build_query().upper()

    assert "INSERT INTO" not in query
    assert "CREATE TABLE" not in query
    assert "MERGE INTO" not in query
    assert "DELETE FROM" not in query
    assert "UPDATE " not in query


def test_dry_run_command_is_read_only():
    command = build_fact_sales_transform_dry_run_command(
        PROJECT,
        DATASET,
        RUN_ID,
        source_table=SOURCE,
    )

    assert command[:3] == [
        "bq",
        "--location=US",
        "query",
    ]
    assert "--dry_run" in command
    assert "--use_legacy_sql=false" in command
    assert command[-1] == build_query()


@pytest.mark.parametrize(
    "source",
    [
        "fact_sales",
        "_stage_fact_sales_591c8097",
        "_ingest_fact_sales_00000000",
        "_ingest_dim_product_591c8097",
    ],
)
def test_rejects_unexpected_source(source):
    with pytest.raises(ValueError, match="Unexpected"):
        build_query(source_table=source)


def test_rejects_invalid_project():
    with pytest.raises(ValueError):
        build_query(project_id="invalid project")


def test_rejects_unsupported_location():
    with pytest.raises(ValueError, match="location"):
        build_query(location="EU")
