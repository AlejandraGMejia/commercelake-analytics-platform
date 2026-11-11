"""Tests for budget-controlled fact_sales staging INSERT commands."""

import pytest

from commercelake.cloud.bigquery_schema_validation import (
    expected_bigquery_schema,
)
from commercelake.cloud.bigquery_staging_insert import (
    MINIMUM_BYTES_BILLED,
    build_fact_sales_insert_command,
    build_fact_sales_insert_query,
    validate_query_budget,
)
from commercelake.cloud.bigquery_staging_transform import (
    build_fact_sales_transform_query,
)

PROJECT = "commercelake-analytics"
DATASET = "commercelake_gold"
RUN_ID = "591c8097f0554414be18648e3587d2b3"
SOURCE = "_ingest_fact_sales_591c8097"
DESTINATION = "_stage_fact_sales_591c8097"


def make_query(**overrides):
    options = {
        "project_id": PROJECT,
        "dataset": DATASET,
        "run_id": RUN_ID,
        "source_table": SOURCE,
    }
    options.update(overrides)
    return build_fact_sales_insert_query(**options)


def make_command(**overrides):
    options = {
        "project_id": PROJECT,
        "dataset": DATASET,
        "run_id": RUN_ID,
        "source_table": SOURCE,
        "maximum_bytes_billed": MINIMUM_BYTES_BILLED,
    }
    options.update(overrides)
    return build_fact_sales_insert_command(**options)


def test_insert_has_explicit_gold_columns():
    columns = ",\n".join(
        f"    `{field['name']}`"
        for field in expected_bigquery_schema("fact_sales")
    )
    assert f"(\n{columns}\n)" in make_query()


def test_insert_contains_exact_select():
    expected = build_fact_sales_transform_query(
        PROJECT,
        DATASET,
        RUN_ID,
        source_table=SOURCE,
    )
    assert make_query().endswith(expected)


def test_insert_targets_only_gold_staging():
    query = make_query()
    assert query.startswith(
        f"INSERT INTO `{PROJECT}.{DATASET}.{DESTINATION}`"
    )
    assert f"FROM `{PROJECT}.{DATASET}.{SOURCE}`" in query
    assert f"INSERT INTO `{PROJECT}.{DATASET}.fact_sales`" not in query


def test_insert_command_has_explicit_billing_limit():
    command = make_command()
    assert command[:3] == ["bq", "--location=US", "query"]
    assert "--use_legacy_sql=false" in command
    assert f"--maximum_bytes_billed={MINIMUM_BYTES_BILLED}" in command
    assert command[-1] == make_query()


@pytest.mark.parametrize(
    "budget",
    [None, 0, -1, True, 1.5, "10485760", 10485759],
)
def test_rejects_invalid_budgets(budget):
    with pytest.raises(ValueError, match="maximum_bytes_billed"):
        validate_query_budget(budget)


def test_accepts_explicit_larger_budget():
    assert validate_query_budget(100000000) == 100000000
    assert "--maximum_bytes_billed=100000000" in make_command(
        maximum_bytes_billed=100000000
    )


@pytest.mark.parametrize(
    "source",
    [
        "_stage_fact_sales_591c8097",
        "_ingest_fact_sales_00000000",
        "fact_sales",
    ],
)
def test_insert_rejects_wrong_source(source):
    with pytest.raises(ValueError, match="Unexpected"):
        make_query(source_table=source)


def test_insert_rejects_invalid_project():
    with pytest.raises(ValueError):
        make_query(project_id="invalid project")


def test_insert_rejects_unsupported_location():
    with pytest.raises(ValueError, match="location"):
        make_query(location="EU")


def test_gold_schema_is_unchanged():
    before = expected_bigquery_schema("fact_sales")
    make_query()
    assert expected_bigquery_schema("fact_sales") == before
