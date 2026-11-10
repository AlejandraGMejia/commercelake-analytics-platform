"""Tests for versioned BigQuery promotion planning."""

import pytest

from commercelake.cloud.bigquery_promotion import (
    build_promotion_plan,
    version_table_name,
)
from commercelake.warehouse.loader import LOAD_ORDER

RUN_ID = "591c8097f0554414be18648e3587d2b3"


def make_counts():
    """Return expected rows for six Gold tables."""
    return {
        "dim_product": 5305,
        "dim_customer": 5943,
        "dim_country": 44,
        "dim_date": 739,
        "fact_sales": 1067371,
        "mart_monthly_sales": 25,
    }


def make_plan(**overrides):
    """Build a valid plan with optional input overrides."""
    arguments = {
        "project_id": "commercelake-analytics",
        "dataset": "commercelake_gold",
        "run_id": RUN_ID,
        "expected_counts": make_counts(),
        "location": "US",
    }
    arguments.update(overrides)

    return build_promotion_plan(**arguments)


def test_builds_complete_promotion_plan():
    plan = make_plan()

    assert len(plan.tables) == 6
    assert [table.logical_name for table in plan.tables] == list(LOAD_ORDER)
    assert plan.total_rows == sum(make_counts().values())


def test_uses_full_run_id_for_versioned_tables():
    plan = make_plan()

    for table in plan.tables:
        assert table.version_name.endswith(RUN_ID)
        assert table.staging_name.endswith(RUN_ID[:8])
        assert table.version_name != table.staging_name


def test_versions_from_different_runs_have_different_names():
    first = version_table_name("fact_sales", RUN_ID)
    second = version_table_name("fact_sales", "a" * 32)

    assert first != second


@pytest.mark.parametrize(
    "run_id",
    ["", "invalid", "A" * 32, "a" * 31, "a" * 33],
)
def test_rejects_invalid_run_ids(run_id):
    with pytest.raises(ValueError, match="Invalid Gold run ID"):
        make_plan(run_id=run_id)


def test_rejects_incomplete_counts():
    counts = make_counts()
    counts.pop("fact_sales")

    with pytest.raises(ValueError, match="all six Gold tables"):
        make_plan(expected_counts=counts)


def test_rejects_extra_tables():
    counts = make_counts()
    counts["unexpected"] = 1

    with pytest.raises(ValueError, match="all six Gold tables"):
        make_plan(expected_counts=counts)


@pytest.mark.parametrize("invalid_count", [-1, True, 1.5, "100", None])
def test_rejects_invalid_row_counts(invalid_count):
    counts = make_counts()
    counts["fact_sales"] = invalid_count

    with pytest.raises(ValueError, match="Invalid expected row count"):
        make_plan(expected_counts=counts)


def test_rejects_unsupported_location():
    with pytest.raises(ValueError, match="Unsupported BigQuery location"):
        make_plan(location="EU")


def test_rejects_invalid_dataset():
    with pytest.raises(ValueError):
        make_plan(dataset="invalid.dataset")


def test_rejects_unsupported_table_name():
    with pytest.raises(ValueError, match="Unsupported CommerceLake table"):
        version_table_name("unknown_table", RUN_ID)
