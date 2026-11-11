"""Tests for safe BigQuery versioned copy command construction."""

from dataclasses import replace

import pytest

from commercelake.cloud.bigquery_promotion import build_promotion_plan
from commercelake.cloud.bigquery_versioned_copy import (
    build_versioned_copy_command,
    build_versioned_copy_plan,
)
from commercelake.warehouse.loader import LOAD_ORDER

RUN_ID = "591c8097f0554414be18648e3587d2b3"

COUNTS = {
    "dim_product": 5305,
    "dim_customer": 5943,
    "dim_country": 44,
    "dim_date": 739,
    "fact_sales": 1067371,
    "mart_monthly_sales": 25,
}


def make_plan():
    return build_promotion_plan(
        project_id="commercelake-analytics",
        dataset="commercelake_gold",
        run_id=RUN_ID,
        expected_counts=COUNTS,
    )


def test_build_safe_copy_command():
    command = build_versioned_copy_command(
        project_id="commercelake-analytics",
        dataset="commercelake_gold",
        source_table="_stage_dim_product_591c8097",
        destination_table=f"gold_dim_product_{RUN_ID}",
    )

    assert command == [
        "bq",
        "--location=US",
        "cp",
        "--no_clobber",
        "commercelake-analytics:commercelake_gold._stage_dim_product_591c8097",
        f"commercelake-analytics:commercelake_gold.gold_dim_product_{RUN_ID}",
    ]


def test_copy_command_never_requests_overwrite():
    command = build_versioned_copy_command(
        project_id="commercelake-analytics",
        dataset="commercelake_gold",
        source_table="source_table",
        destination_table="destination_table",
    )

    assert "--no_clobber" in command
    assert "--force" not in command
    assert "--replace" not in command
    assert "--append_table" not in command


def test_build_complete_versioned_copy_plan():
    commands = build_versioned_copy_plan(make_plan())

    assert len(commands) == 6
    assert tuple(item.logical_name for item in commands) == LOAD_ORDER
    assert all("--no_clobber" in item.command for item in commands)


def test_versioned_destinations_use_full_run_id():
    commands = build_versioned_copy_plan(make_plan())

    for item in commands:
        assert item.destination_table.endswith(RUN_ID)
        assert item.command[-1].endswith(item.destination_table)


def test_copy_sources_match_staging_tables():
    plan = make_plan()
    commands = build_versioned_copy_plan(plan)

    for table, item in zip(plan.tables, commands, strict=True):
        assert item.source_table == table.staging_name
        assert item.destination_table == table.version_name


@pytest.mark.parametrize(
    "invalid_table",
    [
        "table-name",
        "table.name",
        "table name",
        "table;drop",
        "",
    ],
)
def test_rejects_invalid_source_table(invalid_table):
    with pytest.raises(ValueError, match="Invalid BigQuery table name"):
        build_versioned_copy_command(
            project_id="commercelake-analytics",
            dataset="commercelake_gold",
            source_table=invalid_table,
            destination_table="valid_destination",
        )


@pytest.mark.parametrize(
    "invalid_table",
    [
        "table-name",
        "table.name",
        "table name",
        "table;drop",
        "",
    ],
)
def test_rejects_invalid_destination_table(invalid_table):
    with pytest.raises(ValueError, match="Invalid BigQuery table name"):
        build_versioned_copy_command(
            project_id="commercelake-analytics",
            dataset="commercelake_gold",
            source_table="valid_source",
            destination_table=invalid_table,
        )


def test_rejects_same_source_and_destination():
    with pytest.raises(ValueError, match="must be different"):
        build_versioned_copy_command(
            project_id="commercelake-analytics",
            dataset="commercelake_gold",
            source_table="same_table",
            destination_table="same_table",
        )


def test_rejects_unsupported_location():
    with pytest.raises(ValueError, match="Unsupported BigQuery location"):
        build_versioned_copy_command(
            project_id="commercelake-analytics",
            dataset="commercelake_gold",
            source_table="source_table",
            destination_table="destination_table",
            location="EU",
        )


def test_rejects_invalid_project_identifier():
    with pytest.raises(ValueError):
        build_versioned_copy_command(
            project_id="invalid project",
            dataset="commercelake_gold",
            source_table="source_table",
            destination_table="destination_table",
        )


def test_rejects_modified_promotion_plan():
    plan = make_plan()
    modified_table = replace(
        plan.tables[0],
        version_name="unexpected_destination",
    )
    modified_plan = replace(
        plan,
        tables=(modified_table, *plan.tables[1:]),
    )

    with pytest.raises(ValueError, match="canonical Gold layout"):
        build_versioned_copy_plan(modified_plan)
