"""Tests for read-only BigQuery promotion preflight."""

import json
from dataclasses import replace
from subprocess import CompletedProcess

import pytest

from commercelake.cloud.bigquery_promotion import (
    build_promotion_plan,
)
from commercelake.cloud.bigquery_promotion_preflight import (
    validate_promotion_plan,
    validate_staging_preflight,
)
from commercelake.cloud.bigquery_schema_validation import (
    expected_bigquery_schema,
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
    """Build a complete valid Gold promotion plan."""
    return build_promotion_plan(
        project_id="commercelake-analytics",
        dataset="commercelake_gold",
        run_id=RUN_ID,
        expected_counts=COUNTS,
    )


def make_runner(
    *,
    bad_rows=None,
    bad_schema=None,
    fail_table=None,
):
    """Create a fake BigQuery metadata runner."""
    plan = make_plan()
    staging_to_logical = {
        table.staging_name: table.logical_name for table in plan.tables
    }
    calls = []

    def runner(command):
        calls.append(command)

        assert command[:3] == [
            "bq",
            "--location=US",
            "show",
        ]
        assert command[3] == "--format=prettyjson"

        staging_name = command[-1].split(".")[-1]
        logical_name = staging_to_logical[staging_name]

        if logical_name == fail_table:
            return CompletedProcess(
                args=command,
                returncode=1,
                stdout="",
                stderr="simulated failure",
            )

        rows = COUNTS[logical_name]
        if logical_name == bad_rows:
            rows += 1

        fields = expected_bigquery_schema(logical_name)

        if logical_name == bad_schema:
            fields[0]["type"] = "BYTES"

        metadata = json.dumps(
            {
                "numRows": str(rows),
                "schema": {"fields": fields},
            }
        )

        return CompletedProcess(
            args=command,
            returncode=0,
            stdout=metadata,
            stderr="",
        )

    return runner, calls


def test_preflight_validates_all_six_tables():
    runner, calls = make_runner()

    result = validate_staging_preflight(
        make_plan(),
        runner=runner,
    )

    assert result.run_id == RUN_ID
    assert result.validated_tables == tuple(LOAD_ORDER)
    assert result.table_row_counts == COUNTS
    assert result.total_rows == sum(COUNTS.values())
    assert len(calls) == 6


def test_preflight_uses_only_read_only_show_commands():
    runner, calls = make_runner()

    validate_staging_preflight(make_plan(), runner=runner)

    assert all(command[2] == "show" for command in calls)
    assert not any(
        token in command
        for command in calls
        for token in ("load", "cp", "rm", "--replace")
    )


def test_preflight_rejects_wrong_row_count():
    runner, calls = make_runner(bad_rows="dim_customer")

    with pytest.raises(ValueError, match="row count mismatch"):
        validate_staging_preflight(make_plan(), runner=runner)

    assert len(calls) == 2


def test_preflight_rejects_wrong_schema():
    runner, calls = make_runner(bad_schema="fact_sales")

    with pytest.raises(ValueError, match="schema mismatch"):
        validate_staging_preflight(make_plan(), runner=runner)

    assert len(calls) == 5


def test_preflight_stops_on_metadata_command_failure():
    runner, calls = make_runner(fail_table="dim_country")

    with pytest.raises(RuntimeError, match="metadata command failed"):
        validate_staging_preflight(make_plan(), runner=runner)

    assert len(calls) == 3


def test_preflight_rejects_incomplete_plan_before_runner():
    plan = make_plan()
    invalid = replace(plan, tables=plan.tables[:-1])
    runner, calls = make_runner()

    with pytest.raises(ValueError, match="all six Gold tables"):
        validate_staging_preflight(invalid, runner=runner)

    assert calls == []


def test_preflight_rejects_modified_staging_name_before_runner():
    plan = make_plan()
    changed = replace(
        plan.tables[0],
        staging_name="unexpected_table",
    )
    invalid = replace(
        plan,
        tables=(changed, *plan.tables[1:]),
    )
    runner, calls = make_runner()

    with pytest.raises(ValueError, match="canonical Gold layout"):
        validate_staging_preflight(invalid, runner=runner)

    assert calls == []


def test_preflight_rejects_duplicate_tables_before_runner():
    plan = make_plan()
    invalid = replace(
        plan,
        tables=(plan.tables[0], plan.tables[0], *plan.tables[2:]),
    )
    runner, calls = make_runner()

    with pytest.raises(ValueError, match="duplicate Gold tables"):
        validate_staging_preflight(invalid, runner=runner)

    assert calls == []


def test_preflight_requires_explicit_callable_runner():
    with pytest.raises(TypeError, match="callable runner"):
        validate_staging_preflight(make_plan(), runner=None)


def test_validate_promotion_plan_accepts_canonical_plan():
    validate_promotion_plan(make_plan())


def test_preflight_rejects_invalid_runner_response():
    def invalid_runner(command):
        return {"stdout": "{}"}

    with pytest.raises(TypeError, match="CompletedProcess"):
        validate_staging_preflight(
            make_plan(),
            runner=invalid_runner,
        )
