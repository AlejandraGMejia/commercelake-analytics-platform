"""Tests for conservative BigQuery destination checks."""

from subprocess import CalledProcessError, CompletedProcess

import pytest

from commercelake.cloud.bigquery_destination_guard import (
    check_versioned_destinations,
    is_explicit_table_not_found,
)
from commercelake.cloud.bigquery_promotion import build_promotion_plan

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


def not_found(command):
    identifier = command[-1]
    return CompletedProcess(
        args=command,
        returncode=1,
        stdout="",
        stderr=f"Not found: Table {identifier}",
    )


def test_all_six_destinations_are_available():
    calls = []

    def runner(command):
        calls.append(command)
        return not_found(command)

    result = check_versioned_destinations(make_plan(), runner=runner)

    assert len(result.available_destinations) == 6
    assert result.run_id == RUN_ID
    assert len(calls) == 6
    assert all(command[2] == "show" for command in calls)


def test_existing_destination_blocks_publication():
    calls = []

    def runner(command):
        calls.append(command)
        return CompletedProcess(command, 0, "{}", "")

    with pytest.raises(FileExistsError, match="already exists"):
        check_versioned_destinations(make_plan(), runner=runner)

    assert len(calls) == 1


@pytest.mark.parametrize(
    "error",
    [
        "Access Denied: Permission denied",
        "Authentication failed",
        "Connection timed out",
        "Not found: Dataset another_dataset",
        "Unknown error",
        "",
    ],
)
def test_unknown_errors_fail_closed(error):
    def runner(command):
        return CompletedProcess(command, 1, "", error)

    with pytest.raises(RuntimeError, match="Cannot verify destination"):
        check_versioned_destinations(make_plan(), runner=runner)


def test_called_process_error_not_found_is_accepted():
    def runner(command):
        raise CalledProcessError(
            returncode=1,
            cmd=command,
            stderr=f"Not found: Table {command[-1]}",
        )

    result = check_versioned_destinations(make_plan(), runner=runner)

    assert len(result.available_destinations) == 6


def test_called_process_error_permission_denied_is_rejected():
    def runner(command):
        raise CalledProcessError(
            returncode=1,
            cmd=command,
            stderr="Access Denied",
        )

    with pytest.raises(RuntimeError, match="Cannot verify destination"):
        check_versioned_destinations(make_plan(), runner=runner)


def test_wrong_table_not_found_is_rejected():
    def runner(command):
        return CompletedProcess(
            command,
            1,
            "",
            "Not found: Table other-project:other_dataset.other_table",
        )

    with pytest.raises(RuntimeError, match="Cannot verify destination"):
        check_versioned_destinations(make_plan(), runner=runner)


def test_not_found_with_additional_error_is_rejected():
    plan = make_plan()
    table = plan.tables[0].version_name

    error = (
        f"Not found: Table commercelake-analytics:commercelake_gold.{table}\n"
        "Access Denied"
    )

    assert not is_explicit_table_not_found(
        error,
        project_id=plan.project_id,
        dataset=plan.dataset,
        table=table,
    )


def test_missing_runner_is_rejected():
    with pytest.raises(TypeError, match="callable runner"):
        check_versioned_destinations(make_plan(), runner=None)


def test_invalid_runner_response_is_rejected():
    def runner(command):
        return {"returncode": 0}

    with pytest.raises(TypeError, match="CompletedProcess"):
        check_versioned_destinations(make_plan(), runner=runner)
