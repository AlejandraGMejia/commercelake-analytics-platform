"""Tests for read-only BigQuery Gold recovery inspection."""

from subprocess import CalledProcessError, CompletedProcess

import pytest
from test_bigquery_versioned_publisher import FakeBigQuery, make_plan

from commercelake.cloud.bigquery_recovery_inspector import (
    inspect_gold_version,
)


def test_all_valid_destinations_are_ready():
    fake = FakeBigQuery()
    plan = make_plan()

    for table in plan.tables:
        fake.versions[table.version_name] = table.logical_name

    result = inspect_gold_version(plan, runner=fake)

    assert result.ready
    assert result.status_counts["valid"] == 6
    assert all(command[2] == "show" for command in fake.commands)


def test_all_missing_destinations_are_not_ready():
    fake = FakeBigQuery()

    result = inspect_gold_version(make_plan(), runner=fake)

    assert not result.ready
    assert result.status_counts["missing"] == 6
    assert len(fake.commands) == 6


def test_partial_publication_is_not_ready():
    fake = FakeBigQuery()
    plan = make_plan()

    for table in plan.tables[:3]:
        fake.versions[table.version_name] = table.logical_name

    result = inspect_gold_version(plan, runner=fake)

    assert not result.ready
    assert result.status_counts == {
        "valid": 3,
        "missing": 3,
        "invalid": 0,
        "unknown": 0,
    }


def test_invalid_schema_is_reported():
    fake = FakeBigQuery(corrupt_version="fact_sales")
    plan = make_plan()

    for table in plan.tables:
        fake.versions[table.version_name] = table.logical_name

    result = inspect_gold_version(plan, runner=fake)

    assert not result.ready
    assert result.status_counts["invalid"] == 1
    assert result.status_counts["valid"] == 5


def test_metadata_failure_is_unknown():
    fake = FakeBigQuery(fail_verification="dim_customer")
    plan = make_plan()

    for table in plan.tables:
        fake.versions[table.version_name] = table.logical_name

    result = inspect_gold_version(plan, runner=fake)

    assert not result.ready
    assert result.status_counts["unknown"] == 1


def test_permission_denied_is_not_treated_as_missing():

    def runner(command):
        return CompletedProcess(
            command,
            1,
            "",
            "Access Denied",
        )

    result = inspect_gold_version(make_plan(), runner=runner)

    assert result.status_counts["unknown"] == 6
    assert result.status_counts["missing"] == 0


def test_called_process_error_not_found_is_missing():
    def runner(command):
        raise CalledProcessError(
            returncode=1,
            cmd=command,
            stderr=f"Not found: Table {command[-1]}",
        )

    result = inspect_gold_version(make_plan(), runner=runner)

    assert result.status_counts["missing"] == 6


def test_connection_exception_is_unknown():
    def runner(command):
        raise ConnectionError("Simulated network interruption")

    result = inspect_gold_version(make_plan(), runner=runner)

    assert result.status_counts["unknown"] == 6
    assert not result.ready


def test_invalid_runner_response_is_unknown():
    def runner(command):
        return {"status": "success"}

    result = inspect_gold_version(make_plan(), runner=runner)

    assert result.status_counts["unknown"] == 6


def test_missing_runner_is_rejected():
    with pytest.raises(TypeError, match="callable runner"):
        inspect_gold_version(make_plan(), runner=None)


def test_inspector_never_executes_copy_or_delete():
    fake = FakeBigQuery()

    inspect_gold_version(make_plan(), runner=fake)

    assert len(fake.commands) == 6
    assert all(command[2] == "show" for command in fake.commands)
    assert not any(
        token in command
        for command in fake.commands
        for token in ("cp", "rm", "load", "query", "--replace")
    )
