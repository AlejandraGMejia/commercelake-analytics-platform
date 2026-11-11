"""Tests for conservative BigQuery staging existence checks."""

from subprocess import CalledProcessError, CompletedProcess

import pytest

from commercelake.cloud.bigquery_staging_guard import (
    require_staging_table_absent,
)
from commercelake.warehouse.loader import LOAD_ORDER

PROJECT = "commercelake-analytics"
DATASET = "commercelake_gold"
RUN_ID = "591c8097f0554414be18648e3587d2b3"


def check(table, runner):
    """Check a staging destination with standard test identifiers."""
    return require_staging_table_absent(
        PROJECT,
        DATASET,
        table,
        RUN_ID,
        runner=runner,
    )


def missing_result(command):
    """Simulate an explicit BigQuery table-not-found response."""
    return CompletedProcess(
        args=command,
        returncode=1,
        stdout="",
        stderr=f"Not found: Table {command[-1]}",
    )


@pytest.mark.parametrize("table", LOAD_ORDER)
def test_confirms_missing_staging_table(table):
    calls = []

    def runner(command):
        calls.append(command)
        return missing_result(command)

    stage = check(table, runner)

    assert stage == f"_stage_{table}_{RUN_ID[:8]}"
    assert len(calls) == 1
    assert calls[0][2] == "show"
    assert calls[0][-1] == f"{PROJECT}:{DATASET}.{stage}"


def test_existing_staging_table_is_rejected():
    calls = []

    def runner(command):
        calls.append(command)
        return CompletedProcess(command, 0, "{}", "")

    with pytest.raises(FileExistsError, match="already exists"):
        check("fact_sales", runner)

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
def test_ambiguous_errors_are_rejected(error):
    def runner(command):
        return CompletedProcess(command, 1, "", error)

    with pytest.raises(RuntimeError, match="Cannot verify staging"):
        check("fact_sales", runner)


def test_wrong_table_not_found_is_rejected():
    def runner(command):
        return CompletedProcess(
            command,
            1,
            "",
            "Not found: Table other-project:other_dataset.other_table",
        )

    with pytest.raises(RuntimeError, match="Cannot verify staging"):
        check("fact_sales", runner)


def test_called_process_error_missing_is_accepted():
    def runner(command):
        raise CalledProcessError(
            returncode=1,
            cmd=command,
            stderr=f"Not found: Table {command[-1]}",
        )

    stage = check("fact_sales", runner)

    assert stage == f"_stage_fact_sales_{RUN_ID[:8]}"


def test_called_process_error_permission_denied_is_rejected():
    def runner(command):
        raise CalledProcessError(
            returncode=1,
            cmd=command,
            stderr="Access Denied",
        )

    with pytest.raises(RuntimeError, match="Cannot verify staging"):
        check("fact_sales", runner)


def test_invalid_runner_result_is_rejected():
    def runner(command):
        return {"returncode": 1}

    with pytest.raises(TypeError, match="CompletedProcess"):
        check("fact_sales", runner)


def test_missing_runner_is_rejected():
    with pytest.raises(TypeError, match="callable runner"):
        check("fact_sales", None)


def test_invalid_run_id_is_rejected():
    with pytest.raises(ValueError, match="Invalid Gold run ID"):
        require_staging_table_absent(
            PROJECT,
            DATASET,
            "fact_sales",
            "invalid",
            runner=lambda command: None,
        )


def test_invalid_table_is_rejected():
    with pytest.raises(ValueError, match="Unsupported CommerceLake table"):
        check("unknown_table", lambda command: None)


def test_read_only_command_is_used():
    commands = []

    def runner(command):
        commands.append(command)
        return missing_result(command)

    check("fact_sales", runner)

    assert len(commands) == 1
    assert "show" in commands[0]
    assert "load" not in commands[0]
    assert "mk" not in commands[0]
    assert "rm" not in commands[0]
