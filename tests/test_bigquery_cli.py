"""Unit tests for BigQuery CLI command construction and execution."""

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from commercelake.cloud.bigquery_cli import (
    ALLOWED_TABLES,
    DECIMAL_TABLES,
    build_load_command,
    run_command,
)
from commercelake.warehouse.loader import LOAD_ORDER

PROJECT = "commercelake-analytics"
DATASET = "commercelake_gold"


@pytest.mark.parametrize("table", LOAD_ORDER)
def test_builds_commands_for_all_gold_tables(table):
    source = Path(f"/tmp/{table}.parquet")

    command = build_load_command(
        project_id=PROJECT,
        dataset=DATASET,
        table=table,
        source_path=source,
    )

    assert command[0] == "bq"
    assert "--location=US" in command
    assert "load" in command
    assert "--source_format=PARQUET" in command
    assert "--replace" in command
    assert f"{PROJECT}:{DATASET}.{table}" in command
    assert str(source) == command[-1]


@pytest.mark.parametrize("table", LOAD_ORDER)
def test_decimal_configuration_matches_table(table):
    command = build_load_command(
        PROJECT,
        DATASET,
        table,
        Path(f"/tmp/{table}.parquet"),
    )

    has_decimal_option = "--decimal_target_types=BIGNUMERIC" in command

    assert has_decimal_option == (table in DECIMAL_TABLES)


def test_allowed_tables_match_gold_load_order():
    assert ALLOWED_TABLES == frozenset(LOAD_ORDER)


@pytest.mark.parametrize(
    "table",
    [
        "unknown_table",
        "fact_sales_backup",
        "fact_sales;DROP TABLE",
        "",
    ],
)
def test_rejects_unapproved_tables(table):
    with pytest.raises(ValueError, match="Unsupported CommerceLake table"):
        build_load_command(
            PROJECT,
            DATASET,
            table,
            Path("/tmp/source.parquet"),
        )


@pytest.mark.parametrize(
    "filename",
    [
        "/tmp/source.csv",
        "/tmp/source.json",
        "/tmp/source",
    ],
)
def test_rejects_non_parquet_sources(filename):
    with pytest.raises(ValueError, match="Parquet"):
        build_load_command(
            PROJECT,
            DATASET,
            "fact_sales",
            Path(filename),
        )


def test_rejects_unsupported_location():
    with pytest.raises(ValueError, match="location"):
        build_load_command(
            PROJECT,
            DATASET,
            "fact_sales",
            Path("/tmp/source.parquet"),
            location="EU",
        )


def test_run_command_uses_safe_subprocess_options():
    command = [
        "bq",
        "--location=US",
        "ls",
        f"{PROJECT}:{DATASET}",
    ]

    completed = subprocess.CompletedProcess(
        args=command,
        returncode=0,
        stdout="OK",
        stderr="",
    )

    with patch(
        "commercelake.cloud.bigquery_cli.subprocess.run",
        return_value=completed,
    ) as mocked_run:
        result = run_command(command)

    assert result.stdout == "OK"

    mocked_run.assert_called_once_with(
        command,
        check=True,
        capture_output=True,
        text=True,
    )


def test_run_command_propagates_cli_failure():
    command = ["bq", "--location=US", "ls"]

    with (
        patch(
            "commercelake.cloud.bigquery_cli.subprocess.run",
            side_effect=subprocess.CalledProcessError(
                returncode=1,
                cmd=command,
                stderr="Simulated BigQuery failure",
            ),
        ),
        pytest.raises(subprocess.CalledProcessError),
    ):
        run_command(command)


@pytest.mark.parametrize(
    "command",
    [
        [],
        ["python", "script.py"],
        ["bash", "-c", "echo hello"],
    ],
)
def test_run_command_rejects_non_bigquery_commands(command):
    with pytest.raises(ValueError, match="Only BigQuery"):
        run_command(command)
