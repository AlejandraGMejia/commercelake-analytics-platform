"""Tests for coordinated BigQuery staging publication."""

import json
import subprocess

import pytest

from commercelake.cloud.bigquery_publisher import PublicationPlan
from commercelake.cloud.bigquery_staging_runner import publish_staging_tables
from commercelake.warehouse.loader import LOAD_ORDER

RUN_ID = "591c8097f0554414be18648e3587d2b3"


def make_plan(tmp_path):
    """Create a synthetic six-table publication plan."""
    files_by_table = {
        table: [(f"{table}.parquet", index)]
        for index, table in enumerate(LOAD_ORDER, start=1)
    }

    return PublicationPlan(
        project_id="commercelake-analytics",
        dataset="commercelake_gold",
        location="US",
        run_id=RUN_ID,
        run_directory=tmp_path,
        files_by_table=files_by_table,
    )


def make_prepared_files(tmp_path):
    """Create dummy local files for command orchestration tests."""
    prepared = {}

    for table in LOAD_ORDER:
        path = tmp_path / f"{table}.parquet"
        path.write_bytes(b"mock-parquet")
        prepared[table] = path

    return prepared


def test_staging_loads_and_validates_all_tables(tmp_path):
    plan = make_plan(tmp_path)
    prepared = make_prepared_files(tmp_path)
    commands = []

    def fake_runner(command):
        commands.append(command)

        if "show" in command:
            table_index = (len(commands) // 2) - 1
            rows = table_index + 1
            stdout = json.dumps({"numRows": str(rows)})
        else:
            stdout = ""

        return subprocess.CompletedProcess(
            command,
            0,
            stdout=stdout,
            stderr="",
        )

    result = publish_staging_tables(
        plan,
        prepared,
        runner=fake_runner,
    )

    assert len(commands) == 12
    assert list(result) == list(LOAD_ORDER)
    assert list(result.values()) == list(range(1, 7))

    for index, table in enumerate(LOAD_ORDER):
        load_command = commands[index * 2]
        show_command = commands[index * 2 + 1]

        assert "load" in load_command
        assert "show" in show_command
        assert f"_stage_{table}_{RUN_ID[:8]}" in " ".join(load_command)


def test_staging_stops_on_row_count_mismatch(tmp_path):
    plan = make_plan(tmp_path)
    prepared = make_prepared_files(tmp_path)
    commands = []

    def fake_runner(command):
        commands.append(command)

        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps({"numRows": "999"}),
            stderr="",
        )

    with pytest.raises(ValueError, match="row count mismatch"):
        publish_staging_tables(
            plan,
            prepared,
            runner=fake_runner,
        )

    assert len(commands) == 2


def test_staging_stops_on_load_failure(tmp_path):
    plan = make_plan(tmp_path)
    prepared = make_prepared_files(tmp_path)
    commands = []

    def fake_runner(command):
        commands.append(command)

        raise subprocess.CalledProcessError(
            1,
            command,
            stderr="Simulated load failure",
        )

    with pytest.raises(subprocess.CalledProcessError):
        publish_staging_tables(
            plan,
            prepared,
            runner=fake_runner,
        )

    assert len(commands) == 1


def test_staging_rejects_missing_prepared_table(tmp_path):
    plan = make_plan(tmp_path)
    prepared = make_prepared_files(tmp_path)
    prepared.pop("fact_sales")

    with pytest.raises(ValueError, match="six Gold tables"):
        publish_staging_tables(
            plan,
            prepared,
            runner=lambda command: None,
        )


def test_staging_rejects_missing_local_file(tmp_path):
    plan = make_plan(tmp_path)
    prepared = make_prepared_files(tmp_path)
    prepared["fact_sales"].unlink()

    with pytest.raises(FileNotFoundError, match="Missing prepared"):
        publish_staging_tables(
            plan,
            prepared,
            runner=lambda command: None,
        )
