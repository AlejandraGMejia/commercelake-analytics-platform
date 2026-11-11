"""Tests for fail-closed BigQuery staging publication."""

import json
import subprocess
from pathlib import Path

import pytest

from commercelake.cloud.bigquery_publisher import PublicationPlan
from commercelake.cloud.bigquery_schema_validation import (
    expected_bigquery_schema,
)
from commercelake.cloud.bigquery_staging_runner import (
    publish_staging_tables,
)
from commercelake.warehouse.loader import LOAD_ORDER

RUN_ID = "591c8097f0554414be18648e3587d2b3"
PROJECT = "commercelake-analytics"
DATASET = "commercelake_gold"


def make_plan(tmp_path):
    files_by_table = {
        table: [(f"{table}.parquet", index)]
        for index, table in enumerate(LOAD_ORDER, start=1)
    }

    return PublicationPlan(
        project_id=PROJECT,
        dataset=DATASET,
        location="US",
        run_id=RUN_ID,
        run_directory=tmp_path,
        files_by_table=files_by_table,
    )


def make_prepared_files(tmp_path):
    prepared = {}

    for table in LOAD_ORDER:
        path = tmp_path / f"{table}.parquet"
        path.write_bytes(b"mock-parquet")
        prepared[table] = path

    return prepared


def make_metadata(table, rows, schema=None):
    if schema is None:
        schema = expected_bigquery_schema(table)

    return json.dumps(
        {
            "numRows": str(rows),
            "schema": {"fields": schema},
        }
    )


def table_from_command(command):
    """Identify the Gold table referenced by a staging command."""
    for table in LOAD_ORDER:
        stage = f"_stage_{table}_{RUN_ID[:8]}"

        if any(stage in part for part in command):
            return table

    raise AssertionError(f"Unexpected staging command: {command}")


def command_kind(command):
    if "show" in command:
        return "show"

    if "mk" in command:
        return "create"

    if "load" in command:
        return "load"

    raise AssertionError(f"Unexpected command: {command}")


class FakeBigQuery:
    """Simulate staging existence, creation, loading and metadata."""

    def __init__(self):
        self.commands = []
        self.created = set()
        self.loaded = set()
        self.fail_kind = None
        self.fail_table = None
        self.fail_returncode = False
        self.wrong_schema_table = None
        self.wrong_rows_table = None
        self.existing_table = None
        self.ambiguous_table = None

    def __call__(self, command):
        self.commands.append(command)
        table = table_from_command(command)
        kind = command_kind(command)

        if kind == "show" and table not in self.created:
            if table == self.existing_table:
                return subprocess.CompletedProcess(
                    command,
                    0,
                    make_metadata(table, 0),
                    "",
                )

            if table == self.ambiguous_table:
                return subprocess.CompletedProcess(
                    command,
                    1,
                    "",
                    "Access Denied",
                )

            return subprocess.CompletedProcess(
                command,
                1,
                "",
                f"Not found: Table {command[-1]}",
            )

        if kind == self.fail_kind and table == self.fail_table:
            if self.fail_returncode:
                return subprocess.CompletedProcess(
                    command,
                    1,
                    "",
                    "Simulated command failure",
                )

            raise subprocess.CalledProcessError(
                1,
                command,
                stderr="Simulated command failure",
            )

        if kind == "create":
            # Verify that the JSON schema exists when bq mk is invoked.
            schema_option = next(
                part for part in command if part.startswith("--schema=")
            )
            schema_path = Path(schema_option.split("=", 1)[1])

            assert schema_path.is_file()
            assert json.loads(schema_path.read_text()) == (
                expected_bigquery_schema(table)
            )
            assert "--expiration=86400" in command

            self.created.add(table)

        if kind == "load":
            assert table in self.created
            assert "--replace_data" in command
            assert "--replace" not in command
            self.loaded.add(table)

        if kind == "show":
            rows = 0

            if table in self.loaded:
                rows = LOAD_ORDER.index(table) + 1

                if table == self.wrong_rows_table:
                    rows = 999

            schema = expected_bigquery_schema(table)

            if table == self.wrong_schema_table:
                schema[0] = {**schema[0], "type": "BOOLEAN"}

            stdout = make_metadata(table, rows, schema)
        else:
            stdout = ""

        return subprocess.CompletedProcess(
            command,
            0,
            stdout,
            "",
        )


def test_creates_loads_and_validates_all_six_tables(tmp_path):
    plan = make_plan(tmp_path)
    prepared = make_prepared_files(tmp_path)
    fake = FakeBigQuery()

    result = publish_staging_tables(plan, prepared, runner=fake)

    assert list(result) == list(LOAD_ORDER)
    assert list(result.values()) == list(range(1, 7))
    assert len(fake.commands) == 30

    # All six existence checks must happen before any creation.
    assert all(command_kind(command) == "show" for command in fake.commands[:6])

    for index, table in enumerate(LOAD_ORDER):
        offset = 6 + index * 4
        commands = fake.commands[offset : offset + 4]

        assert [command_kind(command) for command in commands] == [
            "create",
            "show",
            "load",
            "show",
        ]

        assert all(table_from_command(command) == table for command in commands)


def test_existing_staging_blocks_all_writes(tmp_path):
    fake = FakeBigQuery()
    fake.existing_table = LOAD_ORDER[2]

    with pytest.raises(FileExistsError, match="already exists"):
        publish_staging_tables(
            make_plan(tmp_path),
            make_prepared_files(tmp_path),
            runner=fake,
        )

    assert all(command_kind(command) == "show" for command in fake.commands)


def test_ambiguous_existence_blocks_all_writes(tmp_path):
    fake = FakeBigQuery()
    fake.ambiguous_table = LOAD_ORDER[1]

    with pytest.raises(RuntimeError, match="Cannot verify staging"):
        publish_staging_tables(
            make_plan(tmp_path),
            make_prepared_files(tmp_path),
            runner=fake,
        )

    assert all(command_kind(command) == "show" for command in fake.commands)


@pytest.mark.parametrize("kind", ["create", "load"])
@pytest.mark.parametrize("returncode", [False, True])
def test_create_or_load_failure_stops_publication(
    tmp_path,
    kind,
    returncode,
):
    fake = FakeBigQuery()
    fake.fail_kind = kind
    fake.fail_table = LOAD_ORDER[0]
    fake.fail_returncode = returncode

    with pytest.raises(subprocess.CalledProcessError):
        publish_staging_tables(
            make_plan(tmp_path),
            make_prepared_files(tmp_path),
            runner=fake,
        )

    assert len(fake.commands) == (7 if kind == "create" else 9)
    assert LOAD_ORDER[1] not in fake.created


def test_row_count_mismatch_stops_publication(tmp_path):
    fake = FakeBigQuery()
    fake.wrong_rows_table = LOAD_ORDER[0]

    with pytest.raises(ValueError, match="row count mismatch"):
        publish_staging_tables(
            make_plan(tmp_path),
            make_prepared_files(tmp_path),
            runner=fake,
        )

    assert len(fake.commands) == 10
    assert LOAD_ORDER[1] not in fake.created


def test_schema_mismatch_stops_before_loading(tmp_path):
    fake = FakeBigQuery()
    fake.wrong_schema_table = LOAD_ORDER[0]

    with pytest.raises(ValueError, match="schema mismatch"):
        publish_staging_tables(
            make_plan(tmp_path),
            make_prepared_files(tmp_path),
            runner=fake,
        )

    assert len(fake.commands) == 8
    assert not fake.loaded


def test_missing_prepared_table_blocks_cloud_commands(tmp_path):
    prepared = make_prepared_files(tmp_path)
    prepared.pop("fact_sales")
    commands = []

    with pytest.raises(ValueError, match="six Gold tables"):
        publish_staging_tables(
            make_plan(tmp_path),
            prepared,
            runner=lambda command: commands.append(command),
        )

    assert not commands


def test_missing_local_file_blocks_cloud_commands(tmp_path):
    prepared = make_prepared_files(tmp_path)
    prepared["fact_sales"].unlink()
    commands = []

    with pytest.raises(FileNotFoundError, match="Missing prepared"):
        publish_staging_tables(
            make_plan(tmp_path),
            prepared,
            runner=lambda command: commands.append(command),
        )

    assert not commands


def test_invalid_location_blocks_cloud_commands(tmp_path):
    plan = make_plan(tmp_path)
    plan = PublicationPlan(
        project_id=plan.project_id,
        dataset=plan.dataset,
        location="EU",
        run_id=plan.run_id,
        run_directory=plan.run_directory,
        files_by_table=plan.files_by_table,
    )
    commands = []

    with pytest.raises(ValueError, match="location"):
        publish_staging_tables(
            plan,
            make_prepared_files(tmp_path),
            runner=lambda command: commands.append(command),
        )

    assert not commands


def test_invalid_runner_response_stops_after_preflight(tmp_path):
    fake = FakeBigQuery()

    def runner(command):
        if "mk" in command:
            return {"returncode": 0}

        return fake(command)

    with pytest.raises(TypeError, match="CompletedProcess"):
        publish_staging_tables(
            make_plan(tmp_path),
            make_prepared_files(tmp_path),
            runner=runner,
        )

    assert len(fake.commands) == 6


def test_missing_runner_is_rejected(tmp_path):
    with pytest.raises(TypeError, match="callable runner"):
        publish_staging_tables(
            make_plan(tmp_path),
            make_prepared_files(tmp_path),
            runner=None,
        )
