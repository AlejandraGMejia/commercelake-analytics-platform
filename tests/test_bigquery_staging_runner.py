"""Tests for fail-closed two-phase BigQuery staging publication."""

import json
import subprocess
from pathlib import Path

import pytest

from commercelake.cloud.bigquery_ingestion_schema import (
    expected_ingestion_schema,
)
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
BUDGET = 10485760
INGESTION = f"_ingest_fact_sales_{RUN_ID[:8]}"


def make_plan(tmp_path):
    return PublicationPlan(
        project_id=PROJECT,
        dataset=DATASET,
        location="US",
        run_id=RUN_ID,
        run_directory=tmp_path,
        files_by_table={
            table: [(f"{table}.parquet", index)]
            for index, table in enumerate(LOAD_ORDER, start=1)
        },
    )


def make_prepared_files(tmp_path):
    prepared = {}

    for table in LOAD_ORDER:
        path = tmp_path / f"{table}.parquet"
        path.write_bytes(b"mock-parquet")
        prepared[table] = path

    return prepared


def make_metadata(table, rows, *, ingestion=False, schema=None):
    if schema is None:
        schema = (
            expected_ingestion_schema(table)
            if ingestion
            else expected_bigquery_schema(table)
        )

    return json.dumps(
        {"numRows": str(rows), "schema": {"fields": schema}}
    )


def make_dry_run_response(
    *,
    estimate=1024,
    dry_run=True,
    budget=BUDGET,
    state="DONE",
    error=False,
):
    status = {"state": state}

    if error:
        status["errorResult"] = {
            "reason": "invalidQuery",
            "message": "Simulated dry-run failure",
        }

    return json.dumps(
        {
            "configuration": {
                "dryRun": dry_run,
                "jobType": "QUERY",
                "query": {
                    "maximumBytesBilled": str(budget),
                },
            },
            "status": status,
            "statistics": {
                "query": {
                    "totalBytesProcessed": str(estimate),
                    "totalBytesProcessedAccuracy": "PRECISE",
                },
            },
        }
    )


def command_kind(command):
    if "show" in command:
        return "show"
    if "mk" in command:
        return "create"
    if "load" in command:
        return "load"
    if "query" in command:
        return "dry_run" if "--dry_run" in command else "insert"

    raise AssertionError(f"Unexpected command: {command}")


def target_from_command(command):
    kind = command_kind(command)

    if kind in {"dry_run", "insert"}:
        return "fact_sales", False

    target = (
        command[-1]
        if kind in {"show", "create"}
        else command[-2]
    )

    if INGESTION in target:
        return "fact_sales", True

    for table in LOAD_ORDER:
        if f"_stage_{table}_{RUN_ID[:8]}" in target:
            return table, False

    raise AssertionError(f"Unexpected staging command: {command}")


class FakeBigQuery:
    """Simulate staging tables and a budget-controlled SQL dry run."""

    def __init__(self):
        self.commands = []
        self.created = set()
        self.loaded = set()

        self.fail_kind = None
        self.fail_target = None
        self.fail_returncode = False

        self.wrong_schema_target = None
        self.wrong_rows_target = None
        self.existing_target = None
        self.ambiguous_target = None

        self.dry_run_output = make_dry_run_response()
        self.dry_run_sql = None

    def __call__(self, command):
        self.commands.append(command)

        kind = command_kind(command)
        table, ingestion = target_from_command(command)
        target = (table, ingestion)

        if kind == "show" and target not in self.created:
            if target == self.existing_target:
                return subprocess.CompletedProcess(
                    command,
                    0,
                    make_metadata(table, 0, ingestion=ingestion),
                    "",
                )

            if target == self.ambiguous_target:
                return subprocess.CompletedProcess(
                    command, 1, "", "Access Denied"
                )

            return subprocess.CompletedProcess(
                command,
                1,
                "",
                f"Not found: Table {command[-1]}",
            )

        if kind == self.fail_kind and target == self.fail_target:
            if self.fail_returncode:
                return subprocess.CompletedProcess(
                    command, 1, "", "Simulated command failure"
                )

            raise subprocess.CalledProcessError(
                1,
                command,
                stderr="Simulated command failure",
            )

        if kind == "create":
            option = next(
                part for part in command if part.startswith("--schema=")
            )
            path = Path(option.split("=", 1)[1])

            assert path.is_file()
            assert "--expiration=86400" in command

            expected = (
                expected_ingestion_schema(table)
                if ingestion
                else expected_bigquery_schema(table)
            )

            assert json.loads(path.read_text()) == expected
            self.created.add(target)

        elif kind == "load":
            assert target in self.created
            assert "--replace_data" in command
            assert "--replace" not in command
            self.loaded.add(target)

        elif kind == "dry_run":
            assert target == ("fact_sales", False)
            assert ("fact_sales", True) in self.loaded
            assert target in self.created
            assert "--format=prettyjson" in command
            assert f"--maximum_bytes_billed={BUDGET}" in command

            self.dry_run_sql = command[-1]

            return subprocess.CompletedProcess(
                command,
                0,
                self.dry_run_output,
                "",
            )

        elif kind == "insert":
            assert target == ("fact_sales", False)
            assert ("fact_sales", True) in self.loaded
            assert target in self.created

            assert self.dry_run_sql is not None
            assert command[-1] == self.dry_run_sql
            assert "--dry_run" not in command
            assert f"--maximum_bytes_billed={BUDGET}" in command
            assert f"FROM `{PROJECT}.{DATASET}.{INGESTION}`" in command[-1]
            assert "DATETIME(`invoice_date`, 'UTC')" in command[-1]

            self.loaded.add(target)

        if kind == "show":
            rows = (
                LOAD_ORDER.index(table) + 1
                if target in self.loaded
                else 0
            )

            if target == self.wrong_rows_target and target in self.loaded:
                rows = 999

            schema = (
                expected_ingestion_schema(table)
                if ingestion
                else expected_bigquery_schema(table)
            )

            if target == self.wrong_schema_target:
                schema[0] = {**schema[0], "type": "BOOLEAN"}

            stdout = make_metadata(
                table,
                rows,
                ingestion=ingestion,
                schema=schema,
            )
        else:
            stdout = ""

        return subprocess.CompletedProcess(command, 0, stdout, "")


def publish(tmp_path, fake):
    return publish_staging_tables(
        make_plan(tmp_path),
        make_prepared_files(tmp_path),
        runner=fake,
        maximum_bytes_billed=BUDGET,
    )


def test_creates_loads_and_validates_all_six_tables(tmp_path):
    fake = FakeBigQuery()
    result = publish(tmp_path, fake)

    assert list(result) == list(LOAD_ORDER)
    assert list(result.values()) == list(range(1, 7))
    assert len(fake.commands) == 36

    kinds = [command_kind(command) for command in fake.commands]

    assert kinds[:7] == ["show"] * 7
    assert kinds.count("show") == 21
    assert kinds.count("create") == 7
    assert kinds.count("load") == 6
    assert kinds.count("dry_run") == 1
    assert kinds.count("insert") == 1

    assert kinds.index("dry_run") < kinds.index("insert")
    assert kinds[kinds.index("dry_run") + 1] == "insert"

    assert ("fact_sales", True) in fake.loaded
    assert ("fact_sales", False) in fake.loaded


@pytest.mark.parametrize(
    "target",
    [
        ("dim_product", False),
        ("fact_sales", False),
        ("fact_sales", True),
    ],
)
def test_existing_destination_blocks_all_writes(tmp_path, target):
    fake = FakeBigQuery()
    fake.existing_target = target

    with pytest.raises(FileExistsError, match="already exists"):
        publish(tmp_path, fake)

    assert all(
        command_kind(command) == "show"
        for command in fake.commands
    )


@pytest.mark.parametrize(
    "target",
    [
        ("fact_sales", False),
        ("fact_sales", True),
    ],
)
def test_ambiguous_existence_blocks_all_writes(tmp_path, target):
    fake = FakeBigQuery()
    fake.ambiguous_target = target

    with pytest.raises(RuntimeError, match="Cannot verify staging"):
        publish(tmp_path, fake)

    assert all(
        command_kind(command) == "show"
        for command in fake.commands
    )


@pytest.mark.parametrize(
    "kind",
    ["create", "load", "dry_run", "insert"],
)
@pytest.mark.parametrize("returncode", [False, True])
def test_fact_sales_command_failure_stops_publication(
    tmp_path,
    kind,
    returncode,
):
    fake = FakeBigQuery()
    fake.fail_kind = kind
    fake.fail_target = (
        "fact_sales",
        kind in {"create", "load"},
    )
    fake.fail_returncode = returncode

    with pytest.raises(subprocess.CalledProcessError):
        publish(tmp_path, fake)

    assert ("fact_sales", False) not in fake.loaded

    if kind == "dry_run":
        assert not any(
            command_kind(command) == "insert"
            for command in fake.commands
        )


@pytest.mark.parametrize(
    "target",
    [
        ("fact_sales", True),
        ("fact_sales", False),
    ],
)
def test_fact_sales_schema_mismatch_stops_publication(tmp_path, target):
    fake = FakeBigQuery()
    fake.wrong_schema_target = target

    with pytest.raises(ValueError, match="schema mismatch"):
        publish(tmp_path, fake)

    assert ("fact_sales", False) not in fake.loaded


@pytest.mark.parametrize(
    "target",
    [
        ("fact_sales", True),
        ("fact_sales", False),
    ],
)
def test_fact_sales_row_mismatch_stops_publication(tmp_path, target):
    fake = FakeBigQuery()
    fake.wrong_rows_target = target

    with pytest.raises(ValueError, match="row count mismatch"):
        publish(tmp_path, fake)


@pytest.mark.parametrize(
    "output",
    [
        "",
        "not json",
        "{}",
        "[]",
        make_dry_run_response(estimate=BUDGET + 1),
        make_dry_run_response(dry_run=False),
        make_dry_run_response(budget=BUDGET + 1),
        make_dry_run_response(state="RUNNING"),
        make_dry_run_response(error=True),
    ],
)
def test_invalid_dry_run_never_executes_insert(tmp_path, output):
    fake = FakeBigQuery()
    fake.dry_run_output = output

    with pytest.raises((ValueError, TypeError)):
        publish(tmp_path, fake)

    kinds = [command_kind(command) for command in fake.commands]

    assert kinds.count("dry_run") == 1
    assert kinds.count("insert") == 0
    assert ("fact_sales", False) not in fake.loaded


def test_missing_budget_blocks_all_cloud_commands(tmp_path):
    commands = []

    with pytest.raises(ValueError, match="maximum_bytes_billed"):
        publish_staging_tables(
            make_plan(tmp_path),
            make_prepared_files(tmp_path),
            runner=lambda command: commands.append(command),
        )

    assert not commands


@pytest.mark.parametrize(
    "budget",
    [0, True, 10485759, "10485760"],
)
def test_invalid_budget_blocks_all_cloud_commands(tmp_path, budget):
    commands = []

    with pytest.raises(ValueError, match="maximum_bytes_billed"):
        publish_staging_tables(
            make_plan(tmp_path),
            make_prepared_files(tmp_path),
            runner=lambda command: commands.append(command),
            maximum_bytes_billed=budget,
        )

    assert not commands


def test_missing_prepared_table_blocks_cloud_commands(tmp_path):
    prepared = make_prepared_files(tmp_path)
    prepared.pop("fact_sales")
    commands = []

    with pytest.raises(ValueError, match="six Gold tables"):
        publish_staging_tables(
            make_plan(tmp_path),
            prepared,
            runner=lambda command: commands.append(command),
            maximum_bytes_billed=BUDGET,
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
            maximum_bytes_billed=BUDGET,
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
            maximum_bytes_billed=BUDGET,
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
            maximum_bytes_billed=BUDGET,
        )

    assert len(fake.commands) == 7


def test_missing_runner_is_rejected(tmp_path):
    with pytest.raises(TypeError, match="callable runner"):
        publish_staging_tables(
            make_plan(tmp_path),
            make_prepared_files(tmp_path),
            runner=None,
            maximum_bytes_billed=BUDGET,
        )
