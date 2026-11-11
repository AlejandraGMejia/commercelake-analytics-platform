"""Integration tests for direct-CSV BigQuery staging publication."""

import json
import subprocess
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest

from commercelake.cloud.bigquery_csv_runner import publish_csv_staging_tables
from commercelake.cloud.bigquery_publisher import PublicationPlan
from commercelake.cloud.bigquery_schema_validation import expected_bigquery_schema
from commercelake.warehouse.loader import LOAD_ORDER

RUN_ID = "591c8097f0554414be18648e3587d2b3"
PROJECT = "commercelake-analytics"
DATASET = "commercelake_gold"


def make_inputs(tmp_path):
    inventory = {}
    prepared = {}

    for index, table in enumerate(LOAD_ORDER, start=1):
        source = tmp_path / f"{table}.parquet"
        source.write_bytes(b"mock-parquet")

        inventory[table] = [(source.name, index)]
        prepared[table] = source

    plan = PublicationPlan(
        project_id=PROJECT,
        dataset=DATASET,
        location="US",
        run_id=RUN_ID,
        run_directory=tmp_path,
        files_by_table=inventory,
    )

    return plan, prepared


def identify(command):
    if "show" in command:
        kind = "show"
        destination = command[-1]
    elif "mk" in command:
        kind = "create"
        destination = command[-1]
    elif "load" in command:
        kind = "load"
        destination = command[-2]
    else:
        raise AssertionError(f"Unexpected command: {command}")

    for table in LOAD_ORDER:
        if f"_stage_{table}_{RUN_ID[:8]}" in destination:
            return kind, table

    raise AssertionError(f"Unexpected destination: {destination}")


class FakeBigQuery:
    """Simulate only the six permitted staging destinations."""

    def __init__(self):
        self.commands = []
        self.created = set()
        self.loaded = set()
        self.existing = None
        self.wrong_rows = None
        self.wrong_schema = None
        self.fail_load = None

    def __call__(self, command):
        self.commands.append(command)
        kind, table = identify(command)

        if kind == "show" and table not in self.created:
            if table == self.existing:
                return subprocess.CompletedProcess(
                    command,
                    0,
                    json.dumps(
                        {
                            "numRows": "0",
                            "schema": {"fields": expected_bigquery_schema(table)},
                        }
                    ),
                    "",
                )

            return subprocess.CompletedProcess(
                command,
                1,
                "",
                f"Not found: Table {command[-1]}",
            )

        if kind == "create":
            assert "--expiration=86400" in command
            assert table not in self.created

            option = next(item for item in command if item.startswith("--schema="))
            schema_path = Path(option.split("=", 1)[1])

            assert json.loads(schema_path.read_text()) == (
                expected_bigquery_schema(table)
            )

            self.created.add(table)

        elif kind == "load":
            assert table in self.created

            if table == self.fail_load:
                return subprocess.CompletedProcess(
                    command, 1, "", "Simulated load failure"
                )

            if table == "fact_sales":
                assert "--source_format=CSV" in command
                assert "--noreplace" in command
                assert "--allow_quoted_newlines=true" in command
                assert command[-1].endswith(".csv")
            else:
                assert "--source_format=PARQUET" in command
                assert command[-1].endswith(".parquet")

            self.loaded.add(table)

        if kind == "show":
            rows = LOAD_ORDER.index(table) + 1 if table in self.loaded else 0

            if table == self.wrong_rows and table in self.loaded:
                rows = 999

            schema = expected_bigquery_schema(table)

            if table == self.wrong_schema:
                schema[0] = {**schema[0], "type": "BOOLEAN"}

            output = json.dumps(
                {
                    "numRows": str(rows),
                    "schema": {"fields": schema},
                }
            )
        else:
            output = ""

        return subprocess.CompletedProcess(command, 0, output, "")


def fake_csv_export(plan, output_path):
    output_path.write_text("mock CSV\n", encoding="utf-8")
    return sum(rows for _, rows in plan.files_by_table["fact_sales"])


def publish(tmp_path, fake):
    plan, prepared = make_inputs(tmp_path)

    with patch(
        "commercelake.cloud.bigquery_csv_runner.prepare_fact_sales_csv",
        side_effect=fake_csv_export,
    ):
        return publish_csv_staging_tables(plan, prepared, runner=fake)


def test_publishes_six_staging_tables_without_sql(tmp_path):
    fake = FakeBigQuery()
    result = publish(tmp_path, fake)

    assert list(result) == list(LOAD_ORDER)
    assert list(result.values()) == list(range(1, 7))

    kinds = [identify(command)[0] for command in fake.commands]

    assert kinds.count("show") == 18
    assert kinds.count("create") == 6
    assert kinds.count("load") == 6

    assert len(fake.commands) == 30
    assert fake.loaded == set(LOAD_ORDER)

    assert not any("query" in command for command in fake.commands)


def test_existing_staging_table_blocks_all_writes(tmp_path):
    fake = FakeBigQuery()
    fake.existing = "fact_sales"

    with pytest.raises(FileExistsError, match="already exists"):
        publish(tmp_path, fake)

    assert all(identify(command)[0] == "show" for command in fake.commands)


def test_csv_preparation_failure_blocks_cloud_commands(tmp_path):
    plan, prepared = make_inputs(tmp_path)
    fake = FakeBigQuery()

    with (
        patch(
            "commercelake.cloud.bigquery_csv_runner.prepare_fact_sales_csv",
            side_effect=ValueError("Invalid CSV source"),
        ),
        pytest.raises(ValueError, match="Invalid CSV source"),
    ):
        publish_csv_staging_tables(plan, prepared, runner=fake)

    assert fake.commands == []


def test_csv_row_mismatch_blocks_cloud_commands(tmp_path):
    plan, prepared = make_inputs(tmp_path)
    fake = FakeBigQuery()

    with (
        patch(
            "commercelake.cloud.bigquery_csv_runner.prepare_fact_sales_csv",
            return_value=0,
        ),
        pytest.raises(ValueError, match="row count mismatch"),
    ):
        publish_csv_staging_tables(plan, prepared, runner=fake)

    assert fake.commands == []


def test_load_failure_stops_publication(tmp_path):
    fake = FakeBigQuery()
    fake.fail_load = "fact_sales"

    with pytest.raises(subprocess.CalledProcessError):
        publish(tmp_path, fake)

    assert "fact_sales" not in fake.loaded


def test_wrong_row_count_stops_publication(tmp_path):
    fake = FakeBigQuery()
    fake.wrong_rows = "fact_sales"

    with pytest.raises(ValueError, match="row count mismatch"):
        publish(tmp_path, fake)


def test_wrong_schema_stops_publication(tmp_path):
    fake = FakeBigQuery()
    fake.wrong_schema = "fact_sales"

    with pytest.raises(ValueError, match="schema mismatch"):
        publish(tmp_path, fake)


def test_rejects_missing_prepared_table_before_cloud(tmp_path):
    plan, prepared = make_inputs(tmp_path)
    prepared.pop("fact_sales")
    fake = FakeBigQuery()

    with pytest.raises(ValueError, match="six Gold tables"):
        publish_csv_staging_tables(plan, prepared, runner=fake)

    assert fake.commands == []


def test_rejects_invalid_location_before_cloud(tmp_path):
    plan, prepared = make_inputs(tmp_path)
    plan = replace(plan, location="EU")
    fake = FakeBigQuery()

    with pytest.raises(ValueError, match="location"):
        publish_csv_staging_tables(plan, prepared, runner=fake)

    assert fake.commands == []
