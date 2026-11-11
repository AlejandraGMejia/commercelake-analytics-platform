"""Tests for budget-controlled BigQuery staging orchestration."""

import json
from decimal import Decimal
from pathlib import Path
from subprocess import CompletedProcess

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from gold_test_data import sample_value, write_gold_parquet

from commercelake.cloud.bigquery_ingestion_schema import (
    expected_ingestion_schema,
)
from commercelake.cloud.bigquery_orchestrator import (
    execute_staging_publication,
    production_bigquery_runner,
    publish_gold_to_staging,
)
from commercelake.cloud.bigquery_publisher import PublicationPlan
from commercelake.cloud.bigquery_schema_validation import expected_bigquery_schema
from commercelake.gold.monthly_mart import MonthlySalesCollector
from commercelake.warehouse.loader import LOAD_ORDER, TABLE_SCHEMAS

RUN_ID = "591c8097f0554414be18648e3587d2b3"
PROJECT = "commercelake-analytics"
DATASET = "commercelake_gold"
BUDGET = 10485760
INGESTION = f"_ingest_fact_sales_{RUN_ID[:8]}"


def make_fact_table() -> pa.Table:
    """Create financially valid Gold transactions."""
    transactions = [
        {
            "date_key": 20200115,
            "transaction_type": "sale",
            "net_transaction_amount": Decimal("10.0000"),
            "customer_id": "customer_a",
        },
        {
            "date_key": 20200215,
            "transaction_type": "cancellation",
            "net_transaction_amount": Decimal("-2.0000"),
            "customer_id": "customer_b",
        },
        {
            "date_key": 20200216,
            "transaction_type": "sale",
            "net_transaction_amount": Decimal("5.0000"),
            "customer_id": "customer_b",
        },
    ]

    schema = TABLE_SCHEMAS["fact_sales"]
    arrays = []

    for field in schema:
        values = []

        for index, transaction in enumerate(transactions):
            if field.name in transaction:
                value = transaction[field.name]
            elif field.nullable:
                value = None
            else:
                value = sample_value(field.type, index)

            values.append(value)

        arrays.append(pa.array(values, type=field.type))

    return pa.Table.from_arrays(arrays, schema=schema)


def make_plan(tmp_path: Path) -> PublicationPlan:
    """Create six Gold tables with consistent financial facts and mart."""
    run_directory = tmp_path / "gold"
    run_directory.mkdir()

    files_by_table = {}
    facts = make_fact_table()

    for table in LOAD_ORDER:
        if table == "fact_sales":
            files_by_table[table] = [
                ("fact_sales_1.parquet", 2),
                ("fact_sales_2.parquet", 1),
            ]

            pq.write_table(
                facts.slice(0, 2),
                run_directory / "fact_sales_1.parquet",
            )
            pq.write_table(
                facts.slice(2, 1),
                run_directory / "fact_sales_2.parquet",
            )

        elif table == "mart_monthly_sales":
            collector = MonthlySalesCollector()

            for batch in facts.to_batches():
                collector.consume(batch)

            mart = collector.build()
            collector.validate(mart)

            filename = "mart_monthly_sales.parquet"
            pq.write_table(mart, run_directory / filename)

            files_by_table[table] = [
                (filename, mart.num_rows),
            ]

        else:
            filename = f"{table}.parquet"

            write_gold_parquet(
                run_directory / filename,
                table,
                2,
            )

            files_by_table[table] = [
                (filename, 2),
            ]

    return PublicationPlan(
        project_id=PROJECT,
        dataset=DATASET,
        location="US",
        run_id=RUN_ID,
        run_directory=run_directory,
        files_by_table=files_by_table,
    )


def command_kind(command):
    """Distinguish dry-run validation from the actual INSERT."""
    if "show" in command:
        return "show"
    if "mk" in command:
        return "create"
    if "load" in command:
        return "load"
    if "query" in command:
        return "dry_run" if "--dry_run" in command else "insert"

    raise AssertionError(f"Unexpected command: {command}")


def identify_target(command):
    """Identify the exact temporary table referenced by a CLI command."""
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
        stage = f"_stage_{table}_{RUN_ID[:8]}"

        if stage in target:
            return table, False

    raise AssertionError(f"Unexpected staging command: {command}")


def make_dry_run_response():
    """Reproduce the observed successful BigQuery CLI JSON structure."""
    return json.dumps(
        {
            "configuration": {
                "dryRun": True,
                "jobType": "QUERY",
                "query": {
                    "maximumBytesBilled": str(BUDGET),
                },
            },
            "status": {
                "state": "DONE",
            },
            "statistics": {
                "query": {
                    "totalBytesProcessed": "1024",
                    "totalBytesProcessedAccuracy": "PRECISE",
                },
            },
        }
    )


def test_orchestrator_prepares_and_validates_six_tables(tmp_path):
    """Simulate the complete two-phase lifecycle without cloud writes."""
    plan = make_plan(tmp_path)
    commands = []
    temporary_root = tmp_path / "temporary"

    created = set()
    loaded = set()
    dry_run_sql = None

    expected_counts = {
        table: sum(rows for _, rows in plan.files_by_table[table])
        for table in LOAD_ORDER
    }

    def fake_runner(command):
        nonlocal dry_run_sql

        commands.append(command)
        table, ingestion = identify_target(command)
        target = (table, ingestion)
        kind = command_kind(command)

        if kind == "show":
            if target not in created:
                return CompletedProcess(
                    command,
                    1,
                    "",
                    f"Not found: Table {command[-1]}",
                )

            rows = expected_counts[table] if target in loaded else 0
            schema = (
                expected_ingestion_schema(table)
                if ingestion
                else expected_bigquery_schema(table)
            )

            metadata = {
                "numRows": str(rows),
                "schema": {"fields": schema},
            }

            return CompletedProcess(
                command,
                0,
                json.dumps(metadata),
                "",
            )

        if kind == "create":
            assert target not in created

            schema_option = next(
                option
                for option in command
                if option.startswith("--schema=")
            )
            schema_path = Path(schema_option.split("=", 1)[1])

            assert schema_path.is_file()
            assert "--expiration=86400" in command

            expected_schema = (
                expected_ingestion_schema(table)
                if ingestion
                else expected_bigquery_schema(table)
            )

            assert json.loads(schema_path.read_text()) == expected_schema

            created.add(target)
            return CompletedProcess(command, 0, "", "")

        if kind == "load":
            assert target in created
            assert target not in loaded
            assert "--replace_data" in command
            assert "--replace" not in command

            source = Path(command[-1])
            assert source.exists()

            if table == "fact_sales":
                assert ingestion
                assert pq.read_metadata(source).num_rows == 3

            loaded.add(target)
            return CompletedProcess(command, 0, "", "")

        if kind == "dry_run":
            assert target == ("fact_sales", False)
            assert ("fact_sales", True) in loaded
            assert target in created
            assert f"--maximum_bytes_billed={BUDGET}" in command
            assert "--format=prettyjson" in command
            assert f"FROM `{PROJECT}.{DATASET}.{INGESTION}`" in command[-1]
            assert "DATETIME(`invoice_date`, 'UTC')" in command[-1]

            dry_run_sql = command[-1]

            return CompletedProcess(
                command,
                0,
                make_dry_run_response(),
                "",
            )

        if kind == "insert":
            assert target == ("fact_sales", False)
            assert ("fact_sales", True) in loaded
            assert target in created
            assert dry_run_sql is not None
            assert command[-1] == dry_run_sql
            assert "--dry_run" not in command
            assert f"--maximum_bytes_billed={BUDGET}" in command
            assert f"FROM `{PROJECT}.{DATASET}.{INGESTION}`" in command[-1]
            assert "DATETIME(`invoice_date`, 'UTC')" in command[-1]

            loaded.add(target)
            return CompletedProcess(command, 0, "", "")

        raise AssertionError(f"Unexpected BigQuery command: {command}")

    result = execute_staging_publication(
        plan,
        runner=fake_runner,
        maximum_bytes_billed=BUDGET,
        temporary_root=temporary_root,
    )

    assert len(commands) == 36
    assert all(command_kind(command) == "show" for command in commands[:7])

    kinds = [command_kind(command) for command in commands]
    assert kinds.count("show") == 21
    assert kinds.count("create") == 7
    assert kinds.count("load") == 6
    assert kinds.count("dry_run") == 1
    assert kinds.count("insert") == 1

    assert kinds.index("dry_run") < kinds.index("insert")
    assert kinds[kinds.index("dry_run") + 1] == "insert"

    assert created == {
        *((table, False) for table in LOAD_ORDER),
        ("fact_sales", True),
    }
    assert loaded == created

    assert result.table_row_counts == expected_counts
    assert result.table_row_counts["fact_sales"] == 3
    assert result.total_rows == 13
    assert not list(temporary_root.rglob("*.parquet"))


def test_orchestrator_stops_on_staging_failure(tmp_path):
    plan = make_plan(tmp_path)
    commands = []
    temporary_root = tmp_path / "temporary"

    def failing_runner(command):
        commands.append(command)
        raise RuntimeError("simulated BigQuery failure")

    with pytest.raises(RuntimeError, match="simulated BigQuery failure"):
        execute_staging_publication(
            plan,
            runner=failing_runner,
            maximum_bytes_billed=BUDGET,
            temporary_root=temporary_root,
        )

    assert len(commands) == 1
    assert not list(temporary_root.rglob("*.parquet"))


def test_orchestrator_rejects_incomplete_plan(tmp_path):
    plan = make_plan(tmp_path)
    plan.files_by_table.pop("fact_sales")
    commands = []

    def forbidden_runner(command):
        commands.append(command)
        raise AssertionError("Cloud runner should not be called")

    with pytest.raises(ValueError, match="all six Gold tables"):
        execute_staging_publication(
            plan,
            runner=forbidden_runner,
            maximum_bytes_billed=BUDGET,
        )

    assert not commands


def test_orchestrator_blocks_financial_mismatch_before_cloud(tmp_path):
    plan = make_plan(tmp_path)
    temporary_root = tmp_path / "temporary"

    mart_path = plan.run_directory / "mart_monthly_sales.parquet"
    rows = pq.read_table(mart_path).to_pylist()
    rows[0]["identified_customer_count"] += 1

    corrupted_mart = pa.Table.from_pylist(
        rows,
        schema=TABLE_SCHEMAS["mart_monthly_sales"],
    )
    pq.write_table(corrupted_mart, mart_path)

    commands = []

    def forbidden_runner(command):
        commands.append(command)
        raise AssertionError("Cloud runner should not be called")

    with pytest.raises(ValueError, match="Monthly reconciliation mismatch"):
        execute_staging_publication(
            plan,
            runner=forbidden_runner,
            maximum_bytes_billed=BUDGET,
            temporary_root=temporary_root,
        )

    assert not commands
    assert not list(temporary_root.rglob("*.parquet"))


def test_orchestrator_blocks_invalid_transaction_before_cloud(tmp_path):
    plan = make_plan(tmp_path)
    temporary_root = tmp_path / "temporary"

    fact_path = plan.run_directory / "fact_sales_1.parquet"
    rows = pq.read_table(fact_path).to_pylist()
    rows[0]["transaction_type"] = "invalid_type"

    corrupted_facts = pa.Table.from_pylist(
        rows,
        schema=TABLE_SCHEMAS["fact_sales"],
    )
    pq.write_table(corrupted_facts, fact_path)

    commands = []

    def forbidden_runner(command):
        commands.append(command)
        raise AssertionError("Cloud runner should not be called")

    with pytest.raises(ValueError):
        execute_staging_publication(
            plan,
            runner=forbidden_runner,
            maximum_bytes_billed=BUDGET,
            temporary_root=temporary_root,
        )

    assert not commands
    assert not list(temporary_root.rglob("*.parquet"))


def test_missing_budget_blocks_cloud_and_preparation(tmp_path):
    plan = make_plan(tmp_path)
    commands = []
    temporary_root = tmp_path / "temporary"

    with pytest.raises(ValueError, match="maximum_bytes_billed"):
        execute_staging_publication(
            plan,
            runner=lambda command: commands.append(command),
            temporary_root=temporary_root,
        )

    assert not commands
    assert not temporary_root.exists()


@pytest.mark.parametrize("budget", [0, True, 10485759, "10485760"])
def test_invalid_budget_blocks_cloud_and_preparation(tmp_path, budget):
    plan = make_plan(tmp_path)
    commands = []
    temporary_root = tmp_path / "temporary"

    with pytest.raises(ValueError, match="maximum_bytes_billed"):
        execute_staging_publication(
            plan,
            runner=lambda command: commands.append(command),
            maximum_bytes_billed=budget,
            temporary_root=temporary_root,
        )

    assert not commands
    assert not temporary_root.exists()


def test_public_entrypoint_rejects_missing_budget_before_plan_build():
    commands = []

    with pytest.raises(ValueError, match="maximum_bytes_billed"):
        publish_gold_to_staging(
            PROJECT,
            runner=lambda command: commands.append(command),
        )

    assert not commands


def test_production_runner_is_explicit():
    assert callable(production_bigquery_runner())
