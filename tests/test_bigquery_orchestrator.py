"""Tests for the BigQuery staging publication orchestrator."""

import json
from decimal import Decimal
from pathlib import Path
from subprocess import CompletedProcess

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from gold_test_data import sample_value, write_gold_parquet

from commercelake.cloud.bigquery_orchestrator import (
    execute_staging_publication,
    production_bigquery_runner,
)
from commercelake.cloud.bigquery_publisher import PublicationPlan
from commercelake.cloud.bigquery_schema_validation import expected_bigquery_schema
from commercelake.gold.monthly_mart import MonthlySalesCollector
from commercelake.warehouse.loader import LOAD_ORDER, TABLE_SCHEMAS

RUN_ID = "591c8097f0554414be18648e3587d2b3"


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
        project_id="commercelake-analytics",
        dataset="commercelake_gold",
        location="US",
        run_id=RUN_ID,
        run_directory=run_directory,
        files_by_table=files_by_table,
    )


def test_orchestrator_prepares_and_validates_six_tables(tmp_path):
    plan = make_plan(tmp_path)
    commands = []
    temporary_root = tmp_path / "temporary"

    def fake_runner(command):
        commands.append(command)

        if "load" in command:
            source = Path(command[-1])
            assert source.exists()

            if "_stage_fact_sales_" in command[-2]:
                assert pq.read_metadata(source).num_rows == 3

            return CompletedProcess(command, 0, "", "")

        assert "show" in command
        table_name = command[-1].split(".")[-1]

        for table in LOAD_ORDER:
            if f"_stage_{table}_" in table_name:
                expected = 3 if table == "fact_sales" else 2

                metadata = {
                    "numRows": str(expected),
                    "schema": {
                        "fields": expected_bigquery_schema(table),
                    },
                }

                return CompletedProcess(
                    command,
                    0,
                    json.dumps(metadata),
                    "",
                )

        raise AssertionError("Unexpected staging table")

    result = execute_staging_publication(
        plan,
        runner=fake_runner,
        temporary_root=temporary_root,
    )

    assert len(commands) == 12
    assert set(result.table_row_counts) == set(LOAD_ORDER)
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
            temporary_root=temporary_root,
        )

    assert len(commands) == 1
    assert not list(temporary_root.rglob("*.parquet"))


def test_orchestrator_rejects_incomplete_plan(tmp_path):
    plan = make_plan(tmp_path)
    plan.files_by_table.pop("fact_sales")
    commands = []

    def fake_runner(command):
        commands.append(command)
        raise AssertionError("Cloud runner should not be called")

    with pytest.raises(ValueError, match="all six Gold tables"):
        execute_staging_publication(
            plan,
            runner=fake_runner,
        )

    assert commands == []


def test_orchestrator_blocks_financial_mismatch_before_cloud(tmp_path):
    """Financial corruption must prevent every BigQuery command."""
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
            temporary_root=temporary_root,
        )

    assert commands == []
    assert not list(temporary_root.rglob("*.parquet"))


def test_orchestrator_blocks_invalid_transaction_before_cloud(tmp_path):
    """Invalid financial semantics must stop publication."""
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
            temporary_root=temporary_root,
        )

    assert commands == []
    assert not list(temporary_root.rglob("*.parquet"))


def test_production_runner_is_explicit():
    runner = production_bigquery_runner()
    assert callable(runner)
