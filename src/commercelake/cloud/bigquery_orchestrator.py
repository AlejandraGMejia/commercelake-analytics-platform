"""Orchestrate validated Gold preparation and BigQuery staging."""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from subprocess import CompletedProcess
from typing import Literal

from commercelake.cloud.bigquery_cli import run_command
from commercelake.cloud.bigquery_csv_runner import publish_csv_staging_tables
from commercelake.cloud.bigquery_preparation import prepare_gold_files
from commercelake.cloud.bigquery_publisher import (
    DEFAULT_DATASET,
    DEFAULT_GOLD_ROOT,
    DEFAULT_LOCATION,
    PublicationPlan,
    build_plan,
)
from commercelake.cloud.bigquery_staging_insert import validate_query_budget
from commercelake.cloud.bigquery_staging_runner import publish_staging_tables
from commercelake.cloud.gold_financial_validation import validate_gold_financials
from commercelake.warehouse.loader import LOAD_ORDER

CommandRunner = Callable[[list[str]], CompletedProcess[str]]
LoadStrategy = Literal["parquet_insert", "csv_direct"]

ALLOWED_LOAD_STRATEGIES = frozenset({"parquet_insert", "csv_direct"})


@dataclass(frozen=True)
class StagingPublicationResult:
    """Result of a validated staging publication."""

    project_id: str
    dataset: str
    run_id: str
    table_row_counts: dict[str, int]

    @property
    def total_rows(self) -> int:
        """Return the sum of published table row counts."""
        return sum(self.table_row_counts.values())


def validate_load_strategy(strategy: str) -> LoadStrategy:
    """Reject unknown publication strategies before cloud operations."""
    if strategy == "parquet_insert":
        return "parquet_insert"

    if strategy == "csv_direct":
        return "csv_direct"

    raise ValueError(f"Unsupported BigQuery load strategy: {strategy}")


def execute_staging_publication(
    plan: PublicationPlan,
    *,
    runner: CommandRunner,
    maximum_bytes_billed: int | None = None,
    temporary_root: Path | None = None,
    load_strategy: LoadStrategy = "parquet_insert",
) -> StagingPublicationResult:
    """Validate Gold finances and publish isolated staging tables.

    WARNING: The supplied runner may perform real cloud writes.
    No staging tables are promoted to production by this function.
    """
    if not callable(runner):
        raise TypeError("An explicit callable runner is required")

    strategy = validate_load_strategy(load_strategy)
    budget = validate_query_budget(maximum_bytes_billed)

    if set(plan.files_by_table) != set(LOAD_ORDER):
        raise ValueError("Publication plan must contain all six Gold tables")

    with prepare_gold_files(
        plan,
        temporary_root=temporary_root,
    ) as prepared:
        financial_result = validate_gold_financials(
            fact_files=[prepared["fact_sales"]],
            mart_file=prepared["mart_monthly_sales"],
        )

        expected_fact_rows = sum(rows for _, rows in plan.files_by_table["fact_sales"])

        if financial_result["fact_rows"] != expected_fact_rows:
            raise ValueError("Financial preflight fact_sales row count mismatch")

        expected_mart_rows = sum(
            rows for _, rows in plan.files_by_table["mart_monthly_sales"]
        )

        if financial_result["monthly_periods"] != expected_mart_rows:
            raise ValueError("Financial preflight monthly period count mismatch")

        if strategy == "csv_direct":
            row_counts = publish_csv_staging_tables(
                plan=plan,
                prepared_files=prepared,
                runner=runner,
            )
        else:
            row_counts = publish_staging_tables(
                plan=plan,
                prepared_files=prepared,
                runner=runner,
                maximum_bytes_billed=budget,
            )

    expected_counts = {
        table: sum(rows for _, rows in plan.files_by_table[table])
        for table in LOAD_ORDER
    }

    if row_counts != expected_counts:
        raise ValueError("Staging publication reconciliation failed")

    return StagingPublicationResult(
        project_id=plan.project_id,
        dataset=plan.dataset,
        run_id=plan.run_id,
        table_row_counts=row_counts,
    )


def publish_gold_to_staging(
    project_id: str,
    *,
    gold_root: Path = DEFAULT_GOLD_ROOT,
    dataset: str = DEFAULT_DATASET,
    location: str = DEFAULT_LOCATION,
    temporary_root: Path | None = None,
    runner: CommandRunner,
    maximum_bytes_billed: int | None = None,
    load_strategy: LoadStrategy = "parquet_insert",
) -> StagingPublicationResult:
    """Build a Gold plan and execute validated staging publication."""
    if not callable(runner):
        raise TypeError("An explicit callable runner is required")

    strategy = validate_load_strategy(load_strategy)
    budget = validate_query_budget(maximum_bytes_billed)

    plan = build_plan(
        gold_root=gold_root,
        project_id=project_id,
        dataset=dataset,
        location=location,
    )

    if strategy == "csv_direct":
        return execute_staging_publication(
            plan,
            runner=runner,
            maximum_bytes_billed=budget,
            temporary_root=temporary_root,
            load_strategy=strategy,
        )

    return execute_staging_publication(
        plan,
        runner=runner,
        maximum_bytes_billed=budget,
        temporary_root=temporary_root,
    )


def production_bigquery_runner() -> CommandRunner:
    """Return the real CLI runner for explicitly authorized execution."""
    return run_command
