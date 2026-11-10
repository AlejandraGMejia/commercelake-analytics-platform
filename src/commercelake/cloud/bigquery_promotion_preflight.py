"""Read-only preflight validation for versioned BigQuery Gold promotion."""

from collections.abc import Callable
from dataclasses import dataclass
from subprocess import CompletedProcess

from commercelake.cloud.bigquery_promotion import (
    PromotionPlan,
    build_promotion_plan,
)
from commercelake.cloud.bigquery_schema_validation import (
    validate_bigquery_schema,
)
from commercelake.cloud.bigquery_validation import (
    build_show_command,
    validate_table_row_count,
)
from commercelake.warehouse.loader import LOAD_ORDER

CommandRunner = Callable[[list[str]], CompletedProcess[str]]


@dataclass(frozen=True)
class PromotionPreflightResult:
    """Successful validation of all staging tables for one Gold run."""

    run_id: str
    validated_tables: tuple[str, ...]
    table_row_counts: dict[str, int]

    @property
    def total_rows(self) -> int:
        """Return validated rows across the six Gold tables."""
        return sum(self.table_row_counts.values())


def validate_promotion_plan(plan: PromotionPlan) -> None:
    """Reject incomplete or altered promotion plans before cloud access."""
    if not isinstance(plan, PromotionPlan):
        raise TypeError("Expected a PromotionPlan")

    if len(plan.tables) != len(LOAD_ORDER):
        raise ValueError("Promotion plan must contain all six Gold tables")

    expected_counts = {table.logical_name: table.expected_rows for table in plan.tables}

    if len(expected_counts) != len(LOAD_ORDER):
        raise ValueError("Promotion plan contains duplicate Gold tables")

    canonical = build_promotion_plan(
        project_id=plan.project_id,
        dataset=plan.dataset,
        run_id=plan.run_id,
        expected_counts=expected_counts,
        location=plan.location,
    )

    if plan != canonical:
        raise ValueError("Promotion plan does not match canonical Gold layout")


def validate_staging_preflight(
    plan: PromotionPlan,
    *,
    runner: CommandRunner,
) -> PromotionPreflightResult:
    """Validate every staging table without modifying BigQuery.

    The injected runner receives only `bq show` commands. All six
    tables must pass row-count and schema checks before success.
    """
    validate_promotion_plan(plan)

    if not callable(runner):
        raise TypeError("An explicit callable runner is required")

    validated_tables = []
    row_counts = {}

    for table in plan.tables:
        command = build_show_command(
            project_id=plan.project_id,
            dataset=plan.dataset,
            table=table.staging_name,
            location=plan.location,
        )

        metadata = runner(command)

        if not isinstance(metadata, CompletedProcess):
            raise TypeError("Runner must return a CompletedProcess")

        if metadata.returncode != 0:
            raise RuntimeError(
                f"BigQuery metadata command failed for {table.staging_name}"
            )

        if not isinstance(metadata.stdout, str):
            raise TypeError("BigQuery metadata output must be text")

        rows = validate_table_row_count(
            table=table.staging_name,
            expected_rows=table.expected_rows,
            metadata_output=metadata.stdout,
        )

        validate_bigquery_schema(
            table=table.logical_name,
            metadata_output=metadata.stdout,
        )

        row_counts[table.logical_name] = rows
        validated_tables.append(table.logical_name)

    return PromotionPreflightResult(
        run_id=plan.run_id,
        validated_tables=tuple(validated_tables),
        table_row_counts=row_counts,
    )
