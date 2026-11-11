"""Orchestrate guarded BigQuery Gold version publication."""

from collections.abc import Callable
from dataclasses import dataclass
from subprocess import CompletedProcess

from commercelake.cloud.bigquery_destination_guard import (
    check_versioned_destinations,
)
from commercelake.cloud.bigquery_promotion import PromotionPlan
from commercelake.cloud.bigquery_promotion_preflight import (
    validate_promotion_plan,
    validate_staging_preflight,
)
from commercelake.cloud.bigquery_schema_validation import (
    validate_bigquery_schema,
)
from commercelake.cloud.bigquery_validation import (
    build_show_command,
    validate_table_row_count,
)
from commercelake.cloud.bigquery_versioned_copy import (
    build_versioned_copy_plan,
)

CommandRunner = Callable[[list[str]], CompletedProcess[str]]


@dataclass(frozen=True)
class VersionPublicationResult:
    """Successful publication and validation of one complete Gold version."""

    run_id: str
    published_tables: tuple[str, ...]
    total_rows: int


class VersionPublicationError(RuntimeError):
    """Publication failed; versioned tables may require inspection."""

    def __init__(
        self,
        message: str,
        *,
        completed_copies: tuple[str, ...],
        uncertain_destination: str | None = None,
        phase: str = "unknown",
    ) -> None:
        super().__init__(message)
        self.completed_copies = completed_copies
        self.uncertain_destination = uncertain_destination
        self.phase = phase


def publish_gold_version(
    plan: PromotionPlan,
    *,
    runner: CommandRunner,
) -> VersionPublicationResult:
    """Publish and verify Gold versions without activating consumers.

    All operations use an explicitly injected runner.
    No table is deleted or activated automatically.
    """
    validate_promotion_plan(plan)

    if not callable(runner):
        raise TypeError("An explicit callable runner is required")

    validate_staging_preflight(plan, runner=runner)
    check_versioned_destinations(plan, runner=runner)

    copy_plan = build_versioned_copy_plan(plan)

    completed = []
    uncertain_destination = None
    phase = "copy"

    try:
        for item in copy_plan:
            uncertain_destination = item.destination_table
            command = list(item.command)
            result = runner(command)

            if not isinstance(result, CompletedProcess):
                raise TypeError("Copy runner must return a CompletedProcess")

            if result.returncode != 0:
                raise RuntimeError(f"BigQuery copy failed for {item.destination_table}")

            completed.append(item.destination_table)
            uncertain_destination = None

        phase = "verification"

        for table in plan.tables:
            uncertain_destination = table.version_name

            command = build_show_command(
                project_id=plan.project_id,
                dataset=plan.dataset,
                table=table.version_name,
                location=plan.location,
            )
            metadata = runner(command)

            if not isinstance(metadata, CompletedProcess):
                raise TypeError("Metadata runner must return a CompletedProcess")

            if metadata.returncode != 0:
                raise RuntimeError(
                    f"Cannot verify published table: {table.version_name}"
                )

            validate_table_row_count(
                table=table.version_name,
                expected_rows=table.expected_rows,
                metadata_output=metadata.stdout,
            )

            validate_bigquery_schema(
                table=table.logical_name,
                metadata_output=metadata.stdout,
            )

            uncertain_destination = None

    except Exception as exc:
        raise VersionPublicationError(
            "Gold version publication failed; "
            "inspect versioned destinations before retrying",
            completed_copies=tuple(completed),
            uncertain_destination=uncertain_destination,
            phase=phase,
        ) from exc

    return VersionPublicationResult(
        run_id=plan.run_id,
        published_tables=tuple(completed),
        total_rows=plan.total_rows,
    )
