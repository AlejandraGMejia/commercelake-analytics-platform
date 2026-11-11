"""Conservative destination-existence checks for BigQuery Gold versions."""

from collections.abc import Callable
from dataclasses import dataclass
from subprocess import CalledProcessError, CompletedProcess

from commercelake.cloud.bigquery_cli_errors import (
    is_confirmed_missing,
    is_explicit_table_not_found,
)
from commercelake.cloud.bigquery_promotion import PromotionPlan
from commercelake.cloud.bigquery_promotion_preflight import (
    validate_promotion_plan,
)
from commercelake.cloud.bigquery_validation import build_show_command

__all__ = [
    "DestinationCheckResult",
    "check_versioned_destinations",
    "is_explicit_table_not_found",
]

CommandRunner = Callable[[list[str]], CompletedProcess[str]]


@dataclass(frozen=True)
class DestinationCheckResult:
    """Confirmation that all versioned destinations were absent."""

    run_id: str
    available_destinations: tuple[str, ...]


def check_versioned_destinations(
    plan: PromotionPlan,
    *,
    runner: CommandRunner,
) -> DestinationCheckResult:
    """Require proof that every version destination is absent.

    Only read-only metadata commands are executed. Unknown errors
    block publication.
    """
    validate_promotion_plan(plan)

    if not callable(runner):
        raise TypeError("An explicit callable runner is required")

    available = []

    for table in plan.tables:
        command = build_show_command(
            project_id=plan.project_id,
            dataset=plan.dataset,
            table=table.version_name,
            location=plan.location,
        )

        try:
            result = runner(command)

        except CalledProcessError as exc:
            if not is_confirmed_missing(
                exc.stdout,
                exc.stderr,
                project_id=plan.project_id,
                dataset=plan.dataset,
                table=table.version_name,
            ):
                raise RuntimeError(
                    f"Cannot verify destination: {table.version_name}"
                ) from exc

        else:
            if not isinstance(result, CompletedProcess):
                raise TypeError("Runner must return a CompletedProcess")

            if result.returncode == 0:
                raise FileExistsError(
                    f"BigQuery destination already exists: {table.version_name}"
                )

            if not is_confirmed_missing(
                result.stdout,
                result.stderr,
                project_id=plan.project_id,
                dataset=plan.dataset,
                table=table.version_name,
            ):
                raise RuntimeError(f"Cannot verify destination: {table.version_name}")

        available.append(table.version_name)

    return DestinationCheckResult(
        run_id=plan.run_id,
        available_destinations=tuple(available),
    )
