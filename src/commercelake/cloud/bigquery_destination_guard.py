"""Conservative destination-existence checks for BigQuery Gold versions."""

import re
from collections.abc import Callable
from dataclasses import dataclass
from subprocess import CalledProcessError, CompletedProcess

from commercelake.cloud.bigquery_promotion import PromotionPlan
from commercelake.cloud.bigquery_promotion_preflight import (
    validate_promotion_plan,
)
from commercelake.cloud.bigquery_validation import build_show_command

CommandRunner = Callable[[list[str]], CompletedProcess[str]]


@dataclass(frozen=True)
class DestinationCheckResult:
    """Confirmation that all versioned destinations were absent."""

    run_id: str
    available_destinations: tuple[str, ...]


def is_explicit_table_not_found(
    error_output: str,
    *,
    project_id: str,
    dataset: str,
    table: str,
) -> bool:
    """Accept only an exact, unambiguous BigQuery table-not-found error."""
    if not isinstance(error_output, str):
        raise TypeError("BigQuery error output must be text")

    identifier = f"{project_id}:{dataset}.{table}"
    pattern = (
        r"Not found: Table "
        + re.escape(identifier)
        + r"(?: was not found in location [A-Za-z0-9_-]+)?"
    )

    return re.fullmatch(pattern, error_output.strip(), flags=re.IGNORECASE) is not None


def check_versioned_destinations(
    plan: PromotionPlan,
    *,
    runner: CommandRunner,
) -> DestinationCheckResult:
    """Require proof that every version destination is absent.

    This function executes only read-only `bq show` commands through
    the explicitly supplied runner. Unknown errors fail closed.
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
            error_output = exc.stderr

            if not is_explicit_table_not_found(
                error_output,
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

            if not is_explicit_table_not_found(
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
