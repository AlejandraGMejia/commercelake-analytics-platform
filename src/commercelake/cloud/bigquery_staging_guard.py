"""Fail-closed existence checks for BigQuery staging tables."""

from collections.abc import Callable
from subprocess import CalledProcessError, CompletedProcess

from commercelake.cloud.bigquery_cli_errors import is_confirmed_missing
from commercelake.cloud.bigquery_ingestion_staging import (
    ingestion_staging_table_name,
)
from commercelake.cloud.bigquery_staging import staging_table_name
from commercelake.cloud.bigquery_validation import build_show_command

CommandRunner = Callable[[list[str]], CompletedProcess[str]]


def require_staging_table_absent(
    project_id: str,
    dataset: str,
    table: str,
    run_id: str,
    *,
    runner: CommandRunner,
    location: str = "US",
    ingestion: bool = False,
) -> str:
    """Require a staging destination to be confirmed absent."""
    if not callable(runner):
        raise TypeError("An explicit callable runner is required")

    if not isinstance(ingestion, bool):
        raise TypeError("ingestion must be a boolean")

    if ingestion and table != "fact_sales":
        raise ValueError("Ingestion staging is only supported for fact_sales")

    if ingestion:
        stage = ingestion_staging_table_name(table, run_id)
    else:
        stage = staging_table_name(table, run_id)

    command = build_show_command(
        project_id=project_id,
        dataset=dataset,
        table=stage,
        location=location,
    )

    try:
        result = runner(command)
    except CalledProcessError as exc:
        if is_confirmed_missing(
            exc.stdout,
            exc.stderr,
            project_id=project_id,
            dataset=dataset,
            table=stage,
        ):
            return stage

        raise RuntimeError(
            f"Cannot verify staging destination: {stage}"
        ) from exc

    if not isinstance(result, CompletedProcess):
        raise TypeError("Runner must return a CompletedProcess")

    if result.returncode == 0:
        raise FileExistsError(
            f"BigQuery staging table already exists: {stage}"
        )

    if not is_confirmed_missing(
        result.stdout,
        result.stderr,
        project_id=project_id,
        dataset=dataset,
        table=stage,
    ):
        raise RuntimeError(
            f"Cannot verify staging destination: {stage}"
        )

    return stage
