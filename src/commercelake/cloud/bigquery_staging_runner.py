"""Coordinate BigQuery staging loads and row-count reconciliation."""

from collections.abc import Callable
from pathlib import Path
from subprocess import CompletedProcess

from commercelake.cloud.bigquery_cli import run_command
from commercelake.cloud.bigquery_publisher import PublicationPlan
from commercelake.cloud.bigquery_staging import (
    build_staging_load_command,
    staging_table_name,
)
from commercelake.cloud.bigquery_validation import (
    build_show_command,
    validate_table_row_count,
)
from commercelake.warehouse.loader import LOAD_ORDER, safe_file

CommandRunner = Callable[[list[str]], CompletedProcess[str]]


def publish_staging_tables(
    plan: PublicationPlan,
    prepared_files: dict[str, Path],
    runner: CommandRunner = run_command,
) -> dict[str, int]:
    """Load and reconcile six staging tables, stopping on any failure."""
    if set(prepared_files) != set(LOAD_ORDER):
        raise ValueError("Prepared files must match all six Gold tables")

    if set(plan.files_by_table) != set(LOAD_ORDER):
        raise ValueError("Publication plan must contain all six Gold tables")

    expected_counts = {
        table: sum(rows for _, rows in plan.files_by_table[table])
        for table in LOAD_ORDER
    }

    # Validate every local input before executing the first cloud command.
    resolved_files = {}

    for table in LOAD_ORDER:
        path = Path(prepared_files[table]).resolve()

        if not path.is_file():
            raise FileNotFoundError(f"Missing prepared Parquet: {path}")

        if path.suffix.lower() != ".parquet":
            raise ValueError(f"Invalid prepared Parquet: {path}")

        resolved_files[table] = path

    results = {}

    for table in LOAD_ORDER:
        stage = staging_table_name(table, plan.run_id)

        load_command = build_staging_load_command(
            project_id=plan.project_id,
            dataset=plan.dataset,
            table=table,
            run_id=plan.run_id,
            source_path=resolved_files[table],
            location=plan.location,
        )

        print(f"Loading staging table: {stage}", flush=True)
        runner(load_command)

        show_command = build_show_command(
            project_id=plan.project_id,
            dataset=plan.dataset,
            table=stage,
            location=plan.location,
        )

        metadata = runner(show_command)

        actual_rows = validate_table_row_count(
            table=stage,
            expected_rows=expected_counts[table],
            metadata_output=metadata.stdout,
        )

        results[table] = actual_rows
        print(f"PASS: {stage} ({actual_rows:,} rows)", flush=True)

    return results


def resolve_single_file_tables(
    plan: PublicationPlan,
) -> dict[str, Path]:
    """Resolve Gold tables already represented by one Parquet file."""
    prepared = {}

    for table in LOAD_ORDER:
        files = plan.files_by_table[table]

        if len(files) != 1:
            continue

        relative_path, _ = files[0]
        prepared[table] = safe_file(
            plan.run_directory,
            relative_path,
        )

    return prepared
