"""Coordinate fail-closed BigQuery staging publication."""

import json
from collections.abc import Callable
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess
from tempfile import TemporaryDirectory

from commercelake.cloud.bigquery_cli import run_command
from commercelake.cloud.bigquery_publisher import (
    PublicationPlan,
    validate_identifiers,
)
from commercelake.cloud.bigquery_schema_validation import (
    expected_bigquery_schema,
    validate_bigquery_schema,
)
from commercelake.cloud.bigquery_staging import (
    build_staging_load_command,
    staging_table_name,
)
from commercelake.cloud.bigquery_staging_create import (
    build_staging_create_command,
)
from commercelake.cloud.bigquery_staging_guard import (
    require_staging_table_absent,
)
from commercelake.cloud.bigquery_validation import (
    build_show_command,
    validate_table_row_count,
)
from commercelake.warehouse.loader import LOAD_ORDER, safe_file

CommandRunner = Callable[[list[str]], CompletedProcess[str]]


def _require_success(
    runner: CommandRunner,
    command: list[str],
) -> CompletedProcess[str]:
    """Require a successful, correctly typed CLI response."""
    result = runner(command)

    if not isinstance(result, CompletedProcess):
        raise TypeError("Runner must return a CompletedProcess")

    if result.returncode != 0:
        raise CalledProcessError(
            returncode=result.returncode,
            cmd=command,
            output=result.stdout,
            stderr=result.stderr,
        )

    return result


def _validate_local_inputs(
    plan: PublicationPlan,
    prepared_files: dict[str, Path],
) -> tuple[dict[str, Path], dict[str, int]]:
    """Validate every local input before contacting BigQuery."""
    if set(prepared_files) != set(LOAD_ORDER):
        raise ValueError("Prepared files must match all six Gold tables")

    if set(plan.files_by_table) != set(LOAD_ORDER):
        raise ValueError("Publication plan must contain all six Gold tables")

    validate_identifiers(plan.project_id, plan.dataset)

    if plan.location != "US":
        raise ValueError("Unsupported BigQuery location")

    # Validate the run ID before any remote operation.
    staging_table_name(LOAD_ORDER[0], plan.run_id)

    resolved_files: dict[str, Path] = {}
    expected_counts: dict[str, int] = {}

    for table in LOAD_ORDER:
        entries = plan.files_by_table[table]

        if not entries:
            raise ValueError(f"Missing Gold inventory: {table}")

        total = 0

        for relative_path, rows in entries:
            if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
                raise ValueError(f"Invalid expected row count: {table}")

            # Reject unsafe inventory paths, even when a prepared file exists.
            try:
                safe_file(plan.run_directory, relative_path)
            except FileNotFoundError as exc:
                raise FileNotFoundError(
                    f"Missing prepared Parquet source: {relative_path}"
                ) from exc

            total += rows

        path = Path(prepared_files[table]).resolve()

        if not path.is_file():
            raise FileNotFoundError(f"Missing prepared Parquet: {path}")

        if path.suffix.lower() != ".parquet":
            raise ValueError(f"Invalid prepared Parquet: {path}")

        resolved_files[table] = path
        expected_counts[table] = total

    return resolved_files, expected_counts


def publish_staging_tables(
    plan: PublicationPlan,
    prepared_files: dict[str, Path],
    runner: CommandRunner = run_command,
) -> dict[str, int]:
    """Create, load and validate all six staging tables.

    This function performs BigQuery writes when called with the real
    command runner. Tests must inject a simulated runner.
    """
    if not callable(runner):
        raise TypeError("An explicit callable runner is required")

    resolved_files, expected_counts = _validate_local_inputs(
        plan,
        prepared_files,
    )

    # Check ALL destinations before the first write.
    for table in LOAD_ORDER:
        require_staging_table_absent(
            project_id=plan.project_id,
            dataset=plan.dataset,
            table=table,
            run_id=plan.run_id,
            location=plan.location,
            runner=runner,
        )

    results: dict[str, int] = {}

    with TemporaryDirectory(prefix="commercelake_staging_") as directory:
        schema_directory = Path(directory)

        # Prepare and validate ALL creation commands locally first.
        create_commands: dict[str, list[str]] = {}
        load_commands: dict[str, list[str]] = {}
        show_commands: dict[str, list[str]] = {}

        for table in LOAD_ORDER:
            schema_path = schema_directory / f"{table}.json"
            schema_path.write_text(
                json.dumps(
                    expected_bigquery_schema(table),
                    indent=2,
                ),
                encoding="utf-8",
            )

            create_commands[table] = build_staging_create_command(
                project_id=plan.project_id,
                dataset=plan.dataset,
                table=table,
                run_id=plan.run_id,
                schema_path=schema_path,
                location=plan.location,
            )

            load_commands[table] = build_staging_load_command(
                project_id=plan.project_id,
                dataset=plan.dataset,
                table=table,
                run_id=plan.run_id,
                source_path=resolved_files[table],
                location=plan.location,
            )

            show_commands[table] = build_show_command(
                project_id=plan.project_id,
                dataset=plan.dataset,
                table=staging_table_name(table, plan.run_id),
                location=plan.location,
            )

        for table in LOAD_ORDER:
            stage = staging_table_name(table, plan.run_id)

            print(f"Creating staging table: {stage}", flush=True)
            _require_success(runner, create_commands[table])

            # A newly created staging table must be empty and have
            # exactly the expected schema BEFORE loading.
            created = _require_success(runner, show_commands[table])

            validate_table_row_count(
                table=stage,
                expected_rows=0,
                metadata_output=created.stdout,
            )

            validate_bigquery_schema(
                table=table,
                metadata_output=created.stdout,
            )

            print(f"Loading staging table: {stage}", flush=True)
            _require_success(runner, load_commands[table])

            loaded = _require_success(runner, show_commands[table])

            actual_rows = validate_table_row_count(
                table=stage,
                expected_rows=expected_counts[table],
                metadata_output=loaded.stdout,
            )

            validate_bigquery_schema(
                table=table,
                metadata_output=loaded.stdout,
            )

            results[table] = actual_rows

            print(
                f"PASS: {stage} ({actual_rows:,} rows; schema validated)",
                flush=True,
            )

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
