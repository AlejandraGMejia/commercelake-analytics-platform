"""Publish six isolated BigQuery staging tables using direct CSV for fact_sales."""

import json
from collections.abc import Callable
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess
from tempfile import TemporaryDirectory

from commercelake.cloud.bigquery_cli import run_command
from commercelake.cloud.bigquery_csv_preparation import prepare_fact_sales_csv
from commercelake.cloud.bigquery_csv_staging import (
    build_fact_sales_csv_load_command,
)
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


def _run(runner: CommandRunner, command: list[str]) -> CompletedProcess[str]:
    result = runner(command)

    if not isinstance(result, CompletedProcess):
        raise TypeError("Runner must return a CompletedProcess")

    if result.returncode != 0:
        raise CalledProcessError(
            result.returncode,
            command,
            output=result.stdout,
            stderr=result.stderr,
        )

    return result


def publish_csv_staging_tables(
    plan: PublicationPlan,
    prepared_files: dict[str, Path],
    runner: CommandRunner = run_command,
) -> dict[str, int]:
    """Publish staging tables without SQL transformations.

    WARNING: Using the real runner performs cloud writes.
    Tests must inject a simulated runner.
    """
    if not callable(runner):
        raise TypeError("An explicit callable runner is required")

    validate_identifiers(plan.project_id, plan.dataset)

    if plan.location != "US":
        raise ValueError("Unsupported BigQuery location")

    if set(prepared_files) != set(LOAD_ORDER):
        raise ValueError("Prepared files must match all six Gold tables")

    if set(plan.files_by_table) != set(LOAD_ORDER):
        raise ValueError("Publication plan must contain all six Gold tables")

    staging_table_name("fact_sales", plan.run_id)

    expected_counts = {}
    resolved_files = {}

    for table in LOAD_ORDER:
        inventory = plan.files_by_table[table]

        if not inventory:
            raise ValueError(f"Missing Gold inventory: {table}")

        total = 0

        for relative_path, rows in inventory:
            if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
                raise ValueError(f"Invalid expected row count: {table}")

            safe_file(plan.run_directory, relative_path)
            total += rows

        source = Path(prepared_files[table]).resolve()

        if not source.is_file() or source.suffix.lower() != ".parquet":
            raise ValueError(f"Invalid prepared Parquet: {source}")

        resolved_files[table] = source
        expected_counts[table] = total

    results = {}

    with TemporaryDirectory(prefix="commercelake_csv_staging_") as directory:
        temporary_directory = Path(directory)
        csv_path = temporary_directory / "fact_sales.csv"

        exported_rows = prepare_fact_sales_csv(plan, csv_path)

        if exported_rows != expected_counts["fact_sales"]:
            raise ValueError("fact_sales CSV row count mismatch")

        # Complete local preparation before the first cloud command.
        for table in LOAD_ORDER:
            require_staging_table_absent(
                project_id=plan.project_id,
                dataset=plan.dataset,
                table=table,
                run_id=plan.run_id,
                location=plan.location,
                runner=runner,
            )

        for table in LOAD_ORDER:
            stage = staging_table_name(table, plan.run_id)
            schema_path = temporary_directory / f"{table}.json"

            schema_path.write_text(
                json.dumps(expected_bigquery_schema(table), indent=2),
                encoding="utf-8",
            )

            create_command = build_staging_create_command(
                project_id=plan.project_id,
                dataset=plan.dataset,
                table=table,
                run_id=plan.run_id,
                schema_path=schema_path,
                location=plan.location,
            )

            show_command = build_show_command(
                project_id=plan.project_id,
                dataset=plan.dataset,
                table=stage,
                location=plan.location,
            )

            if table == "fact_sales":
                load_command = build_fact_sales_csv_load_command(
                    project_id=plan.project_id,
                    dataset=plan.dataset,
                    run_id=plan.run_id,
                    source_path=csv_path,
                    location=plan.location,
                )
            else:
                load_command = build_staging_load_command(
                    project_id=plan.project_id,
                    dataset=plan.dataset,
                    table=table,
                    run_id=plan.run_id,
                    source_path=resolved_files[table],
                    location=plan.location,
                )

            print(f"Creating staging table: {stage}", flush=True)
            _run(runner, create_command)

            created = _run(runner, show_command)
            validate_table_row_count(
                table=stage,
                expected_rows=0,
                metadata_output=created.stdout,
            )
            validate_bigquery_schema(table, created.stdout)

            print(f"Loading staging table: {stage}", flush=True)
            _run(runner, load_command)

            loaded = _run(runner, show_command)
            actual_rows = validate_table_row_count(
                table=stage,
                expected_rows=expected_counts[table],
                metadata_output=loaded.stdout,
            )
            validate_bigquery_schema(table, loaded.stdout)

            results[table] = actual_rows
            print(
                f"PASS: {stage} ({actual_rows:,} rows; schema validated)",
                flush=True,
            )

    return results
