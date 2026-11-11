"""Coordinate fail-closed BigQuery staging publication."""

import json
from collections.abc import Callable
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess
from tempfile import TemporaryDirectory

from commercelake.cloud.bigquery_cli import run_command
from commercelake.cloud.bigquery_ingestion_schema import (
    expected_ingestion_schema,
)
from commercelake.cloud.bigquery_ingestion_staging import (
    ingestion_staging_table_name,
)
from commercelake.cloud.bigquery_publisher import (
    PublicationPlan,
    validate_identifiers,
)
from commercelake.cloud.bigquery_schema_validation import (
    expected_bigquery_schema,
    parse_bigquery_schema,
    validate_bigquery_schema,
)
from commercelake.cloud.bigquery_staging import (
    build_staging_load_command,
    staging_table_name,
)
from commercelake.cloud.bigquery_staging_create import (
    build_staging_create_command,
)
from commercelake.cloud.bigquery_staging_dry_run import (
    build_fact_sales_insert_dry_run_command,
    validate_insert_dry_run,
)
from commercelake.cloud.bigquery_staging_guard import (
    require_staging_table_absent,
)
from commercelake.cloud.bigquery_staging_insert import (
    build_fact_sales_insert_command,
    validate_query_budget,
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


def _validate_metadata(
    *,
    table: str,
    stage: str,
    expected_rows: int,
    expected_schema: list[dict[str, str]],
    output: str,
) -> int:
    """Validate exact row count, column order, types and nullability."""
    rows = validate_table_row_count(
        table=stage,
        expected_rows=expected_rows,
        metadata_output=output,
    )

    actual_schema = parse_bigquery_schema(output)

    if actual_schema != expected_schema:
        raise ValueError(
            f"BigQuery schema mismatch for {stage}: "
            f"expected {expected_schema}, got {actual_schema}"
        )

    if expected_schema == expected_bigquery_schema(table):
        validate_bigquery_schema(table, output)

    return rows


def _run_validated_fact_sales_insert(
    *,
    plan: PublicationPlan,
    ingestion_stage: str,
    budget: int,
    runner: CommandRunner,
) -> int:
    """Require a successful exact-SQL dry run before executing INSERT."""
    insert_command = build_fact_sales_insert_command(
        project_id=plan.project_id,
        dataset=plan.dataset,
        run_id=plan.run_id,
        source_table=ingestion_stage,
        maximum_bytes_billed=budget,
        location=plan.location,
    )

    dry_run_command = build_fact_sales_insert_dry_run_command(
        project_id=plan.project_id,
        dataset=plan.dataset,
        run_id=plan.run_id,
        source_table=ingestion_stage,
        maximum_bytes_billed=budget,
        location=plan.location,
    )

    if dry_run_command[-1] != insert_command[-1]:
        raise ValueError("Dry-run SQL differs from execution SQL")

    print("Validating fact_sales INSERT with BigQuery dry run", flush=True)

    dry_run_result = _require_success(runner, dry_run_command)

    estimate = validate_insert_dry_run(
        dry_run_result.stdout,
        maximum_bytes_billed=budget,
    )

    print(
        f"PASS: fact_sales dry run "
        f"({estimate:,} estimated bytes; budget {budget:,})",
        flush=True,
    )

    _require_success(runner, insert_command)
    return estimate


def publish_staging_tables(
    plan: PublicationPlan,
    prepared_files: dict[str, Path],
    runner: CommandRunner = run_command,
    *,
    maximum_bytes_billed: int | None = None,
) -> dict[str, int]:
    """Publish six staging tables, using two phases for fact_sales.

    This function performs cloud writes with the real command runner.
    Tests must inject a simulated runner.
    """
    if not callable(runner):
        raise TypeError("An explicit callable runner is required")

    budget = validate_query_budget(maximum_bytes_billed)

    resolved_files, expected_counts = _validate_local_inputs(
        plan,
        prepared_files,
    )

    # Check every destination before the first write.
    for table in LOAD_ORDER:
        require_staging_table_absent(
            project_id=plan.project_id,
            dataset=plan.dataset,
            table=table,
            run_id=plan.run_id,
            location=plan.location,
            runner=runner,
        )

    require_staging_table_absent(
        project_id=plan.project_id,
        dataset=plan.dataset,
        table="fact_sales",
        run_id=plan.run_id,
        location=plan.location,
        runner=runner,
        ingestion=True,
    )

    results: dict[str, int] = {}

    with TemporaryDirectory(prefix="commercelake_staging_") as directory:
        schema_directory = Path(directory)

        create_commands: dict[str, list[str]] = {}
        load_commands: dict[str, list[str]] = {}
        show_commands: dict[str, list[str]] = {}

        for table in LOAD_ORDER:
            schema_path = schema_directory / f"{table}.json"
            schema_path.write_text(
                json.dumps(expected_bigquery_schema(table), indent=2),
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

            show_commands[table] = build_show_command(
                project_id=plan.project_id,
                dataset=plan.dataset,
                table=staging_table_name(table, plan.run_id),
                location=plan.location,
            )

            if table != "fact_sales":
                load_commands[table] = build_staging_load_command(
                    project_id=plan.project_id,
                    dataset=plan.dataset,
                    table=table,
                    run_id=plan.run_id,
                    source_path=resolved_files[table],
                    location=plan.location,
                )

        ingestion_stage = ingestion_staging_table_name(
            "fact_sales",
            plan.run_id,
        )

        ingestion_schema = expected_ingestion_schema("fact_sales")
        ingestion_schema_path = schema_directory / "fact_sales_ingestion.json"
        ingestion_schema_path.write_text(
            json.dumps(ingestion_schema, indent=2),
            encoding="utf-8",
        )

        ingestion_create = build_staging_create_command(
            project_id=plan.project_id,
            dataset=plan.dataset,
            table="fact_sales",
            run_id=plan.run_id,
            schema_path=ingestion_schema_path,
            location=plan.location,
            ingestion=True,
        )

        ingestion_load = build_staging_load_command(
            project_id=plan.project_id,
            dataset=plan.dataset,
            table="fact_sales",
            run_id=plan.run_id,
            source_path=resolved_files["fact_sales"],
            location=plan.location,
            ingestion=True,
        )

        ingestion_show = build_show_command(
            project_id=plan.project_id,
            dataset=plan.dataset,
            table=ingestion_stage,
            location=plan.location,
        )

        for table in LOAD_ORDER:
            stage = staging_table_name(table, plan.run_id)

            if table == "fact_sales":
                print(
                    f"Creating ingestion table: {ingestion_stage}",
                    flush=True,
                )
                _require_success(runner, ingestion_create)

                created_ingestion = _require_success(
                    runner,
                    ingestion_show,
                )
                _validate_metadata(
                    table=table,
                    stage=ingestion_stage,
                    expected_rows=0,
                    expected_schema=ingestion_schema,
                    output=created_ingestion.stdout,
                )

                print(
                    f"Loading ingestion table: {ingestion_stage}",
                    flush=True,
                )
                _require_success(runner, ingestion_load)

                loaded_ingestion = _require_success(
                    runner,
                    ingestion_show,
                )
                _validate_metadata(
                    table=table,
                    stage=ingestion_stage,
                    expected_rows=expected_counts[table],
                    expected_schema=ingestion_schema,
                    output=loaded_ingestion.stdout,
                )

            print(f"Creating staging table: {stage}", flush=True)
            _require_success(runner, create_commands[table])

            created = _require_success(runner, show_commands[table])
            _validate_metadata(
                table=table,
                stage=stage,
                expected_rows=0,
                expected_schema=expected_bigquery_schema(table),
                output=created.stdout,
            )

            if table == "fact_sales":
                _run_validated_fact_sales_insert(
                    plan=plan,
                    ingestion_stage=ingestion_stage,
                    budget=budget,
                    runner=runner,
                )
            else:
                print(f"Loading staging table: {stage}", flush=True)
                _require_success(runner, load_commands[table])

            loaded = _require_success(runner, show_commands[table])
            actual_rows = _validate_metadata(
                table=table,
                stage=stage,
                expected_rows=expected_counts[table],
                expected_schema=expected_bigquery_schema(table),
                output=loaded.stdout,
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
