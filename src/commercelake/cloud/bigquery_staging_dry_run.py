"""Build and validate budget-controlled BigQuery INSERT dry runs."""

import json

from commercelake.cloud.bigquery_staging_insert import (
    build_fact_sales_insert_command,
    validate_query_budget,
)


def build_fact_sales_insert_dry_run_command(
    project_id: str,
    dataset: str,
    run_id: str,
    *,
    source_table: str,
    maximum_bytes_billed: int,
    location: str = "US",
) -> list[str]:
    """Build a dry run using the exact production INSERT SQL."""
    command = build_fact_sales_insert_command(
        project_id,
        dataset,
        run_id,
        source_table=source_table,
        maximum_bytes_billed=maximum_bytes_billed,
        location=location,
    )

    return [
        *command[:-1],
        "--dry_run",
        "--format=prettyjson",
        command[-1],
    ]


def validate_insert_dry_run(
    output: str,
    *,
    maximum_bytes_billed: int,
) -> int:
    """Fail closed unless BigQuery confirms a successful dry run."""
    budget = validate_query_budget(maximum_bytes_billed)

    try:
        payload = json.loads(output)
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid BigQuery dry-run JSON") from exc

    if not isinstance(payload, dict):
        raise TypeError("Invalid BigQuery dry-run response")

    configuration = payload.get("configuration")
    if not isinstance(configuration, dict):
        raise TypeError("Missing BigQuery dry-run configuration")

    if configuration.get("dryRun") is not True:
        raise ValueError("BigQuery response is not a dry run")

    if configuration.get("jobType") != "QUERY":
        raise ValueError("BigQuery dry run is not a query job")

    query_configuration = configuration.get("query")
    if not isinstance(query_configuration, dict):
        raise TypeError("Missing BigQuery query configuration")

    configured_budget = query_configuration.get("maximumBytesBilled")
    if (
        isinstance(configured_budget, bool)
        or not isinstance(configured_budget, (str, int))
        or not str(configured_budget).isdigit()
        or int(configured_budget) != budget
    ):
        raise ValueError("BigQuery dry-run budget mismatch")

    status = payload.get("status")
    if not isinstance(status, dict):
        raise TypeError("Missing BigQuery dry-run status")

    if status.get("state") != "DONE":
        raise ValueError("BigQuery dry run did not complete")

    if "errorResult" in status or status.get("errors"):
        raise ValueError("BigQuery dry run reported errors")

    statistics = payload.get("statistics")
    if not isinstance(statistics, dict):
        raise TypeError("Missing BigQuery dry-run statistics")

    query_statistics = statistics.get("query")
    if not isinstance(query_statistics, dict):
        raise TypeError("Missing BigQuery query statistics")

    estimate = query_statistics.get("totalBytesProcessed")

    if (
        isinstance(estimate, bool)
        or not isinstance(estimate, (int, str))
        or not str(estimate).isdigit()
    ):
        raise ValueError("Invalid BigQuery byte estimate")

    bytes_processed = int(estimate)

    if bytes_processed > budget:
        raise ValueError(
            "BigQuery dry-run estimate exceeds budget: "
            f"{bytes_processed} > {budget}"
        )

    return bytes_processed
