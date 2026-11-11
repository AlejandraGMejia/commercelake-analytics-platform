"""Build budget-controlled SQL insertion commands for Gold staging."""

from commercelake.cloud.bigquery_ingestion_staging import (
    ingestion_staging_table_name,
)
from commercelake.cloud.bigquery_publisher import (
    DEFAULT_LOCATION,
    validate_identifiers,
)
from commercelake.cloud.bigquery_schema_validation import (
    expected_bigquery_schema,
)
from commercelake.cloud.bigquery_staging import staging_table_name
from commercelake.cloud.bigquery_staging_transform import (
    build_fact_sales_transform_query,
)

MINIMUM_BYTES_BILLED = 10485760


def validate_query_budget(maximum_bytes_billed: int) -> int:
    """Require an explicit positive BigQuery query billing budget."""
    if (
        isinstance(maximum_bytes_billed, bool)
        or not isinstance(maximum_bytes_billed, int)
        or maximum_bytes_billed < MINIMUM_BYTES_BILLED
    ):
        raise ValueError(
            "maximum_bytes_billed must be an integer "
            "of at least 10485760 bytes"
        )

    return maximum_bytes_billed


def build_fact_sales_insert_query(
    project_id: str,
    dataset: str,
    run_id: str,
    *,
    source_table: str,
    location: str = DEFAULT_LOCATION,
) -> str:
    """Build an INSERT into run-specific Gold staging."""
    validate_identifiers(project_id, dataset)

    if location != DEFAULT_LOCATION:
        raise ValueError("Unsupported BigQuery location")

    destination = staging_table_name("fact_sales", run_id)
    expected_source = ingestion_staging_table_name(
        "fact_sales",
        run_id,
    )

    if source_table != expected_source:
        raise ValueError("Unexpected fact_sales ingestion source")

    select_query = build_fact_sales_transform_query(
        project_id,
        dataset,
        run_id,
        source_table=source_table,
        location=location,
    )

    columns = ",\n".join(
        f"    `{field['name']}`"
        for field in expected_bigquery_schema("fact_sales")
    )

    return (
        f"INSERT INTO `{project_id}.{dataset}.{destination}` (\n"
        f"{columns}\n"
        f")\n"
        f"{select_query}"
    )


def build_fact_sales_insert_command(
    project_id: str,
    dataset: str,
    run_id: str,
    *,
    source_table: str,
    maximum_bytes_billed: int,
    location: str = DEFAULT_LOCATION,
) -> list[str]:
    """Build a staging INSERT command with an explicit billing limit."""
    budget = validate_query_budget(maximum_bytes_billed)

    query = build_fact_sales_insert_query(
        project_id,
        dataset,
        run_id,
        source_table=source_table,
        location=location,
    )

    return [
        "bq",
        f"--location={location}",
        "query",
        "--use_legacy_sql=false",
        f"--maximum_bytes_billed={budget}",
        query,
    ]
