"""Build explicit BigQuery transformations for Gold staging."""

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


def build_fact_sales_transform_query(
    project_id: str,
    dataset: str,
    run_id: str,
    *,
    source_table: str,
    location: str = DEFAULT_LOCATION,
) -> str:
    """Build a SELECT matching the fact_sales Gold contract."""
    validate_identifiers(project_id, dataset)

    if location != DEFAULT_LOCATION:
        raise ValueError("Unsupported BigQuery location")

    expected_source = ingestion_staging_table_name(
        "fact_sales",
        run_id,
    )

    if source_table != expected_source:
        raise ValueError("Unexpected fact_sales ingestion source")

    fields = expected_bigquery_schema("fact_sales")

    if sum(field["name"] == "invoice_date" for field in fields) != 1:
        raise ValueError("Expected exactly one invoice_date field")

    expressions = []

    for field in fields:
        name = field["name"]

        if name == "invoice_date":
            if field["type"] != "DATETIME":
                raise ValueError("Unexpected invoice_date Gold type")

            expression = (
                "DATETIME(`invoice_date`, 'UTC') "
                "AS `invoice_date`"
            )
        else:
            expression = f"`{name}`"

        expressions.append(f"    {expression}")

    projection = ",\n".join(expressions)
    source = f"`{project_id}.{dataset}.{source_table}`"

    return f"SELECT\n{projection}\nFROM {source}"


def build_fact_sales_transform_dry_run_command(
    project_id: str,
    dataset: str,
    run_id: str,
    *,
    source_table: str,
    location: str = DEFAULT_LOCATION,
) -> list[str]:
    """Build a BigQuery dry-run command without executing the query."""
    query = build_fact_sales_transform_query(
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
        "--dry_run",
        "--format=prettyjson",
        query,
    ]
