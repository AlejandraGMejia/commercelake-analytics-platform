"""Build deterministic BigQuery ingestion staging identifiers."""

from commercelake.cloud.bigquery_staging import staging_table_name


def ingestion_staging_table_name(
    table: str,
    run_id: str,
) -> str:
    """Return an isolated ingestion table name for a Gold run."""
    gold_stage = staging_table_name(table, run_id)

    return gold_stage.replace("_stage_", "_ingest_", 1)


def gold_staging_table_name(
    table: str,
    run_id: str,
) -> str:
    """Return the existing Gold staging table name."""
    return staging_table_name(table, run_id)
