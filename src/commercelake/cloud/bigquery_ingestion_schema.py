"""Define BigQuery schemas compatible with Gold Parquet ingestion."""

from commercelake.cloud.bigquery_schema_validation import (
    expected_bigquery_schema,
)


def expected_ingestion_schema(table: str) -> list[dict[str, str]]:
    """Return the schema BigQuery needs to ingest a Gold Parquet file.

    The authoritative Gold contract remains unchanged. BigQuery reads
    timezone-naive Parquet timestamps as TIMESTAMP, so fact_sales needs
    a separate ingestion schema before its DATETIME conversion.
    """
    schema = [
        field.copy()
        for field in expected_bigquery_schema(table)
    ]

    if table == "fact_sales":
        matches = [
            field
            for field in schema
            if field["name"] == "invoice_date"
        ]

        if len(matches) != 1:
            raise ValueError(
                "fact_sales must contain exactly one invoice_date field"
            )

        if matches[0]["type"] != "DATETIME":
            raise ValueError(
                "Unexpected Gold type for fact_sales.invoice_date"
            )

        matches[0]["type"] = "TIMESTAMP"

    return schema
