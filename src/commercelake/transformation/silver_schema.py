"""Explicit schema for standardized Silver retail transactions."""

import pyarrow as pa

from commercelake.ingestion.schema import RETAIL_SCHEMA

SILVER_SCHEMA = pa.schema(
    [
        *RETAIL_SCHEMA,
        pa.field("transaction_type", pa.string(), nullable=False),
        pa.field("gross_amount", pa.decimal128(38, 4)),
        pa.field("is_customer_missing", pa.bool_(), nullable=False),
        pa.field("is_price_non_positive", pa.bool_(), nullable=False),
        pa.field("is_date_missing", pa.bool_(), nullable=False),
        pa.field("silver_processed_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)
