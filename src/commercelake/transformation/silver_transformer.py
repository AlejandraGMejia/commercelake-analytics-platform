"""Deterministic business transformations for Silver retail data."""

from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa

from commercelake.ingestion.schema import RETAIL_SCHEMA
from commercelake.transformation.silver_schema import SILVER_SCHEMA


def classify_transaction(invoice, quantity, unit_price):
    """Classify a transaction using explicit business rules."""
    if invoice is not None and invoice.strip().upper().startswith("C"):
        return "cancellation"

    if quantity is not None and quantity < 0:
        return "return"

    if (
        quantity is not None
        and quantity > 0
        and unit_price is not None
        and unit_price > 0
    ):
        return "sale"

    return "review"


def calculate_gross_amount(quantity, unit_price):
    """Calculate a signed monetary amount using exact decimal arithmetic."""
    if quantity is None or unit_price is None:
        return None

    return Decimal(quantity) * unit_price


def transform_retail_batch(batch, processed_at=None):
    """Transform a Bronze Arrow batch without removing source records."""
    if isinstance(batch, pa.RecordBatch):
        source = pa.Table.from_batches([batch])
    elif isinstance(batch, pa.Table):
        source = batch
    else:
        raise TypeError("Expected a PyArrow Table or RecordBatch")

    if not source.schema.equals(RETAIL_SCHEMA, check_metadata=False):
        raise ValueError("Unexpected Bronze schema")

    if processed_at is None:
        processed_at = datetime.now(UTC)

    if processed_at.tzinfo is None:
        raise ValueError("processed_at must be timezone-aware")

    processed_at = processed_at.astimezone(UTC)

    output_rows = []

    for row in source.to_pylist():
        invoice = row["invoice"]
        quantity = row["quantity"]
        unit_price = row["unit_price"]
        customer_id = row["customer_id"]

        output_rows.append(
            {
                **row,
                "transaction_type": classify_transaction(
                    invoice,
                    quantity,
                    unit_price,
                ),
                "gross_amount": calculate_gross_amount(
                    quantity,
                    unit_price,
                ),
                "is_customer_missing": (customer_id is None or not customer_id.strip()),
                "is_price_non_positive": (unit_price is None or unit_price <= 0),
                "is_date_missing": row["invoice_date"] is None,
                "silver_processed_at": processed_at,
            }
        )

    result = pa.Table.from_pylist(
        output_rows,
        schema=SILVER_SCHEMA,
    )

    if result.num_rows != source.num_rows:
        raise RuntimeError("Silver transformation changed the source row count")

    return result
