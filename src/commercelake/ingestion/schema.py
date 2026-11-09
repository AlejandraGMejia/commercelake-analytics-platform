"""Explicit schema and normalization for retail ingestion."""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import pyarrow as pa

from commercelake.ingestion.fingerprint import row_fingerprint

RETAIL_SCHEMA = pa.schema(
    [
        pa.field("invoice", pa.string()),
        pa.field("stock_code", pa.string()),
        pa.field("description", pa.string()),
        pa.field("quantity", pa.int64()),
        pa.field("invoice_date", pa.timestamp("us")),
        pa.field("unit_price", pa.decimal128(18, 4)),
        pa.field("customer_id", pa.string()),
        pa.field("country", pa.string()),
        pa.field("source_sheet", pa.string(), nullable=False),
        pa.field("source_row_number", pa.int64(), nullable=False),
        pa.field("row_fingerprint", pa.string(), nullable=False),
    ]
)


def normalize_text(value):
    """Normalize a text field without changing its meaning."""
    if value is None:
        return None

    return str(value).strip()


def normalize_integer(value):
    """Convert a valid integer-like value."""
    if value is None:
        return None

    if isinstance(value, bool):
        raise TypeError("Boolean is not a valid integer")

    number = Decimal(str(value))

    if not number.is_finite():
        raise ValueError("Non-finite integer value")

    if number != number.to_integral_value():
        raise ValueError(f"Non-integral value: {value}")

    return int(number)


def normalize_price(value):
    """Convert a price into an exact four-decimal value."""
    if value is None:
        return None

    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid price: {value}") from exc

    if not number.is_finite():
        raise ValueError(f"Non-finite price: {value}")

    quantized = number.quantize(Decimal("0.0001"))

    if quantized != number:
        raise ValueError(f"Price exceeds four decimal places: {value}")

    return quantized


def normalize_datetime(value):
    """Require a valid source date or datetime."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    raise ValueError(f"Invalid invoice date: {value}")


def normalize_retail_row(row, source_sheet, source_row_number):
    """Normalize one Excel row and preserve its provenance."""
    if len(row) != 8:
        raise ValueError(f"Expected 8 source columns, received {len(row)}")

    return {
        "invoice": normalize_text(row[0]),
        "stock_code": normalize_text(row[1]),
        "description": normalize_text(row[2]),
        "quantity": normalize_integer(row[3]),
        "invoice_date": normalize_datetime(row[4]),
        "unit_price": normalize_price(row[5]),
        "customer_id": normalize_text(row[6]),
        "country": normalize_text(row[7]),
        "source_sheet": source_sheet,
        "source_row_number": source_row_number,
        "row_fingerprint": row_fingerprint(row),
    }


def rows_to_table(rows):
    """Convert normalized rows to a typed Arrow table."""
    return pa.Table.from_pylist(
        rows,
        schema=RETAIL_SCHEMA,
    )
