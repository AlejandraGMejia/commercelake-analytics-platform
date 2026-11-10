"""Generate valid synthetic Parquet data for Gold contract tests."""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from commercelake.warehouse.loader import TABLE_SCHEMAS


def sample_value(data_type: pa.DataType, index: int):
    """Return a value compatible with an Arrow data type."""
    if pa.types.is_string(data_type):
        return f"test_{index}"
    if pa.types.is_integer(data_type):
        return index + 1
    if pa.types.is_boolean(data_type):
        return False
    if pa.types.is_decimal(data_type):
        return Decimal("1.0000")
    if pa.types.is_date(data_type):
        return date(2020, 1, 1)
    if pa.types.is_timestamp(data_type):
        if data_type.tz is not None:
            return datetime(2020, 1, 1, tzinfo=UTC)
        return datetime(2020, 1, 1)
    raise TypeError(f"Unsupported synthetic data type: {data_type}")


def write_gold_parquet(
    path: Path,
    table_name: str,
    row_count: int,
    schema: pa.Schema | None = None,
) -> None:
    """Write synthetic Parquet with valid values for required fields."""
    if row_count < 0:
        raise ValueError("row_count cannot be negative")

    if schema is None:
        schema = TABLE_SCHEMAS[table_name]

    arrays = []

    for field in schema:
        if field.nullable:
            values = [None] * row_count
        else:
            values = [sample_value(field.type, index) for index in range(row_count)]

        arrays.append(pa.array(values, type=field.type))

    table = pa.Table.from_arrays(arrays, schema=schema)
    pq.write_table(table, path)
