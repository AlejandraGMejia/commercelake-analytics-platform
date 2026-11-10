"""Validate BigQuery schemas against authoritative Gold Arrow schemas."""

import json

import pyarrow as pa

from commercelake.warehouse.loader import TABLE_SCHEMAS


def arrow_type_to_bigquery(data_type: pa.DataType) -> str:
    """Map supported Gold Arrow types to BigQuery standard SQL types."""
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return "STRING"

    if (
        pa.types.is_int8(data_type)
        or pa.types.is_int16(data_type)
        or pa.types.is_int32(data_type)
        or pa.types.is_int64(data_type)
        or pa.types.is_uint8(data_type)
        or pa.types.is_uint16(data_type)
        or pa.types.is_uint32(data_type)
    ):
        return "INTEGER"

    if pa.types.is_boolean(data_type):
        return "BOOLEAN"

    if pa.types.is_float32(data_type) or pa.types.is_float64(data_type):
        return "FLOAT"

    if pa.types.is_date32(data_type) or pa.types.is_date64(data_type):
        return "DATE"

    if pa.types.is_timestamp(data_type):
        if data_type.tz is None:
            return "DATETIME"
        return "TIMESTAMP"

    if pa.types.is_decimal(data_type):
        precision = data_type.precision
        scale = data_type.scale

        if precision <= 29 and scale <= 9:
            return "NUMERIC"

        if precision <= 76 and scale <= 38:
            return "BIGNUMERIC"

        raise ValueError(f"Unsupported decimal type: {data_type}")

    raise ValueError(f"Unsupported Arrow type: {data_type}")


def expected_bigquery_schema(table: str) -> list[dict[str, str]]:
    """Generate the expected BigQuery schema from the Gold contract."""
    if table not in TABLE_SCHEMAS:
        raise ValueError(f"Unknown Gold table: {table}")

    schema = TABLE_SCHEMAS[table]
    expected = []

    for field in schema:
        expected.append(
            {
                "name": field.name,
                "type": arrow_type_to_bigquery(field.type),
                "mode": "NULLABLE" if field.nullable else "REQUIRED",
            }
        )

    return expected


def parse_bigquery_schema(metadata_output: str) -> list[dict[str, str]]:
    """Extract and validate flat BigQuery schema fields from metadata."""
    try:
        metadata = json.loads(metadata_output)
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid BigQuery metadata JSON") from exc

    if not isinstance(metadata, dict):
        raise TypeError("BigQuery metadata must be a JSON object")

    schema = metadata.get("schema")

    if not isinstance(schema, dict):
        raise TypeError("BigQuery metadata schema must be a JSON object")

    fields = schema.get("fields")

    if not isinstance(fields, list):
        raise TypeError("BigQuery schema fields must be a list")

    normalized = []
    names = set()

    aliases = {
        "INT64": "INTEGER",
        "BOOL": "BOOLEAN",
        "FLOAT64": "FLOAT",
        "DECIMAL": "NUMERIC",
        "BIGDECIMAL": "BIGNUMERIC",
    }

    for field in fields:
        if not isinstance(field, dict):
            raise TypeError("Invalid BigQuery schema field")

        name = field.get("name")
        raw_type = field.get("type")
        raw_mode = field.get("mode", "NULLABLE")

        if not isinstance(name, str):
            raise TypeError("Invalid BigQuery field name")

        if not name:
            raise ValueError("Empty BigQuery field name")

        if name in names:
            raise ValueError(f"Duplicate BigQuery field: {name}")

        if not isinstance(raw_type, str):
            raise TypeError(f"Invalid BigQuery field type: {name}")

        if not isinstance(raw_mode, str):
            raise TypeError(f"Invalid BigQuery field mode: {name}")

        field_type = raw_type.upper()
        field_type = aliases.get(field_type, field_type)
        mode = raw_mode.upper()

        if mode not in {"NULLABLE", "REQUIRED"}:
            raise ValueError(f"Unsupported BigQuery field mode: {name}")

        if field_type in {"RECORD", "STRUCT"} or "fields" in field:
            raise ValueError(f"Nested BigQuery fields are unsupported: {name}")

        names.add(name)

        normalized.append(
            {
                "name": name,
                "type": field_type,
                "mode": mode,
            }
        )

    return normalized


def validate_bigquery_schema(
    table: str,
    metadata_output: str,
) -> list[dict[str, str]]:
    """Reject any BigQuery schema differing from the Gold contract."""
    expected = expected_bigquery_schema(table)
    actual = parse_bigquery_schema(metadata_output)

    if actual != expected:
        raise ValueError(
            f"BigQuery schema mismatch for {table}: expected {expected}, got {actual}"
        )

    return actual
