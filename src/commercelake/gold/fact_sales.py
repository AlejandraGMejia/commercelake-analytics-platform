"""Gold sales fact transformation and integrity validation."""

from datetime import datetime

import pyarrow as pa

from commercelake.gold.dimensions import (
    COUNTRY_PREFIX,
    CUSTOMER_PREFIX,
    PRODUCT_PREFIX,
    dimensional_key,
)
from commercelake.transformation.silver_schema import SILVER_SCHEMA

VALID_TRANSACTION_TYPES = frozenset({"sale", "cancellation", "return", "review"})

FACT_SALES_SCHEMA = pa.schema(
    [
        *SILVER_SCHEMA,
        pa.field("product_key", pa.string(), nullable=False),
        pa.field("customer_key", pa.string(), nullable=False),
        pa.field("country_key", pa.string(), nullable=False),
        pa.field("date_key", pa.int32()),
        pa.field("net_transaction_amount", pa.decimal128(38, 4)),
    ]
)


def date_key(timestamp):
    """Convert a transaction timestamp to a calendar key."""
    if timestamp is None:
        return None

    if not isinstance(timestamp, datetime):
        raise TypeError("Invoice date must be a datetime")

    return int(timestamp.strftime("%Y%m%d"))


def net_transaction_amount(transaction_type, gross_amount):
    """Return signed amounts only for eligible transaction types."""
    if transaction_type not in VALID_TRANSACTION_TYPES:
        raise ValueError(f"Unsupported transaction type: {transaction_type}")

    if transaction_type == "review":
        return None

    return gross_amount


def transform_fact_batch(batch):
    """Transform one Silver batch without losing source rows."""
    if not batch.schema.equals(SILVER_SCHEMA, check_metadata=False):
        raise ValueError("Silver batch schema does not match SILVER_SCHEMA")

    source = batch.to_pydict()

    product_keys = []
    customer_keys = []
    country_keys = []
    date_keys = []
    net_amounts = []

    for index in range(batch.num_rows):
        product_keys.append(
            dimensional_key(
                PRODUCT_PREFIX,
                source["stock_code"][index],
            )
        )
        customer_keys.append(
            dimensional_key(
                CUSTOMER_PREFIX,
                source["customer_id"][index],
            )
        )
        country_keys.append(
            dimensional_key(
                COUNTRY_PREFIX,
                source["country"][index],
            )
        )
        date_keys.append(date_key(source["invoice_date"][index]))
        net_amounts.append(
            net_transaction_amount(
                source["transaction_type"][index],
                source["gross_amount"][index],
            )
        )

    columns = list(batch.columns)

    columns.extend(
        [
            pa.array(product_keys, type=pa.string()),
            pa.array(customer_keys, type=pa.string()),
            pa.array(country_keys, type=pa.string()),
            pa.array(date_keys, type=pa.int32()),
            pa.array(net_amounts, type=pa.decimal128(38, 4)),
        ]
    )

    return pa.RecordBatch.from_arrays(
        columns,
        schema=FACT_SALES_SCHEMA,
    )


def validate_fact_dimensions(fact, dimensions):
    """Check dimensional uniqueness and referential integrity."""
    mappings = {
        "product_key": "dim_product",
        "customer_key": "dim_customer",
        "country_key": "dim_country",
        "date_key": "dim_date",
    }

    for key, dimension_name in mappings.items():
        if dimension_name not in dimensions:
            raise ValueError(f"Missing dimension: {dimension_name}")

        dimension = dimensions[dimension_name]

        if key not in dimension.schema.names:
            raise ValueError(f"Dimension {dimension_name} has no {key}")

        valid_keys = set(dimension.column(key).to_pylist())

        if None in valid_keys:
            raise ValueError(f"Null dimension keys: {dimension_name}")

        if len(valid_keys) != dimension.num_rows:
            raise ValueError(f"Duplicate dimension keys: {dimension_name}")

        fact_keys = fact.column(key).to_pylist()

        missing = {value for value in fact_keys if value not in valid_keys}

        if missing:
            raise ValueError(
                f"Invalid foreign keys in {key}: {len(missing)} unmatched values"
            )

    return True
