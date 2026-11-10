"""Unit tests for CommerceLake Gold dimensions."""

from datetime import datetime

import pyarrow as pa
import pytest

from commercelake.gold.dimensions import (
    CUSTOMER_PREFIX,
    CUSTOMER_SCHEMA,
    DATE_SCHEMA,
    PRODUCT_PREFIX,
    PRODUCT_SCHEMA,
    DimensionCollector,
    dimensional_key,
)

INPUT_SCHEMA = pa.schema(
    [
        pa.field("stock_code", pa.string()),
        pa.field("description", pa.string()),
        pa.field("customer_id", pa.string()),
        pa.field("country", pa.string()),
        pa.field("invoice_date", pa.timestamp("us")),
    ]
)


def make_batch(rows):
    """Create a dimensional input batch."""
    return pa.Table.from_pylist(rows, schema=INPUT_SCHEMA)


def make_row(
    product="SKU001",
    description="Product",
    customer="12345",
    country="United Kingdom",
    timestamp=None,
):
    """Create a representative Silver dimensional record."""
    return {
        "stock_code": product,
        "description": description,
        "customer_id": customer,
        "country": country,
        "invoice_date": timestamp or datetime(2010, 1, 1),
    }


def test_product_description_uses_most_frequent_value():
    collector = DimensionCollector()

    collector.consume(
        make_batch(
            [
                make_row(description="Blue Shirt"),
                make_row(description="Red Shirt"),
                make_row(description="Blue Shirt"),
            ]
        )
    )

    products = collector.build_products().to_pylist()
    product = next(row for row in products if row["stock_code"] == "SKU001")

    assert product["description"] == "Blue Shirt"
    assert product["description_variants"] == 2


def test_product_description_tie_is_deterministic():
    collector = DimensionCollector()

    collector.consume(
        make_batch(
            [
                make_row(description="Zebra"),
                make_row(description="Apple"),
            ]
        )
    )

    product = collector.build_products().to_pylist()[1]
    assert product["description"] == "Apple"


def test_missing_customer_maps_to_unknown_member():
    collector = DimensionCollector()

    collector.consume(
        make_batch(
            [
                make_row(customer=None),
                make_row(customer="12345"),
            ]
        )
    )

    customers = collector.build_customers().to_pylist()

    assert customers[0]["customer_key"] == "CUSTOMER:UNKNOWN"
    assert customers[0]["customer_id"] is None
    assert customers[1]["customer_key"] == "CUSTOMER:VALUE:12345"


def test_country_dimension_uses_transaction_country():
    collector = DimensionCollector()

    collector.consume(
        make_batch(
            [
                make_row(customer="12345", country="France"),
                make_row(customer="12345", country="Germany"),
            ]
        )
    )

    countries = collector.build_countries().to_pylist()

    assert {row["country"] for row in countries} == {
        None,
        "France",
        "Germany",
    }


def test_date_dimension_is_continuous():
    collector = DimensionCollector()

    collector.consume(
        make_batch(
            [
                make_row(timestamp=datetime(2010, 1, 1)),
                make_row(timestamp=datetime(2010, 1, 3)),
            ]
        )
    )

    dates = collector.build_dates().to_pylist()

    assert [row["date_key"] for row in dates] == [
        20100101,
        20100102,
        20100103,
    ]


def test_empty_collector_has_unknown_members():
    collector = DimensionCollector()

    assert collector.build_products().num_rows == 1
    assert collector.build_customers().num_rows == 1
    assert collector.build_countries().num_rows == 1
    assert collector.build_dates().num_rows == 0


def test_dimension_schemas_are_preserved():
    collector = DimensionCollector()

    assert collector.build_products().schema == PRODUCT_SCHEMA
    assert collector.build_customers().schema == CUSTOMER_SCHEMA
    assert collector.build_dates().schema == DATE_SCHEMA


def test_missing_required_columns_are_rejected():
    collector = DimensionCollector()
    invalid = pa.table({"stock_code": ["SKU001"]})

    with pytest.raises(ValueError, match="missing dimensional columns"):
        collector.consume(invalid)


def test_repeated_values_do_not_duplicate_dimension_members():
    collector = DimensionCollector()

    collector.consume(make_batch([make_row()] * 3))

    assert collector.build_products().num_rows == 2
    assert collector.build_customers().num_rows == 2
    assert collector.build_countries().num_rows == 2


def test_literal_unknown_does_not_collide_with_missing():
    assert dimensional_key(CUSTOMER_PREFIX, None) != dimensional_key(
        CUSTOMER_PREFIX, "UNKNOWN"
    )


def test_product_and_customer_keys_are_separate():
    assert dimensional_key(PRODUCT_PREFIX, "12345") != dimensional_key(
        CUSTOMER_PREFIX, "12345"
    )


def test_empty_business_keys_are_rejected():
    with pytest.raises(ValueError, match="Empty"):
        dimensional_key(PRODUCT_PREFIX, "   ")


def test_non_string_business_keys_are_rejected():
    with pytest.raises(TypeError, match="strings"):
        dimensional_key(CUSTOMER_PREFIX, 12345)
