"""Deterministic dimensional models for CommerceLake Gold."""

from collections import Counter, defaultdict
from datetime import timedelta

import pyarrow as pa

UNKNOWN_KEY = "UNKNOWN"

PRODUCT_PREFIX = "PRODUCT:"
CUSTOMER_PREFIX = "CUSTOMER:"
COUNTRY_PREFIX = "COUNTRY:"

PRODUCT_SCHEMA = pa.schema(
    [
        pa.field("product_key", pa.string(), nullable=False),
        pa.field("stock_code", pa.string()),
        pa.field("description", pa.string()),
        pa.field("description_variants", pa.int64(), nullable=False),
    ]
)

CUSTOMER_SCHEMA = pa.schema(
    [
        pa.field("customer_key", pa.string(), nullable=False),
        pa.field("customer_id", pa.string()),
    ]
)

COUNTRY_SCHEMA = pa.schema(
    [
        pa.field("country_key", pa.string(), nullable=False),
        pa.field("country", pa.string()),
    ]
)

DATE_SCHEMA = pa.schema(
    [
        pa.field("date_key", pa.int32(), nullable=False),
        pa.field("calendar_date", pa.date32(), nullable=False),
        pa.field("year", pa.int32(), nullable=False),
        pa.field("month", pa.int32(), nullable=False),
        pa.field("day", pa.int32(), nullable=False),
        pa.field("quarter", pa.int32(), nullable=False),
        pa.field("day_of_week", pa.int32(), nullable=False),
    ]
)


def dimensional_key(prefix, value):
    """Create a collision-safe dimensional key."""
    if value is None:
        return f"{prefix}{UNKNOWN_KEY}"

    if not isinstance(value, str):
        raise TypeError("Dimensional business keys must be strings")

    if not value.strip():
        raise ValueError("Empty dimensional business key")

    return f"{prefix}VALUE:{value}"


class DimensionCollector:
    """Collect distinct dimensional values across bounded batches."""

    def __init__(self):
        self.product_descriptions = defaultdict(Counter)
        self.products = set()
        self.customers = set()
        self.countries = set()
        self.dates = set()

    def consume(self, batch):
        """Accumulate dimensional values from a Silver batch."""
        required = {
            "stock_code",
            "description",
            "customer_id",
            "country",
            "invoice_date",
        }

        if not required.issubset(batch.schema.names):
            raise ValueError("Silver batch is missing dimensional columns")

        columns = batch.select(sorted(required)).to_pydict()

        for product, description, customer, country, timestamp in zip(
            columns["stock_code"],
            columns["description"],
            columns["customer_id"],
            columns["country"],
            columns["invoice_date"],
            strict=True,
        ):
            if product is not None:
                dimensional_key(PRODUCT_PREFIX, product)
                self.products.add(product)

                if description is not None:
                    normalized = description.strip()

                    if normalized:
                        self.product_descriptions[product][normalized] += 1

            if customer is not None:
                dimensional_key(CUSTOMER_PREFIX, customer)
                self.customers.add(customer)

            if country is not None:
                dimensional_key(COUNTRY_PREFIX, country)
                self.countries.add(country)

            if timestamp is not None:
                self.dates.add(timestamp.date())

    def build_products(self):
        """Create one deterministic row per product."""
        rows = [
            {
                "product_key": dimensional_key(PRODUCT_PREFIX, None),
                "stock_code": None,
                "description": "Unknown product",
                "description_variants": 0,
            }
        ]

        for product in sorted(self.products):
            descriptions = self.product_descriptions[product]

            canonical = (
                min(
                    descriptions,
                    key=lambda value: (-descriptions[value], value),
                )
                if descriptions
                else None
            )

            rows.append(
                {
                    "product_key": dimensional_key(PRODUCT_PREFIX, product),
                    "stock_code": product,
                    "description": canonical,
                    "description_variants": len(descriptions),
                }
            )

        return pa.Table.from_pylist(rows, schema=PRODUCT_SCHEMA)

    def build_customers(self):
        """Create customer dimension with an unknown member."""
        rows = [
            {
                "customer_key": dimensional_key(CUSTOMER_PREFIX, None),
                "customer_id": None,
            }
        ]

        rows.extend(
            {
                "customer_key": dimensional_key(CUSTOMER_PREFIX, customer),
                "customer_id": customer,
            }
            for customer in sorted(self.customers)
        )

        return pa.Table.from_pylist(rows, schema=CUSTOMER_SCHEMA)

    def build_countries(self):
        """Create transaction-country dimension."""
        rows = [
            {
                "country_key": dimensional_key(COUNTRY_PREFIX, None),
                "country": None,
            }
        ]

        rows.extend(
            {
                "country_key": dimensional_key(COUNTRY_PREFIX, country),
                "country": country,
            }
            for country in sorted(self.countries)
        )

        return pa.Table.from_pylist(rows, schema=COUNTRY_SCHEMA)

    def build_dates(self):
        """Create a continuous calendar over observed dates."""
        if not self.dates:
            return pa.Table.from_pylist([], schema=DATE_SCHEMA)

        current = min(self.dates)
        last = max(self.dates)
        rows = []

        while current <= last:
            rows.append(
                {
                    "date_key": int(current.strftime("%Y%m%d")),
                    "calendar_date": current,
                    "year": current.year,
                    "month": current.month,
                    "day": current.day,
                    "quarter": (current.month - 1) // 3 + 1,
                    "day_of_week": current.isoweekday(),
                }
            )
            current += timedelta(days=1)

        return pa.Table.from_pylist(rows, schema=DATE_SCHEMA)

    def build_all(self):
        """Build all four dimensional tables."""
        return {
            "dim_product": self.build_products(),
            "dim_customer": self.build_customers(),
            "dim_country": self.build_countries(),
            "dim_date": self.build_dates(),
        }
