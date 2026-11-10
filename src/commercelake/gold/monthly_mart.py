"""Streaming monthly sales mart for CommerceLake Gold."""

from collections import Counter, defaultdict
from decimal import Decimal

import pyarrow as pa

from commercelake.gold.fact_sales import (
    FACT_SALES_SCHEMA,
    VALID_TRANSACTION_TYPES,
)

ZERO = Decimal("0.0000")
AMOUNT_TYPE = pa.decimal128(38, 4)

MONTHLY_MART_SCHEMA = pa.schema(
    [
        pa.field("year", pa.int32(), nullable=False),
        pa.field("month", pa.int32(), nullable=False),
        pa.field("year_month", pa.string(), nullable=False),
        pa.field("transaction_count", pa.int64(), nullable=False),
        pa.field("sale_count", pa.int64(), nullable=False),
        pa.field("cancellation_count", pa.int64(), nullable=False),
        pa.field("return_count", pa.int64(), nullable=False),
        pa.field("review_count", pa.int64(), nullable=False),
        pa.field(
            "identified_customer_count",
            pa.int64(),
            nullable=False,
        ),
        pa.field("sales_amount", AMOUNT_TYPE, nullable=False),
        pa.field("cancellation_amount", AMOUNT_TYPE, nullable=False),
        pa.field("return_amount", AMOUNT_TYPE, nullable=False),
        pa.field("net_amount", AMOUNT_TYPE, nullable=False),
    ]
)


class MonthlySalesCollector:
    """Accumulate monthly metrics across bounded Gold fact batches."""

    def __init__(self):
        self.counts = defaultdict(Counter)
        self.amounts = defaultdict(lambda: defaultdict(lambda: ZERO))
        self.customers = defaultdict(set)
        self.total_rows = 0
        self.total_net_amount = ZERO

    def consume(self, batch):
        """Aggregate a validated Gold fact batch."""
        if not batch.schema.equals(
            FACT_SALES_SCHEMA,
            check_metadata=False,
        ):
            raise ValueError("Gold fact schema mismatch")

        selected = batch.select(
            [
                "date_key",
                "transaction_type",
                "net_transaction_amount",
                "customer_id",
            ]
        ).to_pydict()

        for date, kind, amount, customer in zip(
            selected["date_key"],
            selected["transaction_type"],
            selected["net_transaction_amount"],
            selected["customer_id"],
            strict=True,
        ):
            if date is None:
                raise ValueError("Monthly mart requires a date_key")

            year, month = divmod(date, 10000)
            month, day = divmod(month, 100)

            if not 1 <= month <= 12 or not 1 <= day <= 31:
                raise ValueError("Invalid date_key")

            if kind not in VALID_TRANSACTION_TYPES:
                raise ValueError(f"Unsupported transaction type: {kind}")

            if kind == "review":
                if amount is not None:
                    raise ValueError("Review must have a null net amount")
            elif amount is None:
                raise ValueError(f"Missing net amount for {kind}")

            key = (year, month)

            self.counts[key][kind] += 1
            self.total_rows += 1

            if customer is not None:
                self.customers[key].add(customer)

            if kind != "review":
                self.amounts[key][kind] += amount
                self.total_net_amount += amount

    def build(self):
        """Return one deterministically ordered row per observed month."""
        rows = []

        for year, month in sorted(self.counts):
            key = (year, month)
            counts = self.counts[key]
            amounts = self.amounts[key]

            sales = amounts["sale"]
            cancellations = amounts["cancellation"]
            returns = amounts["return"]

            rows.append(
                {
                    "year": year,
                    "month": month,
                    "year_month": f"{year:04d}-{month:02d}",
                    "transaction_count": sum(counts.values()),
                    "sale_count": counts["sale"],
                    "cancellation_count": counts["cancellation"],
                    "return_count": counts["return"],
                    "review_count": counts["review"],
                    "identified_customer_count": len(self.customers[key]),
                    "sales_amount": sales,
                    "cancellation_amount": cancellations,
                    "return_amount": returns,
                    "net_amount": (sales + cancellations + returns),
                }
            )

        return pa.Table.from_pylist(
            rows,
            schema=MONTHLY_MART_SCHEMA,
        )

    def validate(self, table):
        """Reconcile mart totals with accumulated fact records."""
        if not table.schema.equals(
            MONTHLY_MART_SCHEMA,
            check_metadata=False,
        ):
            raise ValueError("Monthly mart schema mismatch")

        rows = table.to_pylist()

        row_total = sum(row["transaction_count"] for row in rows)

        net_total = sum(
            (row["net_amount"] for row in rows),
            ZERO,
        )

        if row_total != self.total_rows:
            raise ValueError("Monthly transaction count mismatch")

        if net_total != self.total_net_amount:
            raise ValueError("Monthly financial reconciliation failed")

        for row in rows:
            classified = sum(
                row[field]
                for field in (
                    "sale_count",
                    "cancellation_count",
                    "return_count",
                    "review_count",
                )
            )

            if classified != row["transaction_count"]:
                raise ValueError("Monthly classification count mismatch")

            components = (
                row["sales_amount"] + row["cancellation_amount"] + row["return_amount"]
            )

            if components != row["net_amount"]:
                raise ValueError("Monthly amount components do not reconcile")

        return True
