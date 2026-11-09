"""Validate source price compatibility with the Parquet schema."""

import json
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path

from openpyxl import load_workbook

from commercelake.ingestion.schema import normalize_price

SOURCE = Path("data/raw/online_retail_ii/online_retail_II.xlsx")

REPORT = Path("reports/price_precision_validation.json")


def main():
    if not SOURCE.is_file():
        raise FileNotFoundError(SOURCE)

    counts = Counter()
    examples = []
    maximum_absolute_price = Decimal(0)
    workbook = load_workbook(
        SOURCE,
        read_only=True,
        data_only=True,
    )

    try:
        for sheet in workbook.worksheets:
            print(f"Validating {sheet.title}", flush=True)

            rows = sheet.iter_rows(values_only=True)
            headers = next(rows)

            if headers[5] != "Price":
                raise ValueError(f"Unexpected price column in {sheet.title}")

            for row_number, row in enumerate(rows, start=2):
                counts["total_rows"] += 1
                value = row[5]

                if value is None:
                    counts["null_prices"] += 1
                    continue

                try:
                    decimal_value = Decimal(str(value))
                    normalized = normalize_price(value)

                    if len(normalized.as_tuple().digits) > 18:
                        raise ValueError("Price exceeds decimal precision")

                    maximum_absolute_price = max(
                        maximum_absolute_price,
                        abs(decimal_value),
                    )

                    counts["valid_prices"] += 1

                except (
                    InvalidOperation,
                    ValueError,
                    OverflowError,
                ) as exc:
                    counts["invalid_prices"] += 1

                    if len(examples) < 20:
                        examples.append(
                            {
                                "sheet": sheet.title,
                                "row": row_number,
                                "value": str(value),
                                "error": str(exc),
                            }
                        )

                if counts["total_rows"] % 100000 == 0:
                    print(
                        f"Checked {counts['total_rows']:,} rows",
                        flush=True,
                    )

    finally:
        workbook.close()

    result = {
        "total_rows": counts["total_rows"],
        "valid_prices": counts["valid_prices"],
        "null_prices": counts["null_prices"],
        "invalid_prices": counts["invalid_prices"],
        "maximum_absolute_price": str(maximum_absolute_price),
        "invalid_examples": examples,
        "compatible_with_decimal_18_4": (counts["invalid_prices"] == 0),
    }

    REPORT.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    REPORT.write_text(
        json.dumps(result, indent=2) + "\n",
        encoding="utf-8",
    )

    print("\n=== PRICE PRECISION REPORT ===")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
