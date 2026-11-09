"""Profile the UCI Online Retail II source workbook."""

import hashlib
import json
import sqlite3
import tempfile
from collections import Counter
from datetime import date, datetime
from pathlib import Path

from openpyxl import load_workbook

SOURCE = Path("data/raw/online_retail_ii/online_retail_II.xlsx")
REPORT_DIR = Path("reports")
EXPECTED_COLUMNS = (
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer ID",
    "Country",
)


def normalize(value):
    """Normalize a cell for counting and hashing."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip()
    return value


def fingerprint(row):
    """Create a stable fingerprint of a source row."""
    payload = json.dumps(
        [normalize(value) for value in row],
        ensure_ascii=False,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def profile_sheet(sheet, connection):
    """Collect quality metrics for one worksheet."""
    iterator = sheet.iter_rows(values_only=True)
    columns = tuple(next(iterator))

    if columns != EXPECTED_COLUMNS:
        raise ValueError(f"Unexpected columns in {sheet.title}: {columns}")

    metrics = {
        "sheet": sheet.title,
        "rows": 0,
        "missing": Counter(),
        "duplicate_rows": 0,
        "cancellation_invoices": 0,
        "negative_quantities": 0,
        "zero_quantities": 0,
        "negative_prices": 0,
        "zero_prices": 0,
        "invalid_dates": 0,
        "invalid_quantities": 0,
        "invalid_prices": 0,
        "min_date": None,
        "max_date": None,
        "countries": Counter(),
        "gross_positive_amount": 0.0,
        "negative_quantity_amount": 0.0,
    }

    for row in iterator:
        if all(value is None for value in row):
            continue

        metrics["rows"] += 1
        record = dict(zip(columns, row))

        for column, value in record.items():
            if value is None or (isinstance(value, str) and not value.strip()):
                metrics["missing"][column] += 1

        digest = fingerprint(row)
        cursor = connection.execute(
            "INSERT OR IGNORE INTO fingerprints (digest) VALUES (?)",
            (digest,),
        )
        if cursor.rowcount == 0:
            metrics["duplicate_rows"] += 1

        invoice = str(record["Invoice"] or "").strip()
        if invoice.upper().startswith("C"):
            metrics["cancellation_invoices"] += 1

        quantity = record["Quantity"]
        price = record["Price"]
        valid_quantity = False
        valid_price = False

        try:
            quantity = float(quantity)
            valid_quantity = True
            if quantity < 0:
                metrics["negative_quantities"] += 1
            elif quantity == 0:
                metrics["zero_quantities"] += 1
        except TypeError, ValueError, OverflowError:
            metrics["invalid_quantities"] += 1

        try:
            price = float(price)
            valid_price = True
            if price < 0:
                metrics["negative_prices"] += 1
            elif price == 0:
                metrics["zero_prices"] += 1
        except TypeError, ValueError, OverflowError:
            metrics["invalid_prices"] += 1

        if valid_quantity and valid_price:
            amount = quantity * price
            if quantity > 0 and price > 0:
                metrics["gross_positive_amount"] += amount
            elif quantity < 0:
                metrics["negative_quantity_amount"] += amount

        invoice_date = record["InvoiceDate"]
        if isinstance(invoice_date, datetime):
            value = invoice_date.date()
        elif isinstance(invoice_date, date):
            value = invoice_date
        else:
            value = None
            metrics["invalid_dates"] += 1

        if value is not None:
            if metrics["min_date"] is None:
                metrics["min_date"] = value
            if metrics["max_date"] is None:
                metrics["max_date"] = value

            metrics["min_date"] = min(metrics["min_date"], value)
            metrics["max_date"] = max(metrics["max_date"], value)

        country = record["Country"]
        if country is not None and str(country).strip():
            metrics["countries"][str(country).strip()] += 1

        if metrics["rows"] % 100000 == 0:
            print(
                f"  {sheet.title}: {metrics['rows']:,} rows processed",
                flush=True,
            )
            connection.commit()

    connection.commit()

    metrics["missing"] = dict(metrics["missing"])
    metrics["countries"] = dict(metrics["countries"])
    metrics["min_date"] = (
        metrics["min_date"].isoformat() if metrics["min_date"] else None
    )
    metrics["max_date"] = (
        metrics["max_date"].isoformat() if metrics["max_date"] else None
    )
    metrics["gross_positive_amount"] = round(metrics["gross_positive_amount"], 2)
    metrics["negative_quantity_amount"] = round(metrics["negative_quantity_amount"], 2)

    return metrics


def write_report(results):
    """Generate human-readable and machine-readable reports."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    total_rows = sum(item["rows"] for item in results)
    total_duplicates = sum(item["duplicate_rows"] for item in results)

    output = {
        "source": "UCI Online Retail II",
        "total_rows": total_rows,
        "duplicate_rows": total_duplicates,
        "sheets": results,
    }

    json_path = REPORT_DIR / "retail_profile.json"
    json_path.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    lines = [
        "# Online Retail II - Data Quality Profile",
        "",
        "## Source",
        "",
        "UCI Machine Learning Repository - Online Retail II.",
        "",
        "## Summary",
        "",
        f"- Total source rows: {total_rows:,}",
        f"- Exact duplicate rows: {total_duplicates:,}",
        "",
        "Duplicates are counted within each worksheet.",
        "They are not automatically removed.",
        "",
    ]

    for item in results:
        lines.extend(
            [
                f"## {item['sheet']}",
                "",
                f"- Rows: {item['rows']:,}",
                f"- Exact duplicates: {item['duplicate_rows']:,}",
                (f"- Cancellation invoice lines: {item['cancellation_invoices']:,}"),
                (f"- Negative quantities: {item['negative_quantities']:,}"),
                f"- Zero quantities: {item['zero_quantities']:,}",
                f"- Negative prices: {item['negative_prices']:,}",
                f"- Zero prices: {item['zero_prices']:,}",
                f"- Invalid dates: {item['invalid_dates']:,}",
                (f"- Invalid quantities: {item['invalid_quantities']:,}"),
                f"- Invalid prices: {item['invalid_prices']:,}",
                f"- Earliest date: {item['min_date']}",
                f"- Latest date: {item['max_date']}",
                f"- Distinct countries: {len(item['countries'])}",
                "",
                "### Missing values",
                "",
                "| Column | Missing |",
                "|---|---:|",
            ]
        )

        for column in EXPECTED_COLUMNS:
            count = item["missing"].get(column, 0)
            lines.append(f"| {column} | {count:,} |")

        lines.extend(
            [
                "",
                "### Top 10 countries by row count",
                "",
                "| Country | Rows |",
                "|---|---:|",
            ]
        )

        top_countries = sorted(
            item["countries"].items(),
            key=lambda pair: (-pair[1], pair[0]),
        )[:10]

        for country, count in top_countries:
            lines.append(f"| {country} | {count:,} |")

        lines.extend(
            [
                "",
                "### Preliminary monetary diagnostics",
                "",
                (
                    f"- Positive quantity/price line amount: "
                    f"{item['gross_positive_amount']:,.2f}"
                ),
                (
                    f"- Negative quantity line amount: "
                    f"{item['negative_quantity_amount']:,.2f}"
                ),
                "",
                ("These figures are diagnostics, not approved business revenue KPIs."),
                "",
            ]
        )

    lines.extend(
        [
            "## Pending modeling decisions",
            "",
            "- Confirm cancellation and return semantics.",
            "- Investigate duplicate source lines.",
            "- Determine treatment of unknown customers.",
            "- Validate product identifiers and descriptions.",
            "- Define approved revenue calculations.",
            "- Investigate anomalies before applying filters.",
            "",
        ]
    )

    md_path = REPORT_DIR / "retail_profile.md"
    md_path.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )

    print("\n=== FINAL SUMMARY ===")
    print(f"Total rows: {total_rows:,}")
    print(f"Exact duplicates: {total_duplicates:,}")

    for item in results:
        print(f"\n{item['sheet']}")
        print(f"  Rows: {item['rows']:,}")
        print(f"  Duplicates: {item['duplicate_rows']:,}")
        print(
            "  Missing customers:",
            item["missing"].get("Customer ID", 0),
        )
        print(
            "  Cancellation invoice lines:",
            item["cancellation_invoices"],
        )
        print(
            "  Negative quantities:",
            item["negative_quantities"],
        )
        print(
            "  Negative prices:",
            item["negative_prices"],
        )
        print(
            "  Date range:",
            item["min_date"],
            "to",
            item["max_date"],
        )

    print("\nGenerated reports:")
    print(md_path)
    print(json_path)


def main():
    """Execute the source profiling process."""
    if not SOURCE.is_file():
        raise FileNotFoundError(SOURCE)

    print("=== COMMERCELAKE SOURCE PROFILING ===")

    workbook = load_workbook(
        SOURCE,
        read_only=True,
        data_only=True,
    )

    results = []

    try:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "fingerprints.db"

            with sqlite3.connect(database) as connection:
                connection.execute(
                    "CREATE TABLE fingerprints (digest TEXT PRIMARY KEY)"
                )

                for sheet in workbook:
                    print(f"\nProfiling {sheet.title}...")

                    connection.execute("DELETE FROM fingerprints")

                    results.append(profile_sheet(sheet, connection))
    finally:
        workbook.close()

    write_report(results)


if __name__ == "__main__":
    main()
