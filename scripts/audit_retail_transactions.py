"""Audit cross-sheet duplicates and transaction anomalies."""

import hashlib
import json
import sqlite3
import tempfile
from collections import Counter
from datetime import date, datetime
from pathlib import Path

from openpyxl import load_workbook

SOURCE = Path("data/raw/online_retail_ii/online_retail_II.xlsx")

OUTPUT_JSON = Path("reports/transaction_audit.json")
OUTPUT_MD = Path("reports/transaction_audit.md")

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
    """Produce a stable representation for hashing."""
    if value is None:
        return None

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, str):
        return value.strip()

    if isinstance(value, float):
        return format(value, ".15g")

    return str(value)


def row_fingerprint(row):
    """Hash normalized source values."""
    payload = json.dumps(
        [normalize(value) for value in row],
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def classify_transaction(invoice, quantity, price):
    """Classify source signals without excluding records."""
    invoice_text = str(invoice or "").strip().upper()

    cancellation = invoice_text.startswith("C")

    negative_quantity = (
        isinstance(quantity, (int, float))
        and not isinstance(quantity, bool)
        and quantity < 0
    )

    zero_price = (
        isinstance(price, (int, float)) and not isinstance(price, bool) and price == 0
    )

    negative_price = (
        isinstance(price, (int, float)) and not isinstance(price, bool) and price < 0
    )

    return {
        "cancellation": cancellation,
        "negative_quantity": negative_quantity,
        "zero_price": zero_price,
        "negative_price": negative_price,
    }


def audit():
    """Run the source transaction investigation."""
    if not SOURCE.exists():
        raise FileNotFoundError(f"Source workbook not found: {SOURCE}")

    print("=== COMMERCELAKE TRANSACTION AUDIT ===")

    workbook = load_workbook(
        SOURCE,
        read_only=True,
        data_only=True,
    )

    counters = Counter()
    sheets = {}

    with tempfile.TemporaryDirectory(prefix="commercelake_audit_") as temp_dir:
        database_path = Path(temp_dir) / "audit.sqlite"

        connection = sqlite3.connect(database_path)

        try:
            connection.execute(
                """
                CREATE TABLE fingerprints (
                    fingerprint TEXT PRIMARY KEY,
                    first_sheet TEXT NOT NULL
                )
                """
            )

            for worksheet in workbook.worksheets:
                sheet_name = worksheet.title

                print(
                    f"\nAuditing {sheet_name}...",
                    flush=True,
                )

                iterator = worksheet.iter_rows(values_only=True)

                headers = tuple(next(iterator))

                if headers != EXPECTED_COLUMNS:
                    raise ValueError(f"Unexpected columns in {sheet_name}: {headers}")

                sheet_counter = Counter()
                batch = []

                for row_number, row in enumerate(
                    iterator,
                    start=1,
                ):
                    fingerprint = row_fingerprint(row)

                    batch.append((fingerprint, sheet_name))

                    flags = classify_transaction(
                        row[0],
                        row[3],
                        row[5],
                    )

                    sheet_counter["rows"] += 1

                    for name, enabled in flags.items():
                        if enabled:
                            sheet_counter[name] += 1

                    if flags["cancellation"] and flags["negative_quantity"]:
                        sheet_counter["cancellation_and_negative_quantity"] += 1

                    if flags["negative_quantity"] and not flags["cancellation"]:
                        sheet_counter["negative_quantity_without_cancellation"] += 1

                    if flags["cancellation"] and not flags["negative_quantity"]:
                        sheet_counter["cancellation_without_negative_quantity"] += 1

                    if len(batch) >= 5000:
                        duplicates = save_batch(
                            connection,
                            batch,
                            sheet_name,
                        )

                        sheet_counter.update(duplicates)
                        batch.clear()

                    if row_number % 100000 == 0:
                        print(
                            f"  {row_number:,} rows processed",
                            flush=True,
                        )

                if batch:
                    duplicates = save_batch(
                        connection,
                        batch,
                        sheet_name,
                    )
                    sheet_counter.update(duplicates)

                connection.commit()

                sheets[sheet_name] = dict(sheet_counter)
                counters.update(sheet_counter)

            result = {
                "source": "UCI Online Retail II",
                "total_rows": counters["rows"],
                "metrics": dict(counters),
                "sheets": sheets,
                "methodology": {
                    "fingerprint": ("SHA256 of normalized source values"),
                    "duplicate_scope": (
                        "Previously observed fingerprints across both worksheets"
                    ),
                    "raw_data_modified": False,
                },
            }

            write_reports(result)
            return result

        finally:
            connection.close()
            workbook.close()


def save_batch(connection, batch, current_sheet):
    """Insert hashes and count previously observed rows."""
    counts = Counter()

    for fingerprint, sheet_name in batch:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO fingerprints
            (fingerprint, first_sheet)
            VALUES (?, ?)
            """,
            (fingerprint, sheet_name),
        )

        if cursor.rowcount == 0:
            first_sheet = connection.execute(
                """
                SELECT first_sheet
                FROM fingerprints
                WHERE fingerprint = ?
                """,
                (fingerprint,),
            ).fetchone()[0]

            if first_sheet == current_sheet:
                counts["within_sheet_duplicates"] += 1
            else:
                counts["cross_sheet_duplicates"] += 1

    connection.commit()
    return counts


def write_reports(result):
    """Write machine-readable and human-readable outputs."""
    OUTPUT_JSON.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    OUTPUT_JSON.write_text(
        json.dumps(
            result,
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    metrics = result["metrics"]

    lines = [
        "# CommerceLake - Transaction Audit",
        "",
        "## Summary",
        "",
        f"- Source rows: {result['total_rows']:,}",
        (f"- Within-sheet duplicates: {metrics.get('within_sheet_duplicates', 0):,}"),
        (f"- Cross-sheet duplicates: {metrics.get('cross_sheet_duplicates', 0):,}"),
        (f"- Cancellation invoice lines: {metrics.get('cancellation', 0):,}"),
        (f"- Negative quantity lines: {metrics.get('negative_quantity', 0):,}"),
        (
            "- Cancellation + negative quantity: "
            f"{metrics.get('cancellation_and_negative_quantity', 0):,}"
        ),
        (
            "- Negative quantity without cancellation: "
            f"{metrics.get('negative_quantity_without_cancellation', 0):,}"
        ),
        (
            "- Cancellation without negative quantity: "
            f"{metrics.get('cancellation_without_negative_quantity', 0):,}"
        ),
        (f"- Zero-price lines: {metrics.get('zero_price', 0):,}"),
        (f"- Negative-price lines: {metrics.get('negative_price', 0):,}"),
        "",
        "## Methodology",
        "",
        ("Rows are compared using SHA256 fingerprints of normalized source values."),
        "",
        (
            "The first occurrence of each fingerprint "
            "determines whether later matches are "
            "within-sheet or cross-sheet duplicates."
        ),
        "",
        "Source records are preserved without modification.",
        "",
        "## Interpretation",
        "",
        ("Cancellation invoice prefixes and negative quantities are separate signals."),
        "",
        (
            "These counts do not establish final business "
            "definitions for returns or net revenue."
        ),
        "",
    ]

    OUTPUT_MD.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


if __name__ == "__main__":
    results = audit()

    print("\n=== FINAL SUMMARY ===")

    for key, value in results["metrics"].items():
        print(f"{key}: {value:,}")

    print("\nGenerated reports:")
    print(OUTPUT_JSON)
    print(OUTPUT_MD)
