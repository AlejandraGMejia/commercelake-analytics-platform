"""Generate the Gold analytical report from the active run."""

import json
from collections import Counter
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

GOLD_ROOT = Path("data/processed/retail_gold")
REPORT_PATH = Path("reports/gold_analytics_report.md")
ZERO = Decimal("0.0000")


def load_active_gold():
    """Load the active Gold manifest and monthly mart."""
    pointer = json.loads((GOLD_ROOT / "current.json").read_text(encoding="utf-8"))

    run_id = pointer["run_id"]
    run_directory = GOLD_ROOT / "runs" / run_id

    manifest = json.loads((run_directory / "manifest.json").read_text(encoding="utf-8"))

    if manifest["run_id"] != run_id:
        raise ValueError("Gold pointer mismatch")

    if manifest["layer"] != "gold":
        raise ValueError("Invalid Gold layer")

    if manifest["complete_source"] is not True:
        raise ValueError("Incomplete Gold run")

    mart_info = manifest["marts"]["mart_monthly_sales"]
    mart_path = run_directory / mart_info["path"]

    rows = pq.read_table(mart_path).to_pylist()

    return manifest, mart_info, rows


def fmt_amount(value):
    """Format an amount without assigning a currency."""
    return f"{value:,.4f}"


def main():
    """Validate and document Gold analytical results."""
    manifest, mart_info, rows = load_active_gold()

    fact = manifest["fact_sales"]

    total_rows = sum(row["transaction_count"] for row in rows)

    total_net = sum(
        (row["net_amount"] for row in rows),
        ZERO,
    )

    if total_rows != fact["total_rows"]:
        raise ValueError("Fact-to-mart row mismatch")

    if total_net != Decimal(fact["net_transaction_amount"]):
        raise ValueError("Fact-to-mart financial mismatch")

    if len(rows) != mart_info["rows"]:
        raise ValueError("Monthly mart inventory mismatch")

    counts = Counter()

    for row in rows:
        for kind in (
            "sale",
            "cancellation",
            "return",
            "review",
        ):
            counts[kind] += row[f"{kind}_count"]

    for kind, expected in fact["transaction_counts"].items():
        if counts[kind] != expected:
            raise ValueError(f"Classification mismatch: {kind}")

    annual = {}

    for row in rows:
        year = row["year"]

        if year not in annual:
            annual[year] = {
                "transactions": 0,
                "net": ZERO,
            }

        annual[year]["transactions"] += row["transaction_count"]
        annual[year]["net"] += row["net_amount"]

    top_months = sorted(
        rows,
        key=lambda row: row["net_amount"],
        reverse=True,
    )[:5]

    bottom_months = sorted(
        rows,
        key=lambda row: row["net_amount"],
    )[:5]

    lines = [
        "# Gold Analytics — Sprint 05",
        "",
        "## Executive summary",
        "",
        (
            "CommerceLake transforms the validated Silver "
            "dataset into a versioned Gold analytical layer."
        ),
        "",
        f"- Gold run: `{manifest['run_id']}`",
        f"- Source Silver run: `{manifest['source_silver_run_id']}`",
        f"- Fact rows: {fact['total_rows']:,}",
        f"- Monthly periods: {len(rows)}",
        f"- Net transaction amount: {fmt_amount(total_net)}",
        "- Currency: not explicitly established by the analytical contract",
        "",
        "## Dimensional model",
        "",
        "| Table | Rows |",
        "|---|---:|",
    ]

    for name, info in manifest["dimensions"].items():
        lines.append(f"| `{name}` | {info['rows']:,} |")

    lines.extend(
        [
            "",
            "### Grain and business rules",
            "",
            "- `fact_sales`: one row per Silver transaction line.",
            "- All source rows are preserved.",
            (
                "- Product, customer, country and date "
                "dimensions support analytical joins."
            ),
            (
                "- Unknown dimension members preserve "
                "referential integrity where applicable."
            ),
            "- Sales, cancellations and returns contribute signed financial amounts.",
            "- Review transactions are excluded from net financial totals.",
            "- Monthly customer counts exclude missing customer identifiers.",
            "- Counts refer to transaction lines, not distinct invoices.",
            "",
            "## Transaction classification",
            "",
            "| Type | Lines |",
            "|---|---:|",
        ]
    )

    for kind in (
        "sale",
        "cancellation",
        "return",
        "review",
    ):
        lines.append(f"| {kind} | {counts[kind]:,} |")

    lines.extend(
        [
            "",
            "## Annual performance",
            "",
            "| Year | Transaction lines | Net amount |",
            "|---|---:|---:|",
        ]
    )

    for year, values in sorted(annual.items()):
        lines.append(
            f"| {year} | {values['transactions']:,} | {fmt_amount(values['net'])} |"
        )

    lines.extend(
        [
            "",
            (
                "Annual figures should not be compared "
                "without considering coverage: 2009 "
                "contains only December, and the final "
                "month of 2011 is partially observed."
            ),
            "",
            "## Highest-net months",
            "",
            "| Month | Net amount | Sale lines |",
            "|---|---:|---:|",
        ]
    )

    for row in top_months:
        lines.append(
            f"| {row['year_month']} | "
            f"{fmt_amount(row['net_amount'])} | "
            f"{row['sale_count']:,} |"
        )

    lines.extend(
        [
            "",
            "## Lowest-net months",
            "",
            "| Month | Net amount | Sale lines |",
            "|---|---:|---:|",
        ]
    )

    for row in bottom_months:
        lines.append(
            f"| {row['year_month']} | "
            f"{fmt_amount(row['net_amount'])} | "
            f"{row['sale_count']:,} |"
        )

    lines.extend(
        [
            "",
            "## Analytical interpretation",
            "",
            (
                "- November 2010 and November 2011 "
                "are among the strongest observed months."
            ),
            (
                "- This pattern warrants further "
                "seasonality analysis rather than "
                "establishing seasonality conclusively."
            ),
            (
                "- December 2011 has partial coverage "
                "and must not be treated as a complete "
                "monthly performance observation."
            ),
            (
                "- Net amounts are signed transaction "
                "measures, not verified accounting profit."
            ),
            "",
            "## Data quality and reconciliation",
            "",
            f"- Silver-to-Gold fact rows: {fact['total_rows']:,}",
            f"- Mart source rows: {total_rows:,}",
            f"- Fact net amount: {fmt_amount(Decimal(fact['net_transaction_amount']))}",
            f"- Mart net amount: {fmt_amount(total_net)}",
            "- Row reconciliation: PASSED",
            "- Financial reconciliation: PASSED",
            "- Transaction classification: PASSED",
            "",
            "## Versioning and reproducibility",
            "",
            "- Each Gold build receives a unique run ID.",
            (
                "- The manifest inventories dimensions, "
                "fact partitions and analytical marts."
            ),
            "- The active version is referenced through `current.json`.",
            "- The pointer is updated only after the build and validations succeed.",
            "- Historical completed runs remain available for reproducibility.",
            "",
            "## Next engineering steps",
            "",
            "- Add SQL analytical queries and warehouse consumption.",
            "- Introduce orchestration and incremental processing.",
            "- Extend automated observability and operational monitoring.",
            "",
        ]
    )

    REPORT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    REPORT_PATH.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )

    print("=== GOLD REPORT GENERATED ===")
    print("Path:", REPORT_PATH)
    print("Gold run:", manifest["run_id"])
    print("Months:", len(rows))
    print("Rows reconciled:", total_rows)
    print("Net amount:", total_net)
    print("Validation: PASSED")


if __name__ == "__main__":
    main()
