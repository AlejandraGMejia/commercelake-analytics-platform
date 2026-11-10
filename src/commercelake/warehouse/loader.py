"""Transactional publication of the active Gold run to PostgreSQL."""

import argparse
import getpass
import json
import os
import re
from decimal import Decimal
from pathlib import Path

import psycopg
import pyarrow.parquet as pq
from psycopg import sql

from commercelake.gold.dimensions import (
    COUNTRY_SCHEMA,
    CUSTOMER_SCHEMA,
    DATE_SCHEMA,
    PRODUCT_SCHEMA,
)
from commercelake.gold.fact_sales import FACT_SALES_SCHEMA
from commercelake.gold.monthly_mart import MONTHLY_MART_SCHEMA

DIMENSIONS = {
    "dim_product": PRODUCT_SCHEMA,
    "dim_customer": CUSTOMER_SCHEMA,
    "dim_country": COUNTRY_SCHEMA,
    "dim_date": DATE_SCHEMA,
}
TABLE_SCHEMAS = {
    **DIMENSIONS,
    "fact_sales": FACT_SALES_SCHEMA,
    "mart_monthly_sales": MONTHLY_MART_SCHEMA,
}
LOAD_ORDER = (
    "dim_product",
    "dim_customer",
    "dim_country",
    "dim_date",
    "fact_sales",
    "mart_monthly_sales",
)
RUN_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")


def read_json(path):
    """Read a JSON object from disk."""
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"Expected JSON object: {path}")
    return value


def safe_file(run_directory, relative_path):
    """Reject paths outside the selected Gold run."""
    if not isinstance(relative_path, str):
        raise TypeError("Invalid Gold inventory path")

    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"Unsafe Gold path: {relative_path}")

    resolved = (run_directory / relative).resolve()
    if not resolved.is_relative_to(run_directory.resolve()):
        raise ValueError(f"Gold path escapes run directory: {relative_path}")

    if not resolved.is_file():
        raise FileNotFoundError(resolved)

    return resolved


def load_manifest(root):
    """Select and validate the active Gold manifest."""
    pointer = read_json(root / "current.json")

    if pointer.get("layer") != "gold":
        raise ValueError("Active pointer is not Gold")

    run_id = pointer.get("run_id")
    if not isinstance(run_id, str) or not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError("Invalid Gold run identifier")

    run_directory = root / "runs" / run_id
    manifest = read_json(run_directory / "manifest.json")

    if manifest.get("run_id") != run_id:
        raise ValueError("Gold manifest run identifier mismatch")
    if manifest.get("layer") != "gold":
        raise ValueError("Gold manifest layer mismatch")
    if manifest.get("complete_source") is not True:
        raise ValueError("Incomplete Gold source cannot be published")

    return run_id, run_directory, manifest


def inventory(manifest):
    """Return declared files and expected row counts per table."""
    result = {}

    for table in DIMENSIONS:
        entry = manifest["dimensions"][table]
        result[table] = [(entry["path"], int(entry["rows"]))]

    facts = manifest["fact_sales"]
    result["fact_sales"] = [
        (entry["path"], int(entry["rows"])) for entry in facts["files"]
    ]

    mart = manifest["marts"]["mart_monthly_sales"]
    result["mart_monthly_sales"] = [(mart["path"], int(mart["rows"]))]

    if not result["fact_sales"]:
        raise ValueError("Gold fact inventory is empty")

    declared = [path for files in result.values() for path, _ in files]
    if len(declared) != len(set(declared)):
        raise ValueError("Duplicate Gold inventory paths")

    if sum(rows for _, rows in result["fact_sales"]) != int(facts["total_rows"]):
        raise ValueError("Gold fact inventory total mismatch")

    if int(mart["source_fact_rows"]) != int(facts["total_rows"]):
        raise ValueError("Gold mart source count mismatch")

    return result


def validate_files(run_directory, files_by_table):
    """Check Parquet schemas, inventory sizes and row counts."""
    for table, files in files_by_table.items():
        expected_schema = TABLE_SCHEMAS[table]

        for relative_path, expected_rows in files:
            path = safe_file(run_directory, relative_path)
            parquet = pq.ParquetFile(path)

            if not parquet.schema_arrow.equals(expected_schema, check_metadata=False):
                raise ValueError(f"Gold schema mismatch: {relative_path}")

            if parquet.metadata.num_rows != expected_rows:
                raise ValueError(f"Gold row count mismatch: {relative_path}")

            print(f"Validated {relative_path}: {expected_rows:,} rows")


def create_staging(cursor):
    """Create staging tables without warehouse-generated identifiers."""
    for table in LOAD_ORDER:
        cursor.execute(
            sql.SQL(
                "CREATE TEMP TABLE {} "
                "(LIKE analytics.{} INCLUDING DEFAULTS "
                "INCLUDING CONSTRAINTS) "
                "ON COMMIT DROP"
            ).format(
                sql.Identifier(f"stage_{table}"),
                sql.Identifier(table),
            )
        )

        if table == "fact_sales":
            cursor.execute(
                sql.SQL("ALTER TABLE {} DROP COLUMN fact_id").format(
                    sql.Identifier("stage_fact_sales")
                )
            )


def copy_table(cursor, table, run_directory, files, batch_size):
    """Stream Parquet rows into a temporary PostgreSQL table."""
    columns = TABLE_SCHEMAS[table].names
    copied = 0

    statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(f"stage_{table}"),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
    )

    with cursor.copy(statement) as copy:
        for relative_path, expected_rows in files:
            parquet = pq.ParquetFile(safe_file(run_directory, relative_path))
            file_rows = 0

            for batch in parquet.iter_batches(batch_size=batch_size):
                selected = batch.select(columns)

                for row in selected.to_pylist():
                    copy.write_row(tuple(row[name] for name in columns))

                file_rows += batch.num_rows
                copied += batch.num_rows

            if file_rows != expected_rows:
                raise ValueError(f"Rows changed while loading {relative_path}")

    print(f"Loaded staging {table}: {copied:,} rows")
    return copied


def table_metrics(cursor, table):
    """Return exact staged or published table row count."""
    cursor.execute(
        sql.SQL("SELECT COUNT(*) FROM {}").format(
            sql.Identifier(table)
            if table.startswith("stage_")
            else sql.Identifier("analytics", table)
        )
    )
    return cursor.fetchone()[0]


def check_staging(cursor, manifest, files_by_table):
    """Reconcile staged data against the Gold manifest."""
    for table, files in files_by_table.items():
        expected = sum(rows for _, rows in files)
        actual = table_metrics(cursor, f"stage_{table}")

        if actual != expected:
            raise ValueError(f"{table}: expected {expected}, found {actual}")

    cursor.execute(
        """
        SELECT transaction_type, COUNT(*),
               COALESCE(SUM(net_transaction_amount), 0)
        FROM stage_fact_sales
        GROUP BY transaction_type
        """
    )

    actual_counts = {}
    actual_amounts = {}
    for kind, count, amount in cursor.fetchall():
        actual_counts[kind] = count
        actual_amounts[kind] = amount

    expected_counts = manifest["fact_sales"]["transaction_counts"]
    expected_amounts = manifest["fact_sales"]["transaction_amounts"]

    if actual_counts != {key: int(value) for key, value in expected_counts.items()}:
        raise ValueError("Gold transaction classification mismatch")

    for kind, amount in expected_amounts.items():
        if actual_amounts.get(kind) != Decimal(str(amount)):
            raise ValueError(f"Gold financial mismatch: {kind}")

    cursor.execute(
        """
        SELECT COALESCE(SUM(net_transaction_amount), 0)
        FROM stage_fact_sales
        """
    )
    fact_net = cursor.fetchone()[0]

    cursor.execute(
        """
        SELECT COALESCE(SUM(net_amount), 0),
               COALESCE(SUM(transaction_count), 0)
        FROM stage_mart_monthly_sales
        """
    )
    mart_net, mart_rows = cursor.fetchone()

    expected_net = Decimal(str(manifest["fact_sales"]["net_transaction_amount"]))
    declared_mart_net = Decimal(
        str(manifest["marts"]["mart_monthly_sales"]["net_amount"])
    )
    expected_rows = int(manifest["fact_sales"]["total_rows"])

    if not (fact_net == mart_net == expected_net == declared_mart_net):
        raise ValueError("Gold financial reconciliation failed")

    if mart_rows != expected_rows:
        raise ValueError("Gold monthly transaction reconciliation failed")

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM stage_fact_sales
        WHERE (
            transaction_type = 'review'
            AND net_transaction_amount IS NOT NULL
        ) OR (
            transaction_type <> 'review'
            AND net_transaction_amount IS NULL
        )
        """
    )
    if cursor.fetchone()[0]:
        raise ValueError("Invalid staged transaction amounts")

    print("Staging reconciliation: PASSED")
    print(f"Fact rows: {expected_rows:,}")
    print(f"Net amount: {fact_net}")
    print(f"Monthly mart rows: {len(files_by_table['mart_monthly_sales'])} file")


def publish(cursor, files_by_table):
    """Replace published tables within the current transaction."""
    cursor.execute(
        """
        TRUNCATE TABLE
            analytics.fact_sales,
            analytics.mart_monthly_sales,
            analytics.dim_product,
            analytics.dim_customer,
            analytics.dim_country,
            analytics.dim_date
        RESTART IDENTITY
        """
    )

    for table in LOAD_ORDER:
        columns = TABLE_SCHEMAS[table].names
        identifiers = sql.SQL(", ").join(map(sql.Identifier, columns))

        cursor.execute(
            sql.SQL("INSERT INTO analytics.{} ({}) SELECT {} FROM {}").format(
                sql.Identifier(table),
                identifiers,
                identifiers,
                sql.Identifier(f"stage_{table}"),
            )
        )

        expected = sum(rows for _, rows in files_by_table[table])
        actual = table_metrics(cursor, table)

        if actual != expected:
            raise ValueError(f"Published {table} count mismatch")

        print(f"Published {table}: {actual:,} rows")


def connect(args):
    """Open a password-authenticated PostgreSQL connection."""
    password = os.getenv("COMMERCE_DB_PASSWORD")

    if password is None:
        password = getpass.getpass("PostgreSQL password for commercelake_app: ")

    return psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=password,
        connect_timeout=10,
    )


def run(args):
    """Validate, stage and atomically publish one active Gold run."""
    root = Path(args.gold_root)
    run_id, run_directory, manifest = load_manifest(root)
    files_by_table = inventory(manifest)

    print(f"Active Gold run: {run_id}")
    print("Validating Gold inventory...")
    validate_files(run_directory, files_by_table)

    print("Connecting to PostgreSQL...")

    with (
        connect(args) as connection,
        connection.transaction(),
        connection.cursor() as cursor,
    ):
        cursor.execute("SELECT pg_advisory_xact_lock(640621, 6)")

        create_staging(cursor)

        for table in LOAD_ORDER:
            copy_table(
                cursor,
                table,
                run_directory,
                files_by_table[table],
                args.batch_size,
            )

        check_staging(cursor, manifest, files_by_table)

        active = read_json(root / "current.json")
        if active.get("run_id") != run_id:
            raise ValueError("Active Gold run changed during publication")

        publish(cursor, files_by_table)

    print("=== WAREHOUSE PUBLICATION COMPLETE ===")
    print(f"Gold run: {run_id}")
    print("PostgreSQL transaction: COMMITTED")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Publish active Gold to PostgreSQL")
    parser.add_argument(
        "--gold-root",
        default="data/processed/retail_gold",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument(
        "--database",
        default="commercelake_warehouse",
    )
    parser.add_argument(
        "--user",
        default="commercelake_app",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
    )
    args = parser.parse_args()

    if args.batch_size < 1:
        parser.error("--batch-size must be positive")

    run(args)


if __name__ == "__main__":
    main()
