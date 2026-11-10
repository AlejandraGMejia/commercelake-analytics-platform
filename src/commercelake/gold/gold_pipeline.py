"""Versioned, validated Gold analytics pipeline."""

import argparse
import json
import shutil
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from commercelake.gold.dimensions import DimensionCollector
from commercelake.gold.fact_sales import (
    FACT_SALES_SCHEMA,
    transform_fact_batch,
    validate_fact_dimensions,
)
from commercelake.gold.monthly_mart import (
    MONTHLY_MART_SCHEMA,
    MonthlySalesCollector,
)
from commercelake.transformation.silver_schema import SILVER_SCHEMA

DEFAULT_SILVER_ROOT = Path("data/processed/retail_silver")
DEFAULT_GOLD_ROOT = Path("data/processed/retail_gold")

TRANSACTION_TYPES = (
    "sale",
    "cancellation",
    "return",
    "review",
)

ZERO = Decimal("0.0000")


def read_json(path):
    """Read a JSON document."""
    with Path(path).open(encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path, payload):
    """Write a JSON document with deterministic formatting."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as handle:
        json.dump(
            payload,
            handle,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        )
        handle.write("\n")


def publish_pointer(root, payload):
    """Atomically publish the active Gold run pointer."""
    temporary = root / f".current-{uuid4().hex}.json"

    try:
        write_json(temporary, payload)
        temporary.replace(root / "current.json")
    finally:
        temporary.unlink(missing_ok=True)


def safe_source_path(run_directory, relative_path):
    """Resolve a manifest path without directory traversal."""
    if not isinstance(relative_path, str):
        raise TypeError("Unsafe Silver file path")

    path = PurePosixPath(relative_path)

    if (
        path.is_absolute()
        or not path.parts
        or any(part in (".", "..") for part in path.parts)
        or path.suffix != ".parquet"
    ):
        raise ValueError(f"Unsafe Silver file path: {relative_path}")

    resolved = (run_directory / Path(*path.parts)).resolve()

    if not resolved.is_relative_to(run_directory.resolve()):
        raise ValueError(f"Silver path escapes run: {relative_path}")

    return resolved


def load_silver_source(silver_root):
    """Load and validate the active Silver inventory."""
    pointer = read_json(silver_root / "current.json")
    run_id = pointer["run_id"]

    if not isinstance(run_id, str) or not run_id:
        raise ValueError("Invalid active Silver run ID")

    if Path(run_id).name != run_id or run_id in (".", ".."):
        raise ValueError("Unsafe Silver run ID")

    run_directory = silver_root / "runs" / run_id
    manifest = read_json(run_directory / "manifest.json")

    if manifest.get("run_id") != run_id:
        raise ValueError("Silver pointer and manifest disagree")

    if manifest.get("layer") != "silver":
        raise ValueError("Active source is not Silver")

    if manifest.get("complete_source") is not True:
        raise ValueError("Silver source is not marked complete")

    files = manifest.get("files")

    if not isinstance(files, list) or not files:
        raise ValueError("Silver manifest has no files")

    paths = [item["path"] for item in files]

    if len(paths) != len(set(paths)):
        raise ValueError("Duplicate paths in Silver manifest")

    inventory_rows = 0
    partition_rows = Counter()

    for item in files:
        path = safe_source_path(
            run_directory,
            item["path"],
        )

        if not path.is_file():
            raise FileNotFoundError(path)

        parquet = pq.ParquetFile(path)

        if not parquet.schema_arrow.equals(
            SILVER_SCHEMA,
            check_metadata=False,
        ):
            raise ValueError(f"Invalid Silver schema: {path}")

        actual_rows = parquet.metadata.num_rows

        if actual_rows != item["rows"]:
            raise ValueError(f"Silver row mismatch: {path}")

        inventory_rows += actual_rows

        partition = str(PurePosixPath(item["path"]).parent)
        partition_rows[partition] += actual_rows

    if inventory_rows != manifest["total_rows"]:
        raise ValueError("Silver inventory total does not reconcile")

    if dict(partition_rows) != manifest["partitions"]:
        raise ValueError("Silver partition totals do not reconcile")

    return run_directory, manifest


def iter_silver_batches(run_directory, manifest, batch_size):
    """Yield Silver batches in manifest order."""
    for item in manifest["files"]:
        path = safe_source_path(
            run_directory,
            item["path"],
        )
        parquet = pq.ParquetFile(path)

        for batch in parquet.iter_batches(batch_size=batch_size):
            if not batch.schema.equals(
                SILVER_SCHEMA,
                check_metadata=False,
            ):
                raise ValueError(f"Silver batch schema mismatch: {path}")

            yield item["path"], batch


def build_dimensions(run_directory, manifest, batch_size):
    """Collect dimensions without loading the full source."""
    collector = DimensionCollector()
    observed_rows = 0

    for _, batch in iter_silver_batches(
        run_directory,
        manifest,
        batch_size,
    ):
        collector.consume(batch)
        observed_rows += batch.num_rows

    if observed_rows != manifest["total_rows"]:
        raise ValueError("Dimension scan row count mismatch")

    dimensions = collector.build_all()

    key_columns = {
        "dim_product": "product_key",
        "dim_customer": "customer_key",
        "dim_country": "country_key",
        "dim_date": "date_key",
    }

    for name, table in dimensions.items():
        values = table.column(key_columns[name]).to_pylist()

        if None in values or len(values) != len(set(values)):
            raise ValueError(f"Invalid dimension keys: {name}")

    return dimensions


def write_dimensions(run_directory, dimensions):
    """Persist validated dimensional tables."""
    inventory = {}

    for name, table in dimensions.items():
        relative_path = f"dimensions/{name}.parquet"
        destination = run_directory / relative_path
        destination.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        pq.write_table(
            table,
            destination,
            compression="zstd",
        )

        if pq.read_metadata(destination).num_rows != table.num_rows:
            raise ValueError(f"Dimension write mismatch: {name}")

        inventory[name] = {
            "path": relative_path,
            "rows": table.num_rows,
        }

    return inventory


def financial_summary(batch, field):
    """Aggregate exact financial measures by classification."""
    amounts = dict.fromkeys(
        TRANSACTION_TYPES,
        ZERO,
    )
    counts = Counter()

    data = batch.select(["transaction_type", field]).to_pydict()

    for kind, amount in zip(
        data["transaction_type"],
        data[field],
        strict=True,
    ):
        if kind not in amounts:
            raise ValueError(f"Unexpected transaction type: {kind}")

        counts[kind] += 1

        if kind == "review":
            if field == "net_transaction_amount" and amount is not None:
                raise ValueError("Review transaction has a net amount")
            continue

        if amount is None:
            raise ValueError(f"Missing financial amount for {kind}")

        amounts[kind] += amount

    return counts, amounts


def build_facts(
    source_directory,
    source_manifest,
    destination,
    dimensions,
    batch_size,
    monthly_collector=None,
):
    """Write Gold facts and reconcile source measures."""
    fact_inventory = []
    source_counts = Counter()
    gold_counts = Counter()

    source_amounts = dict.fromkeys(
        TRANSACTION_TYPES,
        ZERO,
    )
    gold_amounts = dict.fromkeys(
        TRANSACTION_TYPES,
        ZERO,
    )

    source_rows = 0
    gold_rows = 0
    partition_rows = Counter()

    for file_number, (source_path, batch) in enumerate(
        iter_silver_batches(
            source_directory,
            source_manifest,
            batch_size,
        )
    ):
        fact = transform_fact_batch(batch)

        if not fact.schema.equals(
            FACT_SALES_SCHEMA,
            check_metadata=False,
        ):
            raise ValueError("Gold fact schema mismatch")

        if fact.num_rows != batch.num_rows:
            raise ValueError("Gold transformation changed row count")

        validate_fact_dimensions(
            fact,
            dimensions,
        )

        original_count, original_amount = financial_summary(
            batch,
            "gross_amount",
        )
        fact_count, fact_amount = financial_summary(
            fact,
            "net_transaction_amount",
        )

        source_counts.update(original_count)
        gold_counts.update(fact_count)

        for kind in TRANSACTION_TYPES:
            source_amounts[kind] += original_amount[kind]
            gold_amounts[kind] += fact_amount[kind]

        if monthly_collector is not None:
            monthly_collector.consume(fact)

        partition = str(PurePosixPath(source_path).parent)

        relative_path = f"fact_sales/{partition}/part-{file_number:05d}.parquet"

        output_path = destination / relative_path
        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        pq.write_table(
            pa.Table.from_batches([fact]),
            output_path,
            compression="zstd",
        )

        written_rows = pq.read_metadata(output_path).num_rows

        if written_rows != batch.num_rows:
            raise ValueError("Gold Parquet write mismatch")

        fact_inventory.append(
            {
                "path": relative_path,
                "rows": written_rows,
                "bytes": output_path.stat().st_size,
            }
        )

        source_rows += batch.num_rows
        gold_rows += written_rows
        partition_rows[partition] += written_rows

    if source_rows != source_manifest["total_rows"]:
        raise ValueError("Gold source count mismatch")

    if gold_rows != source_rows:
        raise ValueError("Gold fact count mismatch")

    if dict(partition_rows) != source_manifest["partitions"]:
        raise ValueError("Gold partition count mismatch")

    if source_counts != gold_counts:
        raise ValueError("Gold transaction classification mismatch")

    if source_amounts != gold_amounts:
        raise ValueError("Gold financial reconciliation failed")

    net_amount = sum(
        (
            gold_amounts[kind]
            for kind in (
                "sale",
                "cancellation",
                "return",
            )
        ),
        ZERO,
    )

    return {
        "total_rows": gold_rows,
        "files": fact_inventory,
        "partitions": dict(sorted(partition_rows.items())),
        "transaction_counts": dict(sorted(gold_counts.items())),
        "transaction_amounts": {
            kind: str(gold_amounts[kind]) for kind in TRANSACTION_TYPES
        },
        "net_transaction_amount": str(net_amount),
    }


def write_monthly_mart(
    destination,
    collector,
    fact_inventory,
):
    """Persist and reconcile the monthly sales mart."""
    table = collector.build()
    collector.validate(table)

    if not table.schema.equals(
        MONTHLY_MART_SCHEMA,
        check_metadata=False,
    ):
        raise ValueError("Monthly mart schema mismatch")

    if collector.total_rows != fact_inventory["total_rows"]:
        raise ValueError("Monthly mart fact row count mismatch")

    expected_counts = fact_inventory["transaction_counts"]

    observed_counts = Counter()

    for row in table.to_pylist():
        observed_counts["sale"] += row["sale_count"]
        observed_counts["cancellation"] += row["cancellation_count"]
        observed_counts["return"] += row["return_count"]
        observed_counts["review"] += row["review_count"]

    for kind in TRANSACTION_TYPES:
        if observed_counts[kind] != expected_counts.get(kind, 0):
            raise ValueError(f"Monthly mart classification mismatch: {kind}")

    if collector.total_net_amount != Decimal(fact_inventory["net_transaction_amount"]):
        raise ValueError("Monthly mart financial reconciliation failed")

    relative_path = "marts/mart_monthly_sales.parquet"
    output_path = destination / relative_path

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    pq.write_table(
        table,
        output_path,
        compression="zstd",
    )

    persisted = pq.read_table(output_path)

    if not persisted.equals(
        table,
        check_metadata=False,
    ):
        raise ValueError("Monthly mart persisted data mismatch")

    return {
        "path": relative_path,
        "rows": table.num_rows,
        "bytes": output_path.stat().st_size,
        "source_fact_rows": collector.total_rows,
        "net_amount": str(collector.total_net_amount),
        "first_month": (
            table.column("year_month")[0].as_py() if table.num_rows else None
        ),
        "last_month": (
            table.column("year_month")[table.num_rows - 1].as_py()
            if table.num_rows
            else None
        ),
    }


def build_gold(
    silver_root=DEFAULT_SILVER_ROOT,
    gold_root=DEFAULT_GOLD_ROOT,
    batch_size=10000,
):
    """Build and atomically publish a validated Gold version."""
    if batch_size < 1:
        raise ValueError("Batch size must be positive")

    silver_root = Path(silver_root)
    gold_root = Path(gold_root)

    source_directory, source_manifest = load_silver_source(silver_root)

    run_id = uuid4().hex
    destination = gold_root / "runs" / run_id

    if destination.exists():
        raise FileExistsError(destination)

    destination.mkdir(
        parents=True,
        exist_ok=False,
    )

    try:
        print(
            "Building Gold dimensions...",
            flush=True,
        )

        dimensions = build_dimensions(
            source_directory,
            source_manifest,
            batch_size,
        )

        dimension_inventory = write_dimensions(
            destination,
            dimensions,
        )

        monthly_collector = MonthlySalesCollector()

        print(
            "Building and validating Gold facts...",
            flush=True,
        )

        facts = build_facts(
            source_directory,
            source_manifest,
            destination,
            dimensions,
            batch_size,
            monthly_collector=monthly_collector,
        )

        print(
            "Building and validating monthly mart...",
            flush=True,
        )

        monthly_mart = write_monthly_mart(
            destination,
            monthly_collector,
            facts,
        )

        manifest = {
            "run_id": run_id,
            "layer": "gold",
            "source_silver_run_id": source_manifest["run_id"],
            "created_at": datetime.now(UTC).isoformat(),
            "complete_source": True,
            "dimensions": dimension_inventory,
            "fact_sales": facts,
            "marts": {
                "mart_monthly_sales": monthly_mart,
            },
        }

        write_json(
            destination / "manifest.json",
            manifest,
        )

        publish_pointer(
            gold_root,
            {
                "run_id": run_id,
                "layer": "gold",
            },
        )

    except Exception:
        shutil.rmtree(destination)
        raise

    return manifest


def main():
    """Run the Gold pipeline from the command line."""
    parser = argparse.ArgumentParser(description="Build CommerceLake Gold analytics")

    parser.add_argument(
        "--silver-root",
        type=Path,
        default=DEFAULT_SILVER_ROOT,
    )
    parser.add_argument(
        "--gold-root",
        type=Path,
        default=DEFAULT_GOLD_ROOT,
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
    )

    args = parser.parse_args()

    manifest = build_gold(
        silver_root=args.silver_root,
        gold_root=args.gold_root,
        batch_size=args.batch_size,
    )

    print("\n=== GOLD BUILD COMPLETE ===")
    print("Run ID:", manifest["run_id"])
    print(
        "Source Silver:",
        manifest["source_silver_run_id"],
    )
    print(
        "Fact rows:",
        f"{manifest['fact_sales']['total_rows']:,}",
    )

    print("\nDimensions:")
    for name, item in manifest["dimensions"].items():
        print(f"  {name}: {item['rows']:,}")

    print("\nTransaction counts:")
    for kind, count in manifest["fact_sales"]["transaction_counts"].items():
        print(f"  {kind}: {count:,}")

    print(
        "\nNet transaction amount:",
        manifest["fact_sales"]["net_transaction_amount"],
    )

    mart = manifest["marts"]["mart_monthly_sales"]

    print("\nMonthly sales mart:")
    print("  Months:", mart["rows"])
    print("  First month:", mart["first_month"])
    print("  Last month:", mart["last_month"])
    print("  Source fact rows:", mart["source_fact_rows"])
    print("  Net amount:", mart["net_amount"])


if __name__ == "__main__":
    main()
