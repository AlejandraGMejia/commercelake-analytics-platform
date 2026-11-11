"""Explicit, budget-controlled CLI for CommerceLake staging publication."""

import argparse
import json
from pathlib import Path

from commercelake.cloud.bigquery_orchestrator import (
    production_bigquery_runner,
    publish_gold_to_staging,
)
from commercelake.cloud.bigquery_publisher import (
    DEFAULT_GOLD_ROOT,
    build_plan,
    plan_as_dict,
)
from commercelake.cloud.bigquery_staging_insert import validate_query_budget

ALLOWED_PROJECT = "commercelake-analytics"
ALLOWED_DATASET = "commercelake_gold"
ALLOWED_LOCATION = "US"

EXECUTION_CONFIRMATION = (
    "PUBLISH_COMMERCELAKE_STAGING"
)


def build_parser() -> argparse.ArgumentParser:
    """Build a CLI with mutually exclusive validation and execution modes."""
    parser = argparse.ArgumentParser(
        description=(
            "Validate or explicitly execute CommerceLake "
            "Gold-to-BigQuery staging publication."
        )
    )

    parser.add_argument(
        "--gold-root",
        type=Path,
        default=DEFAULT_GOLD_ROOT,
    )
    parser.add_argument(
        "--project-id",
        default=ALLOWED_PROJECT,
    )
    parser.add_argument(
        "--dataset",
        default=ALLOWED_DATASET,
    )
    parser.add_argument(
        "--location",
        default=ALLOWED_LOCATION,
    )
    parser.add_argument(
        "--maximum-bytes-billed",
        type=int,
        required=True,
    )

    mode = parser.add_mutually_exclusive_group(required=True)

    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the local publication plan without cloud writes.",
    )
    mode.add_argument(
        "--execute",
        action="store_true",
        help="Execute real BigQuery staging publication.",
    )

    parser.add_argument(
        "--confirm",
        default=None,
        help="Required confirmation phrase for real execution.",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """Validate inputs before enabling any real BigQuery operations."""
    args = build_parser().parse_args(argv)

    if args.project_id != ALLOWED_PROJECT:
        raise ValueError("Unauthorized GCP project")

    if args.dataset != ALLOWED_DATASET:
        raise ValueError("Unauthorized BigQuery dataset")

    if args.location != ALLOWED_LOCATION:
        raise ValueError("Unauthorized BigQuery location")

    budget = validate_query_budget(args.maximum_bytes_billed)

    if args.dry_run:
        if args.confirm is not None:
            raise ValueError(
                "--confirm is not accepted in dry-run mode"
            )

        plan = build_plan(
            gold_root=args.gold_root,
            project_id=args.project_id,
            dataset=args.dataset,
            location=args.location,
        )

        print(json.dumps(plan_as_dict(plan), indent=2))
        print()
        print("PASS: Local staging publication plan validated.")
        print("No cloud commands were executed.")
        return 0

    if args.confirm != EXECUTION_CONFIRMATION:
        raise ValueError(
            "Real execution requires --confirm "
            f"{EXECUTION_CONFIRMATION}"
        )

    print("=== AUTHORIZED STAGING EXECUTION ===", flush=True)
    print(f"Project: {args.project_id}", flush=True)
    print(f"Dataset: {args.dataset}", flush=True)
    print(f"Location: {args.location}", flush=True)
    print(f"Maximum query bytes billed: {budget}", flush=True)

    result = publish_gold_to_staging(
        project_id=args.project_id,
        gold_root=args.gold_root,
        dataset=args.dataset,
        location=args.location,
        runner=production_bigquery_runner(),
        maximum_bytes_billed=budget,
    )

    print()
    print("PASS: Staging publication completed.")
    print(f"Run ID: {result.run_id}")
    print(f"Total rows: {result.total_rows:,}")

    for table, rows in sorted(result.table_row_counts.items()):
        print(f"{table}: {rows:,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
