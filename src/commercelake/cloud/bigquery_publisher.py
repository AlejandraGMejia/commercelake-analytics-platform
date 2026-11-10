"""Plan and validate publication of the active Gold run to BigQuery."""

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path

from commercelake.config import get_gcp_project_id
from commercelake.warehouse.loader import (
    LOAD_ORDER,
    inventory,
    load_manifest,
    validate_files,
)

DEFAULT_GOLD_ROOT = Path("data/processed/retail_gold")
DEFAULT_DATASET = "commercelake_gold"
DEFAULT_LOCATION = "US"

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PROJECT_PATTERN = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")


@dataclass(frozen=True)
class PublicationPlan:
    """Validated BigQuery publication plan."""

    project_id: str
    dataset: str
    location: str
    run_id: str
    run_directory: Path
    files_by_table: dict

    @property
    def total_files(self) -> int:
        """Return the total number of source Parquet files."""
        return sum(len(files) for files in self.files_by_table.values())

    @property
    def total_rows(self) -> int:
        """Return the sum of the declared table row counts."""
        return sum(rows for files in self.files_by_table.values() for _, rows in files)


def validate_identifiers(project_id: str, dataset: str) -> None:
    """Reject malformed BigQuery project and dataset identifiers."""
    if not PROJECT_PATTERN.fullmatch(project_id):
        raise ValueError(f"Invalid GCP project ID: {project_id}")

    if not IDENTIFIER_PATTERN.fullmatch(dataset):
        raise ValueError(f"Invalid BigQuery dataset ID: {dataset}")


def build_plan(
    gold_root: Path,
    project_id: str,
    dataset: str = DEFAULT_DATASET,
    location: str = DEFAULT_LOCATION,
) -> PublicationPlan:
    """Validate the active Gold run and construct its publication plan."""
    validate_identifiers(project_id, dataset)

    if location != DEFAULT_LOCATION:
        raise ValueError("CommerceLake BigQuery location must be US")

    gold_root = Path(gold_root)

    run_id, run_directory, manifest = load_manifest(gold_root)
    files_by_table = inventory(manifest)

    validate_files(run_directory, files_by_table)

    if set(files_by_table) != set(LOAD_ORDER):
        raise ValueError("Gold inventory does not match expected tables")

    return PublicationPlan(
        project_id=project_id,
        dataset=dataset,
        location=location,
        run_id=run_id,
        run_directory=run_directory,
        files_by_table=files_by_table,
    )


def plan_as_dict(plan: PublicationPlan) -> dict:
    """Return a JSON-serializable summary of the publication plan."""
    tables = []

    for table in LOAD_ORDER:
        files = plan.files_by_table[table]
        tables.append(
            {
                "table": table,
                "destination": (f"{plan.project_id}:{plan.dataset}.{table}"),
                "source_files": len(files),
                "expected_rows": sum(rows for _, rows in files),
            }
        )

    return {
        "mode": "dry-run",
        "project_id": plan.project_id,
        "dataset": plan.dataset,
        "location": plan.location,
        "gold_run_id": plan.run_id,
        "total_source_files": plan.total_files,
        "tables": tables,
    }


def main() -> int:
    """Validate the selected Gold run without modifying cloud resources."""
    parser = argparse.ArgumentParser(
        description="Validate CommerceLake Gold publication to BigQuery."
    )
    parser.add_argument(
        "--gold-root",
        type=Path,
        default=DEFAULT_GOLD_ROOT,
    )
    parser.add_argument(
        "--project-id",
        default=None,
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
    )
    parser.add_argument(
        "--location",
        default=DEFAULT_LOCATION,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        required=True,
        help="Validate and print the publication plan; do not upload data.",
    )

    args = parser.parse_args()

    project_id = args.project_id or get_gcp_project_id()

    plan = build_plan(
        gold_root=args.gold_root,
        project_id=project_id,
        dataset=args.dataset,
        location=args.location,
    )

    print(json.dumps(plan_as_dict(plan), indent=2))
    print("\nPASS: Gold publication plan validated.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
