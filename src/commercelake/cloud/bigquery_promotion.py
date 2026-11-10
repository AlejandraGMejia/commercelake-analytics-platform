"""Plan versioned BigQuery Gold publication without executing commands."""

from dataclasses import dataclass

from commercelake.cloud.bigquery_publisher import (
    DEFAULT_LOCATION,
    validate_identifiers,
)
from commercelake.cloud.bigquery_staging import (
    RUN_ID_PATTERN,
    staging_table_name,
)
from commercelake.warehouse.loader import LOAD_ORDER


@dataclass(frozen=True)
class PromotionTable:
    """Source staging table and immutable-version destination."""

    logical_name: str
    staging_name: str
    version_name: str
    expected_rows: int


@dataclass(frozen=True)
class PromotionPlan:
    """Validated plan for publishing one complete Gold version."""

    project_id: str
    dataset: str
    location: str
    run_id: str
    tables: tuple[PromotionTable, ...]

    @property
    def total_rows(self) -> int:
        """Return expected rows across all six tables."""
        return sum(table.expected_rows for table in self.tables)


def version_table_name(table: str, run_id: str) -> str:
    """Generate a unique destination name using the complete run ID."""
    staging_table_name(table, run_id)

    return f"gold_{table}_{run_id}"


def build_promotion_plan(
    *,
    project_id: str,
    dataset: str,
    run_id: str,
    expected_counts: dict[str, int],
    location: str = DEFAULT_LOCATION,
) -> PromotionPlan:
    """Validate inputs and describe six versioned destinations.

    This function never contacts BigQuery or changes cloud resources.
    """
    validate_identifiers(project_id, dataset)

    if location != DEFAULT_LOCATION:
        raise ValueError("Unsupported BigQuery location")

    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError("Invalid Gold run ID")

    if set(expected_counts) != set(LOAD_ORDER):
        raise ValueError("Promotion requires expected counts for all six Gold tables")

    tables = []

    for table in LOAD_ORDER:
        rows = expected_counts[table]

        if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
            raise ValueError(f"Invalid expected row count for {table}: {rows}")

        tables.append(
            PromotionTable(
                logical_name=table,
                staging_name=staging_table_name(table, run_id),
                version_name=version_table_name(table, run_id),
                expected_rows=rows,
            )
        )

    return PromotionPlan(
        project_id=project_id,
        dataset=dataset,
        location=location,
        run_id=run_id,
        tables=tuple(tables),
    )
