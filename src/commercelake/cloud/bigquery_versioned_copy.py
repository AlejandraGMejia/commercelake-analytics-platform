"""Build non-overwriting BigQuery copy commands for Gold versions."""

from dataclasses import dataclass

from commercelake.cloud.bigquery_promotion import PromotionPlan
from commercelake.cloud.bigquery_promotion_preflight import (
    validate_promotion_plan,
)
from commercelake.cloud.bigquery_publisher import DEFAULT_LOCATION


@dataclass(frozen=True)
class VersionedCopyCommand:
    """A validated staging-to-version copy command."""

    logical_name: str
    source_table: str
    destination_table: str
    command: tuple[str, ...]


def build_versioned_copy_command(
    *,
    project_id: str,
    dataset: str,
    source_table: str,
    destination_table: str,
    location: str = DEFAULT_LOCATION,
) -> list[str]:
    """Build a BigQuery copy command that cannot overwrite a destination."""
    from commercelake.cloud.bigquery_publisher import validate_identifiers
    from commercelake.cloud.bigquery_validation import TABLE_PATTERN

    validate_identifiers(project_id, dataset)

    if location != DEFAULT_LOCATION:
        raise ValueError("Unsupported BigQuery location")

    for table in (source_table, destination_table):
        if not TABLE_PATTERN.fullmatch(table):
            raise ValueError(f"Invalid BigQuery table name: {table}")

    if source_table == destination_table:
        raise ValueError("Source and destination must be different")

    source = f"{project_id}:{dataset}.{source_table}"
    destination = f"{project_id}:{dataset}.{destination_table}"

    return [
        "bq",
        f"--location={location}",
        "cp",
        "--no_clobber",
        source,
        destination,
    ]


def build_versioned_copy_plan(
    plan: PromotionPlan,
) -> tuple[VersionedCopyCommand, ...]:
    """Build six safe copy commands from a canonical promotion plan."""
    validate_promotion_plan(plan)

    commands = []

    for table in plan.tables:
        command = build_versioned_copy_command(
            project_id=plan.project_id,
            dataset=plan.dataset,
            source_table=table.staging_name,
            destination_table=table.version_name,
            location=plan.location,
        )

        commands.append(
            VersionedCopyCommand(
                logical_name=table.logical_name,
                source_table=table.staging_name,
                destination_table=table.version_name,
                command=tuple(command),
            )
        )

    return tuple(commands)
