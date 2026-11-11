"""Read-only inspection of interrupted BigQuery Gold publications."""

from collections.abc import Callable
from dataclasses import dataclass
from subprocess import CalledProcessError, CompletedProcess
from typing import Literal

from commercelake.cloud.bigquery_cli_errors import (
    is_confirmed_missing,
)
from commercelake.cloud.bigquery_promotion import PromotionPlan
from commercelake.cloud.bigquery_promotion_preflight import (
    validate_promotion_plan,
)
from commercelake.cloud.bigquery_schema_validation import (
    validate_bigquery_schema,
)
from commercelake.cloud.bigquery_validation import (
    build_show_command,
    validate_table_row_count,
)

CommandRunner = Callable[[list[str]], CompletedProcess[str]]
RecoveryStatus = Literal["valid", "missing", "invalid", "unknown"]


@dataclass(frozen=True)
class RecoveryTableResult:
    """Inspection outcome for one versioned Gold table."""

    logical_name: str
    version_name: str
    status: RecoveryStatus
    expected_rows: int
    detail: str


@dataclass(frozen=True)
class RecoveryInspectionResult:
    """Read-only recovery report for a complete Gold run."""

    run_id: str
    tables: tuple[RecoveryTableResult, ...]

    @property
    def ready(self) -> bool:
        """Require all six destinations to match the Gold contract."""
        return len(self.tables) == 6 and all(
            table.status == "valid" for table in self.tables
        )

    @property
    def status_counts(self) -> dict[str, int]:
        """Count tables in each inspection category."""
        counts = {
            "valid": 0,
            "missing": 0,
            "invalid": 0,
            "unknown": 0,
        }

        for table in self.tables:
            counts[table.status] += 1

        return counts


def inspect_gold_version(
    plan: PromotionPlan,
    *,
    runner: CommandRunner,
) -> RecoveryInspectionResult:
    """Inspect six versioned destinations without changing cloud resources."""
    validate_promotion_plan(plan)

    if not callable(runner):
        raise TypeError("An explicit callable runner is required")

    results = []

    for table in plan.tables:
        command = build_show_command(
            project_id=plan.project_id,
            dataset=plan.dataset,
            table=table.version_name,
            location=plan.location,
        )

        status: RecoveryStatus
        detail: str

        try:
            metadata = runner(command)

        except CalledProcessError as exc:
            if is_confirmed_missing(
                exc.stdout,
                exc.stderr,
                project_id=plan.project_id,
                dataset=plan.dataset,
                table=table.version_name,
            ):
                status = "missing"
                detail = "Destination explicitly reported as absent"
            else:
                status = "unknown"
                detail = "BigQuery command failed without confirmed absence"

        except Exception:  # noqa: BLE001
            # Fail closed and continue inspecting the remaining tables.
            status = "unknown"
            detail = "BigQuery metadata request raised an exception"

        else:
            if not isinstance(metadata, CompletedProcess):
                status = "unknown"
                detail = "Runner returned an unsupported response"

            elif metadata.returncode != 0:
                if is_confirmed_missing(
                    metadata.stdout,
                    metadata.stderr,
                    project_id=plan.project_id,
                    dataset=plan.dataset,
                    table=table.version_name,
                ):
                    status = "missing"
                    detail = "Destination explicitly reported as absent"
                else:
                    status = "unknown"
                    detail = "BigQuery command failed without confirmed absence"

            elif not isinstance(metadata.stdout, str):
                status = "invalid"
                detail = "Metadata output is not text"

            else:
                try:
                    validate_table_row_count(
                        table=table.version_name,
                        expected_rows=table.expected_rows,
                        metadata_output=metadata.stdout,
                    )
                    validate_bigquery_schema(
                        table=table.logical_name,
                        metadata_output=metadata.stdout,
                    )
                except ValueError, TypeError:
                    status = "invalid"
                    detail = "Metadata does not match the Gold contract"
                else:
                    status = "valid"
                    detail = "Row count and schema match the Gold contract"

        results.append(
            RecoveryTableResult(
                logical_name=table.logical_name,
                version_name=table.version_name,
                status=status,
                expected_rows=table.expected_rows,
                detail=detail,
            )
        )

    return RecoveryInspectionResult(
        run_id=plan.run_id,
        tables=tuple(results),
    )
