"""Safety tests for the explicit CommerceLake staging CLI."""

from unittest.mock import patch

import pytest

from commercelake.cloud.bigquery_staging_cli import (
    EXECUTION_CONFIRMATION,
    main,
)

BASE_ARGS = [
    "--project-id",
    "commercelake-analytics",
    "--dataset",
    "commercelake_gold",
    "--location",
    "US",
    "--maximum-bytes-billed",
    "10485760",
]


def test_execution_requires_confirmation() -> None:
    """Reject real execution without an explicit confirmation."""
    with patch(
        "commercelake.cloud.bigquery_staging_cli."
        "production_bigquery_runner"
    ) as runner:
        with pytest.raises(ValueError, match="requires --confirm"):
            main([*BASE_ARGS, "--execute"])

        runner.assert_not_called()


def test_execution_rejects_wrong_confirmation() -> None:
    """Reject an incorrect execution confirmation."""
    with patch(
        "commercelake.cloud.bigquery_staging_cli."
        "production_bigquery_runner"
    ) as runner:
        with pytest.raises(ValueError, match="requires --confirm"):
            main([
                *BASE_ARGS,
                "--execute",
                "--confirm",
                "WRONG_CONFIRMATION",
            ])

        runner.assert_not_called()


@pytest.mark.parametrize(
    ("flag", "value", "message"),
    [
        ("--project-id", "other-project", "Unauthorized GCP project"),
        ("--dataset", "other_dataset", "Unauthorized BigQuery dataset"),
        ("--location", "EU", "Unauthorized BigQuery location"),
    ],
)
def test_rejects_unauthorized_destination(
    flag: str,
    value: str,
    message: str,
) -> None:
    """Reject attempts to publish outside the approved destination."""
    args = BASE_ARGS.copy()
    index = args.index(flag)
    args[index + 1] = value

    with patch(
        "commercelake.cloud.bigquery_staging_cli."
        "production_bigquery_runner"
    ) as runner:
        with pytest.raises(ValueError, match=message):
            main([
                *args,
                "--execute",
                "--confirm",
                EXECUTION_CONFIRMATION,
            ])

        runner.assert_not_called()


@pytest.mark.parametrize(
    "budget",
    ["0", "10485759", "-1"],
)
def test_rejects_invalid_budget(budget: str) -> None:
    """Reject insufficient or invalid query budgets."""
    args = BASE_ARGS.copy()
    index = args.index("--maximum-bytes-billed")
    args[index + 1] = budget

    with patch(
        "commercelake.cloud.bigquery_staging_cli."
        "production_bigquery_runner"
    ) as runner:
        with pytest.raises(ValueError, match="maximum_bytes_billed"):
            main([
                *args,
                "--execute",
                "--confirm",
                EXECUTION_CONFIRMATION,
            ])

        runner.assert_not_called()


def test_dry_run_rejects_execution_confirmation() -> None:
    """Keep validation and execution modes clearly separated."""
    with pytest.raises(ValueError, match="not accepted"):
        main([
            *BASE_ARGS,
            "--dry-run",
            "--confirm",
            EXECUTION_CONFIRMATION,
        ])


def test_modes_are_mutually_exclusive() -> None:
    """Prevent simultaneous dry-run and execution flags."""
    with pytest.raises(SystemExit) as exc:
        main([
            *BASE_ARGS,
            "--dry-run",
            "--execute",
        ])

    assert exc.value.code == 2
