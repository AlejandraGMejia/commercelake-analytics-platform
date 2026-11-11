"""Regression tests for observed BigQuery CLI error output."""

from subprocess import CalledProcessError, CompletedProcess

import pytest
from test_bigquery_destination_guard import make_plan

from commercelake.cloud.bigquery_destination_guard import (
    check_versioned_destinations,
)
from commercelake.cloud.bigquery_recovery_inspector import (
    inspect_gold_version,
)

SDK_WARNING = (
    "/home/Ale/Tools/google-cloud/google-cloud-sdk/platform/bq/"
    "third_party/requests/__init__.py:103: RequestsDependencyWarning: "
    "urllib3 (1.26.9) or chardet (None)/charset_normalizer (3.5.2) "
    "doesn't match a supported version!\n"
    '  warnings.warn("urllib3 ({}) or chardet ({})/'
    "charset_normalizer ({}) doesn't match supported \"\n"
)


def missing_output(command):
    """Reproduce the observed BigQuery CLI stdout format."""
    return f"BigQuery error in show operation: Not found: Table {command[-1]}"


@pytest.mark.parametrize("raise_error", [False, True])
def test_guard_accepts_observed_cli_missing_error(raise_error):
    def runner(command):
        stdout = missing_output(command)

        if raise_error:
            raise CalledProcessError(
                returncode=2,
                cmd=command,
                output=stdout,
                stderr=SDK_WARNING,
            )

        return CompletedProcess(command, 2, stdout, SDK_WARNING)

    result = check_versioned_destinations(make_plan(), runner=runner)

    assert len(result.available_destinations) == 6


@pytest.mark.parametrize("raise_error", [False, True])
def test_inspector_recognizes_observed_cli_missing_error(raise_error):
    def runner(command):
        stdout = missing_output(command)

        if raise_error:
            raise CalledProcessError(
                returncode=2,
                cmd=command,
                output=stdout,
                stderr=SDK_WARNING,
            )

        return CompletedProcess(command, 2, stdout, SDK_WARNING)

    result = inspect_gold_version(make_plan(), runner=runner)

    assert result.status_counts["missing"] == 6
    assert not result.ready


def test_guard_rejects_conflicting_permission_error():
    def runner(command):
        return CompletedProcess(
            command,
            2,
            missing_output(command),
            "Access Denied",
        )

    with pytest.raises(RuntimeError, match="Cannot verify destination"):
        check_versioned_destinations(make_plan(), runner=runner)


def test_inspector_marks_conflicting_permission_error_unknown():
    def runner(command):
        return CompletedProcess(
            command,
            2,
            missing_output(command),
            "Access Denied",
        )

    result = inspect_gold_version(make_plan(), runner=runner)

    assert result.status_counts["unknown"] == 6
    assert result.status_counts["missing"] == 0


def test_guard_rejects_unrelated_not_found():
    def runner(command):
        return CompletedProcess(
            command,
            2,
            "BigQuery error in show operation: Not found: Dataset other_dataset",
            SDK_WARNING,
        )

    with pytest.raises(RuntimeError, match="Cannot verify destination"):
        check_versioned_destinations(make_plan(), runner=runner)


def test_inspector_marks_unrelated_not_found_unknown():
    def runner(command):
        return CompletedProcess(
            command,
            2,
            "BigQuery error in show operation: Not found: Dataset other_dataset",
            SDK_WARNING,
        )

    result = inspect_gold_version(make_plan(), runner=runner)

    assert result.status_counts["unknown"] == 6
