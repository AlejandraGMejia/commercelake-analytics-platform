"""Security regression tests for BigQuery CLI error recognition."""

import pytest

from commercelake.cloud.bigquery_cli_errors import (
    is_confirmed_missing,
    is_known_cli_warning,
)

PROJECT = "commercelake-analytics"
DATASET = "commercelake_gold"
TABLE = "gold_dim_product_test"

MISSING = (
    f"BigQuery error in show operation: Not found: Table {PROJECT}:{DATASET}.{TABLE}"
)

WARNING = (
    "/home/Ale/Tools/google-cloud/google-cloud-sdk/platform/bq/"
    "third_party/requests/__init__.py:103: RequestsDependencyWarning: "
    "urllib3 (1.26.9) or chardet (None)/charset_normalizer (3.5.2) "
    "doesn't match a supported version!\n"
    '  warnings.warn("urllib3 ({}) or chardet ({})/'
    "charset_normalizer ({}) doesn't match a supported \""
)


def confirmed(stdout, stderr):
    return is_confirmed_missing(
        stdout,
        stderr,
        project_id=PROJECT,
        dataset=DATASET,
        table=TABLE,
    )


def test_observed_warning_is_recognized():
    assert is_known_cli_warning(WARNING)


def test_observed_missing_with_warning_is_accepted():
    assert confirmed(MISSING, WARNING)


@pytest.mark.parametrize(
    "extra",
    [
        "\nAccess Denied",
        "\nAuthentication failed",
        "\nConnection timed out",
        "\nAnother warning",
    ],
)
def test_extra_stderr_messages_are_rejected(extra):
    assert not confirmed(MISSING, WARNING + extra)


def test_unrelated_warning_is_rejected():
    assert not confirmed(MISSING, "Unexpected warning")


def test_wrong_table_is_rejected():
    assert not confirmed(
        MISSING.replace(TABLE, "another_table"),
        WARNING,
    )


def test_permission_error_is_not_missing():
    assert not confirmed("", "Access Denied")


def test_legacy_stderr_missing_remains_supported():
    assert confirmed("", f"Not found: Table {PROJECT}:{DATASET}.{TABLE}")
