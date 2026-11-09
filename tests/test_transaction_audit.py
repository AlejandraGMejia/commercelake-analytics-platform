"""Validate CommerceLake transaction audit results."""

import json
from pathlib import Path

import pytest

from scripts.audit_retail_transactions import (
    classify_transaction,
    row_fingerprint,
)

REPORT = Path("reports/transaction_audit.json")


@pytest.fixture(scope="module")
def audit():
    """Load the transaction audit report."""
    return json.loads(REPORT.read_text(encoding="utf-8"))


def test_total_rows(audit):
    assert audit["total_rows"] == 1_067_371


def test_sheet_reconciliation(audit):
    assert (
        sum(sheet["rows"] for sheet in audit["sheets"].values()) == audit["total_rows"]
    )


def test_cancellation_reconciliation(audit):
    metrics = audit["metrics"]

    assert (
        metrics["cancellation_and_negative_quantity"]
        + metrics["cancellation_without_negative_quantity"]
        == metrics["cancellation"]
    )


def test_negative_quantity_reconciliation(audit):
    metrics = audit["metrics"]

    assert (
        metrics["cancellation_and_negative_quantity"]
        + metrics["negative_quantity_without_cancellation"]
        == metrics["negative_quantity"]
    )


def test_cross_sheet_duplicates(audit):
    assert audit["metrics"]["cross_sheet_duplicates"] == 22_523


def test_classify_cancellation():
    flags = classify_transaction("C123", -2, 10.0)

    assert flags["cancellation"]
    assert flags["negative_quantity"]
    assert not flags["negative_price"]


def test_classify_negative_without_cancellation():
    flags = classify_transaction("123", -1, 5.0)

    assert not flags["cancellation"]
    assert flags["negative_quantity"]


def test_classify_zero_price():
    flags = classify_transaction("123", 2, 0)

    assert flags["zero_price"]


def test_fingerprint_is_deterministic():
    row = ("123", "ABC", "Product", 2)

    assert row_fingerprint(row) == row_fingerprint(row)


def test_fingerprint_changes_with_content():
    first = ("123", "ABC", "Product", 2)
    second = ("123", "ABC", "Product", 3)

    assert row_fingerprint(first) != row_fingerprint(second)
