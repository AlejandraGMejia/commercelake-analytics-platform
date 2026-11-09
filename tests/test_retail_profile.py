"""Validate the generated retail profiling report."""

import json
from pathlib import Path

import pytest

REPORT = Path("reports/retail_profile.json")

EXPECTED_COLUMNS = {
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer ID",
    "Country",
}


@pytest.fixture(scope="module")
def profile():
    """Load the profiling output."""
    if not REPORT.exists():
        pytest.skip("Profiling report not generated")

    return json.loads(REPORT.read_text(encoding="utf-8"))


def test_total_rows(profile):
    """Verify the observed source row count."""
    assert profile["total_rows"] == 1_067_371


def test_worksheet_count(profile):
    """Verify that both source periods were profiled."""
    assert len(profile["sheets"]) == 2


def test_row_reconciliation(profile):
    """Ensure sheet totals reconcile with the report."""
    total = sum(sheet["rows"] for sheet in profile["sheets"])
    assert total == profile["total_rows"]


def test_duplicate_reconciliation(profile):
    """Verify duplicate counts reconcile."""
    total = sum(sheet["duplicate_rows"] for sheet in profile["sheets"])
    assert total == profile["duplicate_rows"]


def test_required_columns(profile):
    """Check the expected source fields."""
    for sheet in profile["sheets"]:
        assert EXPECTED_COLUMNS.issubset(
            set(sheet["missing"])
            | {
                "Invoice",
                "StockCode",
                "Quantity",
                "InvoiceDate",
                "Price",
                "Country",
            }
        )


def test_no_invalid_dates(profile):
    """Verify that all invoice dates were parsed."""
    for sheet in profile["sheets"]:
        assert sheet["invalid_dates"] == 0


def test_no_invalid_numeric_values(profile):
    """Verify quantity and price parsing."""
    for sheet in profile["sheets"]:
        assert sheet["invalid_quantities"] == 0
        assert sheet["invalid_prices"] == 0


def test_missing_customer_count(profile):
    """Protect the observed source quality baseline."""
    total = sum(sheet["missing"].get("Customer ID", 0) for sheet in profile["sheets"])
    assert total == 243_007
