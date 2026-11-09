"""Compatibility tests for shared source fingerprints."""

from datetime import date, datetime

from commercelake.ingestion.fingerprint import (
    normalize,
    row_fingerprint,
)
from scripts.audit_retail_transactions import (
    row_fingerprint as original_fingerprint,
)


def test_normalize_none():
    assert normalize(None) is None


def test_normalize_datetime():
    value = datetime(2010, 12, 1, 8, 26)
    assert normalize(value) == "2010-12-01T08:26:00"


def test_normalize_date():
    assert normalize(date(2010, 12, 1)) == "2010-12-01"


def test_normalize_text():
    assert normalize("  PRODUCT A  ") == "PRODUCT A"


def test_normalize_float():
    assert normalize(2.55) == "2.55"


def test_fingerprint_matches_original():
    row = (
        "536365",
        "85123A",
        "PRODUCT A",
        6,
        datetime(2010, 12, 1, 8, 26),
        2.55,
        17850.0,
        "United Kingdom",
    )

    assert row_fingerprint(row) == original_fingerprint(row)


def test_fingerprint_is_deterministic():
    row = ("536365", "85123A", None, 6)

    assert row_fingerprint(row) == row_fingerprint(row)


def test_fingerprint_detects_content_changes():
    first = ("536365", "85123A", 6)
    second = ("536365", "85123A", 7)

    assert row_fingerprint(first) != row_fingerprint(second)
