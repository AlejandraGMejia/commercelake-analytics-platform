"""Stable fingerprints for source retail records."""

import hashlib
import json
from datetime import date, datetime


def normalize(value):
    """Produce a stable representation for hashing."""
    if value is None:
        return None

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, str):
        return value.strip()

    if isinstance(value, float):
        return format(value, ".15g")

    return str(value)


def row_fingerprint(row):
    """Hash normalized source values."""
    payload = json.dumps(
        [normalize(value) for value in row],
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
