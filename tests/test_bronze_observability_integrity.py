"""Integrity tests for Bronze observability inventory handling."""

import json

import pytest
from test_bronze_observability import (
    RUN_ID,
    make_dataset,
    make_record,
)

from commercelake.quality.bronze_observability import observe_bronze


def test_observability_rejects_duplicate_file_inventory(tmp_path):
    """A repeated file must not inflate quality metrics."""
    output = make_dataset(
        tmp_path,
        [[make_record("unique-a")]],
    )

    manifest_path = output / "runs" / RUN_ID / "manifest.json"

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    manifest["files"].append(dict(manifest["files"][0]))

    manifest_path.write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate"):
        observe_bronze(output)
