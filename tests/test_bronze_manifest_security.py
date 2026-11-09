"""Adversarial tests for Bronze manifest and pointer safety."""

import json

import pytest
from test_bronze_validator import (
    FILE_NAME,
    RUN_ID,
    make_dataset,
)

from commercelake.quality.bronze_observability import observe_bronze
from commercelake.quality.bronze_validator import validate_bronze


def update_json(path, changes):
    """Update a temporary JSON document."""
    document = json.loads(path.read_text(encoding="utf-8"))
    document.update(changes)
    path.write_text(
        json.dumps(document),
        encoding="utf-8",
    )


@pytest.mark.parametrize(
    "unsafe_id",
    ["../outside", "/tmp/outside", "..", ".", "a\\b"],
)
def test_validator_rejects_unsafe_run_ids(tmp_path, unsafe_id):
    """Reject run IDs that could escape the active dataset."""
    output, contract = make_dataset(tmp_path)

    update_json(
        output / "current.json",
        {"run_id": unsafe_id},
    )

    with pytest.raises(ValueError):
        validate_bronze(output, contract)


@pytest.mark.parametrize(
    "unsafe_id",
    ["../outside", "/tmp/outside", "..", ".", "a\\b"],
)
def test_observability_rejects_unsafe_run_ids(tmp_path, unsafe_id):
    """Apply the same pointer protection to observability."""
    output, _ = make_dataset(tmp_path)

    update_json(
        output / "current.json",
        {"run_id": unsafe_id},
    )

    with pytest.raises(ValueError):
        observe_bronze(output)


def test_validator_rejects_unsafe_file_path(tmp_path):
    """Prevent reading files outside the active run."""
    output, contract = make_dataset(tmp_path)
    manifest_path = output / "runs" / RUN_ID / "manifest.json"

    document = json.loads(manifest_path.read_text(encoding="utf-8"))
    document["files"][0]["path"] = "../../outside.parquet"

    manifest_path.write_text(
        json.dumps(document),
        encoding="utf-8",
    )

    result = validate_bronze(output, contract)

    assert result["status"] == "failed"
    assert any(failure.startswith("BRZ002") for failure in result["failures"])


def test_observability_rejects_unsafe_file_path(tmp_path):
    """Prevent observability from reading outside the run."""
    output, _ = make_dataset(tmp_path)
    manifest_path = output / "runs" / RUN_ID / "manifest.json"

    document = json.loads(manifest_path.read_text(encoding="utf-8"))
    document["files"][0]["path"] = "../../outside.parquet"

    manifest_path.write_text(
        json.dumps(document),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsafe"):
        observe_bronze(output)


def test_validator_rejects_duplicate_inventory(tmp_path):
    """Detect the same Parquet file declared twice."""
    output, contract = make_dataset(tmp_path)
    manifest_path = output / "runs" / RUN_ID / "manifest.json"

    document = json.loads(manifest_path.read_text(encoding="utf-8"))
    document["files"].append({"path": FILE_NAME, "rows": 1})

    manifest_path.write_text(
        json.dumps(document),
        encoding="utf-8",
    )

    result = validate_bronze(output, contract)

    assert result["status"] == "failed"
    assert any("Duplicate file inventory" in failure for failure in result["failures"])
