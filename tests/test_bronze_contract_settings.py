"""Tests for mandatory Bronze contract settings."""

from pathlib import Path

import pytest
import yaml

from commercelake.quality.bronze_validator import load_contract

REQUIRED_SETTINGS = (
    "require_complete_source",
    "require_manifest",
    "require_all_files",
    "require_partition_reconciliation",
)


@pytest.mark.parametrize("setting", REQUIRED_SETTINGS)
@pytest.mark.parametrize("invalid_value", [False, None, "true"])
def test_mandatory_setting_must_be_true(
    tmp_path,
    setting,
    invalid_value,
):
    """Critical Bronze checks cannot be disabled."""
    source = Path("config/data_quality/bronze_contract.yaml")

    contract = yaml.safe_load(source.read_text(encoding="utf-8"))

    contract["validation"][setting] = invalid_value

    path = tmp_path / "invalid_contract.yaml"
    path.write_text(
        yaml.safe_dump(contract),
        encoding="utf-8",
    )

    with pytest.raises(
        (TypeError, ValueError),
        match=setting,
    ):
        load_contract(path)


def test_real_contract_requires_all_critical_checks():
    """The production contract enables all mandatory checks."""
    contract = load_contract("config/data_quality/bronze_contract.yaml")

    for setting in REQUIRED_SETTINGS:
        assert contract["validation"][setting] is True
