"""Tests for the Bronze quality validation engine."""

import json
from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from commercelake.ingestion.schema import RETAIL_SCHEMA
from commercelake.quality.bronze_validator import (
    load_contract,
    validate_bronze,
)

RUN_ID = "test_run_001"
PARTITION = "year=2020/month=01"
FILE_NAME = f"{PARTITION}/part-00000.parquet"


def make_dataset(
    tmp_path,
    *,
    missing_file=False,
    wrong_schema=False,
    invalid_provenance=False,
    invalid_fingerprint=False,
    wrong_count=False,
    duplicate_inventory=False,
):
    """Create an isolated Bronze dataset and contract."""
    output = tmp_path / "retail"
    run_dir = output / "runs" / RUN_ID
    parquet_path = run_dir / FILE_NAME
    parquet_path.parent.mkdir(parents=True)

    record = {
        "invoice": ["12345"],
        "stock_code": ["ABC"],
        "description": ["Sample product"],
        "quantity": [1],
        "invoice_date": [datetime(2020, 1, 1)],
        "unit_price": [Decimal("10.0000")],
        "customer_id": ["100"],
        "country": ["United Kingdom"],
        "source_sheet": ["" if invalid_provenance else "Year 2010-2011"],
        "source_row_number": [2],
        "row_fingerprint": ["" if invalid_fingerprint else "abc123"],
    }

    table = pa.Table.from_pydict(
        record,
        schema=RETAIL_SCHEMA,
    )

    if wrong_schema:
        table = table.drop(["country"])

    if not missing_file:
        pq.write_table(table, parquet_path)

    file_rows = 2 if wrong_count else 1

    inventory = [
        {
            "path": FILE_NAME,
            "rows": file_rows,
        }
    ]

    if duplicate_inventory:
        inventory.append(dict(inventory[0]))

    manifest = {
        "run_id": RUN_ID,
        "complete_source": True,
        "total_rows": 1,
        "files": inventory,
        "partitions": {PARTITION: 1},
    }

    (run_dir / "manifest.json").write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    (output / "current.json").write_text(
        json.dumps(
            {
                "run_id": RUN_ID,
                "manifest": f"runs/{RUN_ID}/manifest.json",
            }
        ),
        encoding="utf-8",
    )

    contract = {
        "contract_version": "1.0",
        "validation": {
            "expected_total_rows": 1,
            "require_complete_source": True,
            "require_partition_reconciliation": True,
            "require_manifest": True,
            "require_all_files": True,
        },
        "critical_rules": [],
        "observability_rules": [],
        "preservation_policy": {},
    }

    contract_path = tmp_path / "contract.yaml"
    contract_path.write_text(
        yaml.safe_dump(contract),
        encoding="utf-8",
    )

    return output, contract_path


def run_validation(tmp_path, **kwargs):
    """Build and validate a temporary dataset."""
    output, contract = make_dataset(tmp_path, **kwargs)

    return validate_bronze(
        output=output,
        contract_path=contract,
    )


def has_failure(result, rule_id):
    """Check whether a result contains a specific rule failure."""
    return any(failure.startswith(rule_id) for failure in result["failures"])


def test_valid_bronze_dataset(tmp_path):
    result = run_validation(tmp_path)

    assert result["status"] == "passed"
    assert result["metrics"]["files_checked"] == 1
    assert result["metrics"]["rows_checked"] == 1
    assert result["failures"] == []


def test_missing_parquet_file(tmp_path):
    result = run_validation(tmp_path, missing_file=True)

    assert result["status"] == "failed"
    assert has_failure(result, "BRZ002")


def test_incompatible_parquet_schema(tmp_path):
    result = run_validation(tmp_path, wrong_schema=True)

    assert result["status"] == "failed"
    assert has_failure(result, "BRZ001")


def test_invalid_source_provenance(tmp_path):
    result = run_validation(tmp_path, invalid_provenance=True)

    assert result["status"] == "failed"
    assert has_failure(result, "BRZ004")


def test_missing_fingerprint(tmp_path):
    result = run_validation(tmp_path, invalid_fingerprint=True)

    assert result["status"] == "failed"
    assert has_failure(result, "BRZ005")


def test_inconsistent_file_row_count(tmp_path):
    result = run_validation(tmp_path, wrong_count=True)

    assert result["status"] == "failed"
    assert has_failure(result, "BRZ003")


def test_duplicate_inventory_entry(tmp_path):
    result = run_validation(tmp_path, duplicate_inventory=True)

    assert result["status"] == "failed"
    assert has_failure(result, "BRZ002")


def test_missing_active_pointer(tmp_path):
    output, contract = make_dataset(tmp_path)
    (output / "current.json").unlink()

    with pytest.raises(FileNotFoundError):
        validate_bronze(
            output=output,
            contract_path=contract,
        )


def test_incomplete_source(tmp_path):
    output, contract = make_dataset(tmp_path)

    manifest_path = output / "runs" / RUN_ID / "manifest.json"

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    manifest["complete_source"] = False

    manifest_path.write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    result = validate_bronze(output, contract)

    assert result["status"] == "failed"
    assert has_failure(result, "BRZ003")


def test_invalid_contract_type(tmp_path):
    path = tmp_path / "invalid.yaml"

    path.write_text(
        "- invalid\n- contract\n",
        encoding="utf-8",
    )

    with pytest.raises(TypeError):
        load_contract(path)


def test_invalid_expected_row_count(tmp_path):
    _, contract_path = make_dataset(tmp_path)

    contract = yaml.safe_load(contract_path.read_text(encoding="utf-8"))

    contract["validation"]["expected_total_rows"] = -1

    contract_path.write_text(
        yaml.safe_dump(contract),
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_contract(contract_path)
