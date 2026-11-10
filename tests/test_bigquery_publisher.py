"""Unit tests for BigQuery Gold publication planning."""

from pathlib import Path

import pytest

from commercelake.cloud.bigquery_publisher import (
    PublicationPlan,
    plan_as_dict,
    validate_identifiers,
)
from commercelake.warehouse.loader import LOAD_ORDER


def make_plan():
    """Build a synthetic plan without accessing local or cloud data."""
    files_by_table = {
        table: [(f"{table}.parquet", index + 1)]
        for index, table in enumerate(LOAD_ORDER)
    }

    return PublicationPlan(
        project_id="commercelake-analytics",
        dataset="commercelake_gold",
        location="US",
        run_id="a" * 32,
        run_directory=Path("/tmp/gold-test"),
        files_by_table=files_by_table,
    )


def test_valid_identifiers():
    validate_identifiers(
        "commercelake-analytics",
        "commercelake_gold",
    )


@pytest.mark.parametrize(
    "project_id",
    [
        "",
        "INVALID PROJECT",
        "UPPERCASE-PROJECT",
        "../unsafe",
        "abc",
    ],
)
def test_invalid_project_identifiers(project_id):
    with pytest.raises(ValueError, match="Invalid GCP project ID"):
        validate_identifiers(project_id, "commercelake_gold")


@pytest.mark.parametrize(
    "dataset",
    [
        "",
        "invalid-dataset",
        "invalid.dataset",
        "../unsafe",
        "invalid dataset",
    ],
)
def test_invalid_dataset_identifiers(dataset):
    with pytest.raises(ValueError, match="Invalid BigQuery dataset ID"):
        validate_identifiers("commercelake-analytics", dataset)


def test_plan_totals():
    plan = make_plan()

    assert plan.total_files == len(LOAD_ORDER)
    assert plan.total_rows == sum(range(1, len(LOAD_ORDER) + 1))


def test_plan_json_summary():
    plan = make_plan()
    summary = plan_as_dict(plan)

    assert summary["mode"] == "dry-run"
    assert summary["project_id"] == "commercelake-analytics"
    assert summary["dataset"] == "commercelake_gold"
    assert summary["total_source_files"] == len(LOAD_ORDER)

    assert [item["table"] for item in summary["tables"]] == list(LOAD_ORDER)

    for index, table in enumerate(summary["tables"], start=1):
        assert table["source_files"] == 1
        assert table["expected_rows"] == index
        assert table["destination"].startswith(
            "commercelake-analytics:commercelake_gold."
        )
