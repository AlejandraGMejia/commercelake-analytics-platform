"""Tests for preparing fact_sales CSV from the Gold inventory."""

from unittest.mock import patch

import pytest

from commercelake.cloud.bigquery_csv_preparation import prepare_fact_sales_csv
from commercelake.cloud.bigquery_publisher import PublicationPlan

RUN_ID = "591c8097f0554414be18648e3587d2b3"


def make_plan(tmp_path, inventory):
    return PublicationPlan(
        project_id="commercelake-analytics",
        dataset="commercelake_gold",
        location="US",
        run_id=RUN_ID,
        run_directory=tmp_path,
        files_by_table={"fact_sales": inventory},
    )


def make_source(tmp_path, name="part-00000.parquet"):
    path = tmp_path / name
    path.write_bytes(b"mock-parquet")
    return path


def test_prepares_csv_from_gold_inventory(tmp_path):
    first = make_source(tmp_path, "first.parquet")
    second = make_source(tmp_path, "second.parquet")

    plan = make_plan(
        tmp_path,
        [(first.name, 3), (second.name, 2)],
    )
    destination = tmp_path / "fact_sales.csv"

    def fake_export(files, output_path, *, batch_size):
        assert files == [first, second]
        assert output_path == destination
        assert batch_size == 100
        output_path.write_text("mock CSV", encoding="utf-8")
        return 5

    with patch(
        "commercelake.cloud.bigquery_csv_preparation.export_fact_sales_csv",
        side_effect=fake_export,
    ):
        result = prepare_fact_sales_csv(
            plan,
            destination,
            batch_size=100,
        )

    assert result == 5
    assert destination.is_file()


def test_rejects_missing_inventory(tmp_path):
    plan = make_plan(tmp_path, [])

    with pytest.raises(ValueError, match="Missing fact_sales"):
        prepare_fact_sales_csv(plan, tmp_path / "output.csv")


@pytest.mark.parametrize("rows", [-1, True, 2.5, "3"])
def test_rejects_invalid_inventory_counts(tmp_path, rows):
    source = make_source(tmp_path)
    plan = make_plan(tmp_path, [(source.name, rows)])

    with pytest.raises(ValueError, match="Invalid fact_sales"):
        prepare_fact_sales_csv(plan, tmp_path / "output.csv")


def test_rejects_duplicate_sources(tmp_path):
    source = make_source(tmp_path)
    plan = make_plan(
        tmp_path,
        [(source.name, 2), (source.name, 2)],
    )

    with pytest.raises(ValueError, match="Duplicate"):
        prepare_fact_sales_csv(plan, tmp_path / "output.csv")


def test_rejects_missing_source(tmp_path):
    plan = make_plan(tmp_path, [("missing.parquet", 1)])

    with pytest.raises(FileNotFoundError):
        prepare_fact_sales_csv(plan, tmp_path / "output.csv")


def test_rejects_non_parquet_source(tmp_path):
    source = make_source(tmp_path, "incorrect.csv")
    plan = make_plan(tmp_path, [(source.name, 1)])

    with pytest.raises(ValueError, match="Parquet"):
        prepare_fact_sales_csv(plan, tmp_path / "output.csv")


def test_removes_csv_when_row_count_mismatches(tmp_path):
    source = make_source(tmp_path)
    plan = make_plan(tmp_path, [(source.name, 10)])
    destination = tmp_path / "fact_sales.csv"

    def fake_export(files, output_path, *, batch_size):
        output_path.write_text("partial CSV", encoding="utf-8")
        return 9

    with (
        patch(
            "commercelake.cloud.bigquery_csv_preparation.export_fact_sales_csv",
            side_effect=fake_export,
        ),
        pytest.raises(ValueError, match="row count mismatch"),
    ):
        prepare_fact_sales_csv(plan, destination)

    assert not destination.exists()
