"""Tests for safe fact_sales CSV staging commands."""

import pytest

from commercelake.cloud.bigquery_csv_staging import (
    build_fact_sales_csv_load_command,
)

RUN_ID = "591c8097f0554414be18648e3587d2b3"


def build(tmp_path, **overrides):
    source = tmp_path / "fact_sales.csv"
    source.write_text("example\n", encoding="utf-8")

    arguments = {
        "project_id": "commercelake-analytics",
        "dataset": "commercelake_gold",
        "run_id": RUN_ID,
        "source_path": source,
        "location": "US",
    }
    arguments.update(overrides)

    return build_fact_sales_csv_load_command(**arguments)


def test_builds_explicit_csv_load_command(tmp_path):
    command = build(tmp_path)

    assert command[:3] == ["bq", "--location=US", "load"]
    assert "--source_format=CSV" in command
    assert "--noreplace" in command
    assert "--skip_leading_rows=0" in command
    assert "--allow_quoted_newlines=true" in command
    assert "--max_bad_records=0" in command

    assert command[-2] == (
        "commercelake-analytics:commercelake_gold._stage_fact_sales_591c8097"
    )
    assert command[-1].endswith("fact_sales.csv")


def test_never_uses_replacement_or_sql(tmp_path):
    command = build(tmp_path)

    assert "--replace" not in command
    assert "--replace_data" not in command
    assert "query" not in command
    assert "mk" not in command


def test_rejects_wrong_location(tmp_path):
    with pytest.raises(ValueError, match="location"):
        build(tmp_path, location="EU")


def test_rejects_invalid_run_id(tmp_path):
    with pytest.raises(ValueError, match="run ID"):
        build(tmp_path, run_id="invalid")


def test_rejects_missing_csv(tmp_path):
    with pytest.raises(FileNotFoundError):
        build(tmp_path, source_path=tmp_path / "missing.csv")


def test_rejects_parquet_source(tmp_path):
    with pytest.raises(ValueError, match="CSV"):
        build(tmp_path, source_path=tmp_path / "fact_sales.parquet")
