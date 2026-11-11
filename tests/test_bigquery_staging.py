"""Tests for BigQuery staging command construction."""

from pathlib import Path

import pytest

from commercelake.cloud.bigquery_cli import (
    DECIMAL_TABLES,
    DECIMAL_TARGET_TYPES,
)
from commercelake.cloud.bigquery_staging import (
    build_staging_load_command,
    staging_table_name,
)
from commercelake.warehouse.loader import LOAD_ORDER

PROJECT = "commercelake-analytics"
DATASET = "commercelake_gold"
RUN_ID = "591c8097f0554414be18648e3587d2b3"


@pytest.mark.parametrize("table", LOAD_ORDER)
def test_staging_table_name_is_deterministic(table):
    result = staging_table_name(table, RUN_ID)

    assert result == f"_stage_{table}_591c8097"


@pytest.mark.parametrize("table", LOAD_ORDER)
def test_staging_command_never_targets_production(table):
    command = build_staging_load_command(
        PROJECT,
        DATASET,
        table,
        RUN_ID,
        Path(f"/tmp/{table}.parquet"),
    )

    destination = f"{PROJECT}:{DATASET}._stage_{table}_591c8097"
    production = f"{PROJECT}:{DATASET}.{table}"

    assert destination in command
    assert production not in command
    assert "--replace" in command


@pytest.mark.parametrize("table", LOAD_ORDER)
def test_staging_decimal_configuration_matches_table(table):
    command = build_staging_load_command(
        PROJECT,
        DATASET,
        table,
        RUN_ID,
        Path(f"/tmp/{table}.parquet"),
    )

    actual_options = [
        option for option in command if option.startswith("--decimal_target_types=")
    ]

    if table in DECIMAL_TABLES:
        assert actual_options == list(DECIMAL_TARGET_TYPES)
    else:
        assert actual_options == []


@pytest.mark.parametrize(
    "run_id",
    [
        "",
        "123",
        "not-a-run-id",
        "A" * 32,
        "a" * 33,
    ],
)
def test_rejects_invalid_run_ids(run_id):
    with pytest.raises(ValueError, match="Invalid Gold run ID"):
        staging_table_name("fact_sales", run_id)


def test_rejects_unknown_table():
    with pytest.raises(ValueError, match="Unsupported CommerceLake table"):
        staging_table_name("unknown_table", RUN_ID)


def test_rejects_non_parquet_source():
    with pytest.raises(ValueError, match="Parquet"):
        build_staging_load_command(
            PROJECT,
            DATASET,
            "fact_sales",
            RUN_ID,
            Path("/tmp/source.csv"),
        )


def test_rejects_unsupported_location():
    with pytest.raises(ValueError, match="location"):
        build_staging_load_command(
            PROJECT,
            DATASET,
            "fact_sales",
            RUN_ID,
            Path("/tmp/source.parquet"),
            location="EU",
        )
