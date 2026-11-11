"""Tests for isolated BigQuery ingestion staging names."""

import pytest

from commercelake.cloud.bigquery_ingestion_staging import (
    gold_staging_table_name,
    ingestion_staging_table_name,
)
from commercelake.warehouse.loader import LOAD_ORDER

RUN_ID = "591c8097f0554414be18648e3587d2b3"


@pytest.mark.parametrize("table", LOAD_ORDER)
def test_ingestion_name_is_isolated(table):
    ingestion = ingestion_staging_table_name(table, RUN_ID)
    gold = gold_staging_table_name(table, RUN_ID)

    assert ingestion == f"_ingest_{table}_591c8097"
    assert gold == f"_stage_{table}_591c8097"
    assert ingestion != gold


@pytest.mark.parametrize(
    "table",
    ["unknown_table", "fact_sales;DROP TABLE"],
)
def test_rejects_invalid_table(table):
    with pytest.raises(ValueError, match="Unsupported"):
        ingestion_staging_table_name(table, RUN_ID)


@pytest.mark.parametrize(
    "run_id",
    [
        "",
        "invalid",
        "591c8097",
        "591c8097f0554414be18648e3587d2b3;DROP",
    ],
)
def test_rejects_invalid_run_id(run_id):
    with pytest.raises(ValueError, match="Invalid Gold run ID"):
        ingestion_staging_table_name("fact_sales", run_id)
