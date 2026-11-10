"""Integration checks for the published Gold monthly mart."""

import json
from decimal import Decimal

import pyarrow.parquet as pq
import pytest
from test_gold_pipeline import create_silver_source

from commercelake.gold.gold_pipeline import (
    build_gold,
    write_monthly_mart,
)
from commercelake.gold.monthly_mart import MonthlySalesCollector


def test_gold_publishes_monthly_mart_with_fact(tmp_path):
    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"

    create_silver_source(silver_root)

    manifest = build_gold(
        silver_root,
        gold_root,
        batch_size=2,
    )

    mart = manifest["marts"]["mart_monthly_sales"]

    assert mart["rows"] == 1
    assert mart["source_fact_rows"] == 3
    assert mart["net_amount"] == "10.0000"
    assert mart["first_month"] == "2010-01"
    assert mart["last_month"] == "2010-01"

    run_directory = gold_root / "runs" / manifest["run_id"]

    table = pq.read_table(run_directory / mart["path"])

    row = table.to_pylist()[0]

    assert row["transaction_count"] == 3
    assert row["sale_count"] == 1
    assert row["cancellation_count"] == 1
    assert row["review_count"] == 1
    assert row["net_amount"] == Decimal("10.0000")

    pointer = json.loads((gold_root / "current.json").read_text(encoding="utf-8"))

    assert pointer["run_id"] == manifest["run_id"]


def test_gold_rebuild_keeps_previous_version(tmp_path):
    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"

    create_silver_source(silver_root)

    first = build_gold(
        silver_root,
        gold_root,
        batch_size=2,
    )

    second = build_gold(
        silver_root,
        gold_root,
        batch_size=2,
    )

    assert first["run_id"] != second["run_id"]

    for manifest in (first, second):
        mart = manifest["marts"]["mart_monthly_sales"]

        assert (gold_root / "runs" / manifest["run_id"] / mart["path"]).is_file()

    pointer = json.loads((gold_root / "current.json").read_text(encoding="utf-8"))

    assert pointer["run_id"] == second["run_id"]


def test_monthly_mart_rejects_incorrect_fact_count(tmp_path):
    collector = MonthlySalesCollector()

    facts = {
        "total_rows": 5,
        "transaction_counts": {},
        "net_transaction_amount": "0.0000",
    }

    with pytest.raises(
        ValueError,
        match="fact row count mismatch",
    ):
        write_monthly_mart(
            tmp_path,
            collector,
            facts,
        )

    assert not (tmp_path / "marts" / "mart_monthly_sales.parquet").exists()
