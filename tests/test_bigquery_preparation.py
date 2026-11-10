"""Tests for preparing Gold Parquet files for BigQuery."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from gold_test_data import write_gold_parquet

from commercelake.cloud.bigquery_preparation import (
    prepare_gold_files,
    validate_prepared_file,
)
from commercelake.cloud.bigquery_publisher import PublicationPlan
from commercelake.warehouse.loader import LOAD_ORDER, TABLE_SCHEMAS

RUN_ID = "591c8097f0554414be18648e3587d2b3"


def make_plan(tmp_path, split_fact_sales=False):
    """Build a synthetic plan backed by Gold-compatible Parquet."""
    gold_directory = tmp_path / "gold"
    gold_directory.mkdir()

    files_by_table = {}

    for index, table in enumerate(LOAD_ORDER, start=1):
        if table == "fact_sales" and split_fact_sales:
            parts = [
                (f"{table}_part1.parquet", 2),
                (f"{table}_part2.parquet", 3),
            ]
        else:
            parts = [(f"{table}.parquet", index)]

        files_by_table[table] = []

        for filename, row_count in parts:
            write_gold_parquet(
                gold_directory / filename,
                table,
                row_count,
            )
            files_by_table[table].append((filename, row_count))

    return PublicationPlan(
        project_id="commercelake-analytics",
        dataset="commercelake_gold",
        location="US",
        run_id=RUN_ID,
        run_directory=gold_directory,
        files_by_table=files_by_table,
    )


def test_prepares_all_six_tables(tmp_path):
    plan = make_plan(tmp_path)

    with prepare_gold_files(plan) as prepared:
        assert set(prepared) == set(LOAD_ORDER)

        for table in LOAD_ORDER:
            assert prepared[table].is_file()
            assert pq.read_schema(prepared[table]).equals(
                TABLE_SCHEMAS[table],
                check_metadata=False,
            )


def test_reuses_single_file_tables(tmp_path):
    plan = make_plan(tmp_path)

    with prepare_gold_files(plan) as prepared:
        for table in LOAD_ORDER:
            assert (
                prepared[table] == (plan.run_directory / f"{table}.parquet").resolve()
            )


def test_consolidates_fragmented_fact_sales(tmp_path):
    plan = make_plan(tmp_path, split_fact_sales=True)
    temporary_root = tmp_path / "temporary"

    with prepare_gold_files(
        plan,
        temporary_root=temporary_root,
    ) as prepared:
        fact_path = prepared["fact_sales"]

        assert fact_path.is_file()
        assert fact_path.parent != plan.run_directory
        assert pq.read_metadata(fact_path).num_rows == 5
        assert pq.read_schema(fact_path).equals(
            TABLE_SCHEMAS["fact_sales"],
            check_metadata=False,
        )
        assert prepared["dim_product"].parent == (plan.run_directory.resolve())

    assert not fact_path.exists()


def test_cleans_temporary_files_after_exception(tmp_path):
    plan = make_plan(tmp_path, split_fact_sales=True)

    with (
        pytest.raises(RuntimeError, match="simulated"),
        prepare_gold_files(plan) as prepared,
    ):
        temporary_file = prepared["fact_sales"]
        assert temporary_file.exists()
        raise RuntimeError("simulated")

    assert not temporary_file.exists()


def test_rejects_missing_source_file(tmp_path):
    plan = make_plan(tmp_path)
    (plan.run_directory / "dim_product.parquet").unlink()

    with pytest.raises(FileNotFoundError), prepare_gold_files(plan):
        pass


def test_rejects_incorrect_manifest_count(tmp_path):
    plan = make_plan(tmp_path)
    plan.files_by_table["dim_product"] = [("dim_product.parquet", 999)]

    with (
        pytest.raises(ValueError, match="row count mismatch"),
        prepare_gold_files(plan),
    ):
        pass


def test_rejects_incomplete_inventory(tmp_path):
    plan = make_plan(tmp_path)
    plan.files_by_table.pop("fact_sales")

    with (
        pytest.raises(ValueError, match="all six Gold tables"),
        prepare_gold_files(plan),
    ):
        pass


def test_rejects_empty_inventory(tmp_path):
    plan = make_plan(tmp_path)
    plan.files_by_table["fact_sales"] = []

    with (
        pytest.raises(ValueError, match="Empty Gold inventory"),
        prepare_gold_files(plan),
    ):
        pass


def test_rejects_negative_expected_rows(tmp_path):
    path = tmp_path / "sample.parquet"
    write_gold_parquet(path, "dim_product", 2)

    with pytest.raises(ValueError, match="cannot be negative"):
        validate_prepared_file(path, -1)


def test_rejects_prepared_file_row_mismatch(tmp_path):
    path = tmp_path / "sample.parquet"
    write_gold_parquet(path, "dim_product", 2)

    with pytest.raises(ValueError, match="row count mismatch"):
        validate_prepared_file(path, 3)


def test_rejects_missing_column(tmp_path):
    plan = make_plan(tmp_path)
    path = plan.run_directory / "dim_product.parquet"

    schema = pa.schema(list(TABLE_SCHEMAS["dim_product"])[:-1])
    write_gold_parquet(path, "dim_product", 1, schema=schema)

    with (
        pytest.raises(ValueError, match="schema mismatch"),
        prepare_gold_files(plan),
    ):
        pass


def test_rejects_wrong_column_type(tmp_path):
    plan = make_plan(tmp_path)
    path = plan.run_directory / "dim_customer.parquet"

    schema = TABLE_SCHEMAS["dim_customer"]
    wrong_schema = schema.set(
        0,
        pa.field("customer_key", pa.int64(), nullable=False),
    )
    write_gold_parquet(
        path,
        "dim_customer",
        2,
        schema=wrong_schema,
    )

    with (
        pytest.raises(ValueError, match="schema mismatch"),
        prepare_gold_files(plan),
    ):
        pass


def test_rejects_wrong_decimal_precision(tmp_path):
    plan = make_plan(tmp_path)
    path = plan.run_directory / "fact_sales.parquet"

    schema = TABLE_SCHEMAS["fact_sales"]
    index = schema.get_field_index("gross_amount")
    wrong_schema = schema.set(
        index,
        pa.field(
            "gross_amount",
            pa.decimal128(18, 4),
            nullable=True,
        ),
    )
    write_gold_parquet(
        path,
        "fact_sales",
        5,
        schema=wrong_schema,
    )

    with (
        pytest.raises(ValueError, match="schema mismatch"),
        prepare_gold_files(plan),
    ):
        pass


def test_rejects_wrong_nullability(tmp_path):
    plan = make_plan(tmp_path)
    path = plan.run_directory / "dim_country.parquet"

    schema = TABLE_SCHEMAS["dim_country"]
    wrong_schema = schema.set(
        0,
        pa.field("country_key", pa.string(), nullable=True),
    )
    write_gold_parquet(
        path,
        "dim_country",
        3,
        schema=wrong_schema,
    )

    with (
        pytest.raises(ValueError, match="schema mismatch"),
        prepare_gold_files(plan),
    ):
        pass
