"""Safety tests for selecting CSV publication through the CLI."""

from unittest.mock import Mock, patch

import pytest

from commercelake.cloud.bigquery_orchestrator import (
    execute_staging_publication,
    validate_load_strategy,
)
from commercelake.cloud.bigquery_staging_cli import (
    EXECUTION_CONFIRMATION,
    main,
)
from commercelake.warehouse.loader import LOAD_ORDER

BASE_ARGS = [
    "--project-id",
    "commercelake-analytics",
    "--dataset",
    "commercelake_gold",
    "--location",
    "US",
    "--maximum-bytes-billed",
    "10485760",
]


def test_csv_execution_requires_confirmation():
    with patch(
        "commercelake.cloud.bigquery_staging_cli.production_bigquery_runner"
    ) as runner:
        with pytest.raises(ValueError, match="requires --confirm"):
            main(
                [
                    *BASE_ARGS,
                    "--load-strategy",
                    "csv_direct",
                    "--execute",
                ]
            )

        runner.assert_not_called()


def test_csv_execution_routes_to_selected_strategy():
    result = Mock()
    result.run_id = "591c8097f0554414be18648e3587d2b3"
    result.total_rows = 12
    result.table_row_counts = {"fact_sales": 12}

    with (
        patch(
            "commercelake.cloud.bigquery_staging_cli.production_bigquery_runner"
        ) as runner_factory,
        patch(
            "commercelake.cloud.bigquery_staging_cli.publish_gold_to_staging",
            return_value=result,
        ) as publish,
    ):
        main(
            [
                *BASE_ARGS,
                "--load-strategy",
                "csv_direct",
                "--execute",
                "--confirm",
                EXECUTION_CONFIRMATION,
            ]
        )

    runner_factory.assert_called_once()
    assert publish.call_args.kwargs["load_strategy"] == "csv_direct"
    assert publish.call_args.kwargs["maximum_bytes_billed"] == 10485760


def test_default_execution_preserves_legacy_strategy():
    result = Mock()
    result.run_id = "591c8097f0554414be18648e3587d2b3"
    result.total_rows = 12
    result.table_row_counts = {"fact_sales": 12}

    with (
        patch("commercelake.cloud.bigquery_staging_cli.production_bigquery_runner"),
        patch(
            "commercelake.cloud.bigquery_staging_cli.publish_gold_to_staging",
            return_value=result,
        ) as publish,
    ):
        main(
            [
                *BASE_ARGS,
                "--execute",
                "--confirm",
                EXECUTION_CONFIRMATION,
            ]
        )

    assert "load_strategy" not in publish.call_args.kwargs


def test_csv_dry_run_never_initializes_cloud_runner():
    with (
        patch("commercelake.cloud.bigquery_staging_cli.build_plan") as build_plan,
        patch(
            "commercelake.cloud.bigquery_staging_cli.plan_as_dict",
            return_value={"run_id": "test"},
        ),
        patch(
            "commercelake.cloud.bigquery_staging_cli.production_bigquery_runner"
        ) as runner,
    ):
        assert (
            main(
                [
                    *BASE_ARGS,
                    "--load-strategy",
                    "csv_direct",
                    "--dry-run",
                ]
            )
            == 0
        )

    build_plan.assert_called_once()
    runner.assert_not_called()


@pytest.mark.parametrize("strategy", ["unknown", "", "csv"])
def test_rejects_unsupported_strategies(strategy):
    with pytest.raises(ValueError, match="Unsupported"):
        validate_load_strategy(strategy)


def test_csv_orchestrator_selects_csv_runner():
    plan = Mock()
    plan.files_by_table = {
        table: [(f"{table}.parquet", index)]
        for index, table in enumerate(LOAD_ORDER, start=1)
    }
    plan.project_id = "commercelake-analytics"
    plan.dataset = "commercelake_gold"
    plan.run_id = "591c8097f0554414be18648e3587d2b3"

    expected = {table: index for index, table in enumerate(LOAD_ORDER, start=1)}

    financial = {
        "fact_rows": expected["fact_sales"],
        "monthly_periods": expected["mart_monthly_sales"],
    }

    class FakePreparation:
        def __enter__(self):
            return {table: f"/tmp/{table}.parquet" for table in LOAD_ORDER}

        def __exit__(self, exc_type, exc, traceback):
            return False

    fake_runner = Mock()

    with (
        patch(
            "commercelake.cloud.bigquery_orchestrator.prepare_gold_files",
            return_value=FakePreparation(),
        ),
        patch(
            "commercelake.cloud.bigquery_orchestrator.validate_gold_financials",
            return_value=financial,
        ),
        patch(
            "commercelake.cloud.bigquery_orchestrator.publish_csv_staging_tables",
            return_value=expected,
        ) as csv_publish,
        patch(
            "commercelake.cloud.bigquery_orchestrator.publish_staging_tables"
        ) as legacy_publish,
    ):
        result = execute_staging_publication(
            plan,
            runner=fake_runner,
            maximum_bytes_billed=10485760,
            load_strategy="csv_direct",
        )

    assert result.table_row_counts == expected
    csv_publish.assert_called_once()
    legacy_publish.assert_not_called()


def test_invalid_strategy_blocks_preparation():
    plan = Mock()
    runner = Mock()

    with (
        patch("commercelake.cloud.bigquery_orchestrator.prepare_gold_files") as prepare,
        pytest.raises(ValueError, match="Unsupported"),
    ):
        execute_staging_publication(
            plan,
            runner=runner,
            maximum_bytes_billed=10485760,
            load_strategy="unknown",
        )

    prepare.assert_not_called()
    runner.assert_not_called()
