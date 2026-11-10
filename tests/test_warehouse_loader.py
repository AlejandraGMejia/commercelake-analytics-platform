"""Tests for the Gold-to-PostgreSQL warehouse loader."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from commercelake.warehouse import loader


def test_safe_file_accepts_existing_file(tmp_path):
    file_path = tmp_path / "dimensions" / "dim_product.parquet"
    file_path.parent.mkdir()
    file_path.touch()

    assert (
        loader.safe_file(tmp_path, "dimensions/dim_product.parquet")
        == file_path.resolve()
    )


@pytest.mark.parametrize(
    "relative_path",
    [
        "../outside.parquet",
        "/tmp/outside.parquet",
        "dimensions/../../outside.parquet",
    ],
)
def test_safe_file_rejects_unsafe_paths(tmp_path, relative_path):
    with pytest.raises(ValueError):
        loader.safe_file(tmp_path, relative_path)


def test_safe_file_rejects_invalid_type(tmp_path):
    with pytest.raises(TypeError):
        loader.safe_file(tmp_path, 123)


def test_load_manifest_rejects_incomplete_source(tmp_path):
    root = tmp_path / "gold"
    run_id = "a" * 32
    run_directory = root / "runs" / run_id
    run_directory.mkdir(parents=True)

    (root / "current.json").write_text(json.dumps({"layer": "gold", "run_id": run_id}))
    (run_directory / "manifest.json").write_text(
        json.dumps(
            {
                "layer": "gold",
                "run_id": run_id,
                "complete_source": False,
            }
        )
    )

    with pytest.raises(ValueError, match="Incomplete"):
        loader.load_manifest(root)


def test_inventory_rejects_duplicate_paths():
    manifest = {
        "dimensions": {
            name: {"path": "duplicate.parquet", "rows": 1} for name in loader.DIMENSIONS
        },
        "fact_sales": {
            "files": [{"path": "fact.parquet", "rows": 2}],
            "total_rows": 2,
        },
        "marts": {
            "mart_monthly_sales": {
                "path": "mart.parquet",
                "rows": 1,
                "source_fact_rows": 2,
            }
        },
    }

    with pytest.raises(ValueError, match="Duplicate"):
        loader.inventory(manifest)


def test_transaction_rolls_back_on_publish_failure(tmp_path):
    """An exception inside publication must abort the transaction."""
    run_id = "a" * 32
    root = tmp_path / "gold"
    root.mkdir()

    (root / "current.json").write_text(json.dumps({"layer": "gold", "run_id": run_id}))

    connection = MagicMock()
    transaction = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = connection
    connection.transaction.return_value = transaction
    transaction.__enter__.return_value = None
    connection.cursor.return_value = cursor
    cursor.__enter__.return_value = cursor

    args = SimpleNamespace(
        gold_root=str(root),
        batch_size=100,
    )

    with (
        patch.object(
            loader,
            "load_manifest",
            return_value=(run_id, root, {}),
        ),
        patch.object(
            loader,
            "inventory",
            return_value={name: [] for name in loader.LOAD_ORDER},
        ),
        patch.object(loader, "validate_files"),
        patch.object(loader, "connect", return_value=connection),
        patch.object(loader, "create_staging"),
        patch.object(loader, "copy_table"),
        patch.object(loader, "check_staging"),
        patch.object(
            loader,
            "publish",
            side_effect=RuntimeError("Simulated failure"),
        ),
        pytest.raises(RuntimeError, match="Simulated failure"),
    ):
        loader.run(args)

    assert transaction.__exit__.called

    exit_args = transaction.__exit__.call_args.args
    assert exit_args[0] is RuntimeError
