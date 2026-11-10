"""Tests for safe Gold Parquet consolidation."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from commercelake.cloud.parquet_consolidator import consolidate_parquet


def write_parquet(path, values, column="value"):
    """Create a small Parquet source for testing."""
    path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.table({column: values})
    pq.write_table(table, path)

    return len(values)


def test_consolidates_multiple_files(tmp_path):
    run = tmp_path / "gold"
    output = tmp_path / "output" / "combined.parquet"

    first = write_parquet(run / "first.parquet", [1, 2, 3])
    second = write_parquet(run / "second.parquet", [4, 5])

    total = consolidate_parquet(
        run,
        [
            ("first.parquet", first),
            ("second.parquet", second),
        ],
        output,
        batch_size=2,
    )

    result = pq.read_table(output)

    assert total == 5
    assert result.column("value").to_pylist() == [1, 2, 3, 4, 5]


def test_rejects_empty_inventory(tmp_path):
    with pytest.raises(ValueError, match="empty"):
        consolidate_parquet(
            tmp_path / "gold",
            [],
            tmp_path / "output.parquet",
        )


def test_rejects_invalid_batch_size(tmp_path):
    with pytest.raises(ValueError, match="positive"):
        consolidate_parquet(
            tmp_path / "gold",
            [("first.parquet", 1)],
            tmp_path / "output.parquet",
            batch_size=0,
        )


def test_rejects_output_inside_gold_run(tmp_path):
    run = tmp_path / "gold"
    write_parquet(run / "source.parquet", [1])

    with pytest.raises(ValueError, match="inside the Gold run"):
        consolidate_parquet(
            run,
            [("source.parquet", 1)],
            run / "combined.parquet",
        )


def test_rejects_source_count_mismatch(tmp_path):
    run = tmp_path / "gold"
    output = tmp_path / "combined.parquet"

    write_parquet(run / "source.parquet", [1, 2])

    with pytest.raises(ValueError, match="row count mismatch"):
        consolidate_parquet(
            run,
            [("source.parquet", 3)],
            output,
        )

    assert not output.exists()


def test_rejects_schema_mismatch(tmp_path):
    run = tmp_path / "gold"
    output = tmp_path / "combined.parquet"

    write_parquet(run / "first.parquet", [1], column="value")
    write_parquet(run / "second.parquet", [2], column="other")

    with pytest.raises(ValueError, match="schema mismatch"):
        consolidate_parquet(
            run,
            [
                ("first.parquet", 1),
                ("second.parquet", 1),
            ],
            output,
        )

    assert not output.exists()


def test_preserves_existing_output_on_failure(tmp_path):
    run = tmp_path / "gold"
    output = tmp_path / "combined.parquet"

    write_parquet(run / "source.parquet", [1, 2])
    output.write_bytes(b"previous-valid-output")

    with pytest.raises(ValueError, match="row count mismatch"):
        consolidate_parquet(
            run,
            [("source.parquet", 99)],
            output,
        )

    assert output.read_bytes() == b"previous-valid-output"


def test_rejects_unsafe_source_path(tmp_path):
    run = tmp_path / "gold"

    with pytest.raises(ValueError, match="Unsafe Gold path"):
        consolidate_parquet(
            run,
            [("../outside.parquet", 1)],
            tmp_path / "combined.parquet",
        )
