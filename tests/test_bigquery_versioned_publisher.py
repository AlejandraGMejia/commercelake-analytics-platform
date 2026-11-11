"""Integration-style tests for guarded BigQuery Gold version publication."""

import json
from subprocess import CompletedProcess

import pytest

from commercelake.cloud.bigquery_promotion import build_promotion_plan
from commercelake.cloud.bigquery_schema_validation import expected_bigquery_schema
from commercelake.cloud.bigquery_versioned_publisher import (
    VersionPublicationError,
    publish_gold_version,
)
from commercelake.warehouse.loader import LOAD_ORDER

RUN_ID = "591c8097f0554414be18648e3587d2b3"

COUNTS = {
    "dim_product": 5305,
    "dim_customer": 5943,
    "dim_country": 44,
    "dim_date": 739,
    "fact_sales": 1067371,
    "mart_monthly_sales": 25,
}


def make_plan():
    """Return a canonical promotion plan."""
    return build_promotion_plan(
        project_id="commercelake-analytics",
        dataset="commercelake_gold",
        run_id=RUN_ID,
        expected_counts=COUNTS,
    )


class FakeBigQuery:
    """Simulate metadata lookups and non-overwriting table copies."""

    def __init__(
        self,
        *,
        existing_destination=None,
        fail_copy=None,
        fail_verification=None,
        corrupt_staging=None,
        corrupt_version=None,
    ):
        self.plan = make_plan()
        self.commands = []
        self.fail_copy = fail_copy
        self.fail_verification = fail_verification
        self.corrupt_staging = corrupt_staging
        self.corrupt_version = corrupt_version

        self.staging = {
            table.staging_name: table.logical_name for table in self.plan.tables
        }
        self.versions = {}

        if existing_destination is not None:
            table = next(
                table
                for table in self.plan.tables
                if table.logical_name == existing_destination
            )
            self.versions[table.version_name] = table.logical_name

    @staticmethod
    def completed(command, returncode=0, stdout="", stderr=""):
        return CompletedProcess(
            args=command,
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
        )

    def __call__(self, command):
        self.commands.append(tuple(command))

        if command[2] == "show":
            return self.show(command)

        if command[2] == "cp":
            return self.copy(command)

        raise AssertionError(f"Unexpected command: {command}")

    def show(self, command):
        identifier = command[-1]
        name = identifier.split(".")[-1]

        if name in self.staging:
            logical_name = self.staging[name]
            is_version = False
        elif name in self.versions:
            logical_name = self.versions[name]
            is_version = True
        else:
            return self.completed(
                command,
                returncode=1,
                stderr=f"Not found: Table {identifier}",
            )

        if is_version and logical_name == self.fail_verification:
            return self.completed(
                command,
                returncode=1,
                stderr="Simulated metadata failure",
            )

        rows = COUNTS[logical_name]
        fields = expected_bigquery_schema(logical_name)

        if not is_version and logical_name == self.corrupt_staging:
            rows += 1

        if is_version and logical_name == self.corrupt_version:
            fields[0]["type"] = "BYTES"

        metadata = json.dumps(
            {
                "numRows": str(rows),
                "schema": {"fields": fields},
            }
        )

        return self.completed(command, stdout=metadata)

    def copy(self, command):
        assert "--no_clobber" in command
        assert "--force" not in command
        assert "--replace" not in command

        source = command[-2].split(".")[-1]
        destination = command[-1].split(".")[-1]

        if source not in self.staging:
            return self.completed(
                command,
                returncode=1,
                stderr="Source does not exist",
            )

        logical_name = self.staging[source]

        if logical_name == self.fail_copy:
            return self.completed(
                command,
                returncode=1,
                stderr="Simulated copy failure",
            )

        if destination in self.versions:
            return self.completed(
                command,
                returncode=1,
                stderr="Destination already exists",
            )

        self.versions[destination] = logical_name
        return self.completed(command)


def test_successful_publication_validates_six_versions():
    fake = FakeBigQuery()

    result = publish_gold_version(make_plan(), runner=fake)

    assert result.run_id == RUN_ID
    assert len(result.published_tables) == 6
    assert result.total_rows == sum(COUNTS.values())
    assert len(fake.versions) == 6

    operations = [command[2] for command in fake.commands]

    assert operations == ["show"] * 12 + ["cp"] * 6 + ["show"] * 6


def test_existing_destination_blocks_all_copies():
    fake = FakeBigQuery(existing_destination="dim_product")

    with pytest.raises(FileExistsError, match="already exists"):
        publish_gold_version(make_plan(), runner=fake)

    assert not any(command[2] == "cp" for command in fake.commands)


def test_staging_validation_failure_blocks_all_copies():
    fake = FakeBigQuery(corrupt_staging="dim_customer")

    with pytest.raises(ValueError, match="row count mismatch"):
        publish_gold_version(make_plan(), runner=fake)

    assert not any(command[2] == "cp" for command in fake.commands)
    assert fake.versions == {}


def test_copy_failure_reports_completed_tables():
    fake = FakeBigQuery(fail_copy="dim_country")

    with pytest.raises(VersionPublicationError) as error:
        publish_gold_version(make_plan(), runner=fake)

    expected = tuple(table.version_name for table in make_plan().tables[:2])

    assert error.value.completed_copies == expected
    assert len(fake.versions) == 2

    operations = [command[2] for command in fake.commands]
    assert operations.count("cp") == 3


def test_first_copy_failure_reports_no_completed_tables():
    fake = FakeBigQuery(fail_copy="dim_product")

    with pytest.raises(VersionPublicationError) as error:
        publish_gold_version(make_plan(), runner=fake)

    assert error.value.completed_copies == ()
    assert fake.versions == {}


def test_post_copy_schema_failure_blocks_success():
    fake = FakeBigQuery(corrupt_version="fact_sales")

    with pytest.raises(VersionPublicationError) as error:
        publish_gold_version(make_plan(), runner=fake)

    assert len(error.value.completed_copies) == 6
    assert len(fake.versions) == 6
    assert isinstance(error.value.__cause__, ValueError)


def test_post_copy_metadata_failure_blocks_success():
    fake = FakeBigQuery(fail_verification="mart_monthly_sales")

    with pytest.raises(VersionPublicationError) as error:
        publish_gold_version(make_plan(), runner=fake)

    assert len(error.value.completed_copies) == 6
    assert isinstance(error.value.__cause__, RuntimeError)


def test_missing_runner_is_rejected():
    with pytest.raises(TypeError, match="callable runner"):
        publish_gold_version(make_plan(), runner=None)


def test_no_activation_or_deletion_commands():
    fake = FakeBigQuery()

    publish_gold_version(make_plan(), runner=fake)

    assert all(command[2] in {"show", "cp"} for command in fake.commands)
    assert not any(
        token in command
        for command in fake.commands
        for token in ("rm", "load", "query", "--replace", "--force")
    )


def test_second_publication_is_blocked():
    fake = FakeBigQuery()
    plan = make_plan()

    publish_gold_version(plan, runner=fake)
    first_copy_count = sum(command[2] == "cp" for command in fake.commands)

    with pytest.raises(FileExistsError, match="already exists"):
        publish_gold_version(plan, runner=fake)

    second_copy_count = sum(command[2] == "cp" for command in fake.commands)

    assert first_copy_count == 6
    assert second_copy_count == 6
    assert len(fake.versions) == 6


def test_published_tables_follow_load_order():
    fake = FakeBigQuery()

    result = publish_gold_version(make_plan(), runner=fake)

    assert result.published_tables == tuple(
        f"gold_{table}_{RUN_ID}" for table in LOAD_ORDER
    )
