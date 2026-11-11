"""Recovery diagnostics for versioned BigQuery Gold publication."""

from subprocess import CalledProcessError

import pytest
from test_bigquery_versioned_publisher import FakeBigQuery, make_plan

from commercelake.cloud.bigquery_versioned_publisher import (
    VersionPublicationError,
    publish_gold_version,
)


def test_copy_failure_identifies_uncertain_destination():
    fake = FakeBigQuery(fail_copy="dim_country")
    plan = make_plan()

    with pytest.raises(VersionPublicationError) as error:
        publish_gold_version(plan, runner=fake)

    assert error.value.phase == "copy"
    assert error.value.uncertain_destination == plan.tables[2].version_name
    assert error.value.completed_copies == tuple(
        table.version_name for table in plan.tables[:2]
    )


def test_first_copy_failure_has_no_confirmed_copies():
    fake = FakeBigQuery(fail_copy="dim_product")
    plan = make_plan()

    with pytest.raises(VersionPublicationError) as error:
        publish_gold_version(plan, runner=fake)

    assert error.value.phase == "copy"
    assert error.value.uncertain_destination == plan.tables[0].version_name
    assert error.value.completed_copies == ()


def test_verification_failure_identifies_affected_table():
    fake = FakeBigQuery(fail_verification="mart_monthly_sales")
    plan = make_plan()

    with pytest.raises(VersionPublicationError) as error:
        publish_gold_version(plan, runner=fake)

    assert error.value.phase == "verification"
    assert error.value.uncertain_destination == plan.tables[-1].version_name
    assert len(error.value.completed_copies) == 6


def test_schema_failure_is_reported_as_verification_failure():
    fake = FakeBigQuery(corrupt_version="fact_sales")
    plan = make_plan()

    with pytest.raises(VersionPublicationError) as error:
        publish_gold_version(plan, runner=fake)

    assert error.value.phase == "verification"
    assert error.value.uncertain_destination == plan.tables[4].version_name
    assert len(error.value.completed_copies) == 6


def test_copy_exception_preserves_diagnostics():
    fake = FakeBigQuery()
    plan = make_plan()
    original_copy = fake.copy

    def interrupted_copy(command):
        if command[-1].endswith(plan.tables[1].version_name):
            raise CalledProcessError(
                returncode=1,
                cmd=command,
                stderr="Simulated connection interruption",
            )
        return original_copy(command)

    fake.copy = interrupted_copy

    with pytest.raises(VersionPublicationError) as error:
        publish_gold_version(plan, runner=fake)

    assert error.value.phase == "copy"
    assert error.value.uncertain_destination == plan.tables[1].version_name
    assert error.value.completed_copies == (plan.tables[0].version_name,)
    assert isinstance(error.value.__cause__, CalledProcessError)


def test_failed_copy_does_not_trigger_later_copies():
    fake = FakeBigQuery(fail_copy="dim_country")

    with pytest.raises(VersionPublicationError):
        publish_gold_version(make_plan(), runner=fake)

    copy_commands = [command for command in fake.commands if command[2] == "cp"]

    assert len(copy_commands) == 3


def test_success_has_no_recovery_error():
    fake = FakeBigQuery()

    result = publish_gold_version(make_plan(), runner=fake)

    assert len(result.published_tables) == 6
    assert len(fake.versions) == 6
