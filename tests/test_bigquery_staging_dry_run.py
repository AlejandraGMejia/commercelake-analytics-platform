"""Tests for fail-closed BigQuery INSERT dry-run validation."""

import json

import pytest

from commercelake.cloud.bigquery_staging_dry_run import (
    build_fact_sales_insert_dry_run_command,
    validate_insert_dry_run,
)

PROJECT = "commercelake-analytics"
DATASET = "commercelake_gold"
RUN_ID = "591c8097f0554414be18648e3587d2b3"
SOURCE = "_ingest_fact_sales_591c8097"
BUDGET = 10485760


def make_response(estimate=0):
    """Build a valid response matching the observed BigQuery CLI format."""
    return {
        "configuration": {
            "dryRun": True,
            "jobType": "QUERY",
            "query": {
                "maximumBytesBilled": str(BUDGET),
            },
        },
        "status": {
            "state": "DONE",
        },
        "statistics": {
            "query": {
                "totalBytesProcessed": str(estimate),
                "totalBytesProcessedAccuracy": "PRECISE",
            },
        },
    }


def test_dry_run_uses_exact_insert():
    command = build_fact_sales_insert_dry_run_command(
        PROJECT,
        DATASET,
        RUN_ID,
        source_table=SOURCE,
        maximum_bytes_billed=BUDGET,
    )

    assert "--dry_run" in command
    assert "--format=prettyjson" in command
    assert f"--maximum_bytes_billed={BUDGET}" in command
    assert command[-1].startswith("INSERT INTO ")
    assert f"FROM `{PROJECT}.{DATASET}.{SOURCE}`" in command[-1]


@pytest.mark.parametrize("estimate", [0, 1024, BUDGET])
def test_accepts_estimate_within_budget(estimate):
    assert validate_insert_dry_run(
        json.dumps(make_response(estimate)),
        maximum_bytes_billed=BUDGET,
    ) == estimate


def test_rejects_over_budget():
    with pytest.raises(ValueError, match="exceeds budget"):
        validate_insert_dry_run(
            json.dumps(make_response(BUDGET + 1)),
            maximum_bytes_billed=BUDGET,
        )


@pytest.mark.parametrize(
    "output",
    [
        "",
        "not json",
        "[]",
        "{}",
        '{"statistics": {}}',
    ],
)
def test_rejects_invalid_json_or_structure(output):
    with pytest.raises((ValueError, TypeError)):
        validate_insert_dry_run(
            output,
            maximum_bytes_billed=BUDGET,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dryRun", False),
        ("dryRun", None),
        ("jobType", "LOAD"),
    ],
)
def test_rejects_invalid_configuration(field, value):
    response = make_response()
    response["configuration"][field] = value

    with pytest.raises(ValueError):
        validate_insert_dry_run(
            json.dumps(response),
            maximum_bytes_billed=BUDGET,
        )


@pytest.mark.parametrize("value", ["0", "10485761", None, True])
def test_rejects_budget_mismatch(value):
    response = make_response()
    response["configuration"]["query"]["maximumBytesBilled"] = value

    with pytest.raises(ValueError, match="budget mismatch"):
        validate_insert_dry_run(
            json.dumps(response),
            maximum_bytes_billed=BUDGET,
        )


def test_rejects_incomplete_job():
    response = make_response()
    response["status"]["state"] = "RUNNING"

    with pytest.raises(ValueError, match="did not complete"):
        validate_insert_dry_run(
            json.dumps(response),
            maximum_bytes_billed=BUDGET,
        )


def test_rejects_job_errors():
    response = make_response()
    response["status"]["errorResult"] = {
        "reason": "invalidQuery",
        "message": "Simulated failure",
    }

    with pytest.raises(ValueError, match="reported errors"):
        validate_insert_dry_run(
            json.dumps(response),
            maximum_bytes_billed=BUDGET,
        )


@pytest.mark.parametrize("estimate", [None, True, "-1", "unknown"])
def test_rejects_invalid_estimate(estimate):
    response = make_response()
    response["statistics"]["query"]["totalBytesProcessed"] = estimate

    with pytest.raises(ValueError, match="Invalid BigQuery byte estimate"):
        validate_insert_dry_run(
            json.dumps(response),
            maximum_bytes_billed=BUDGET,
        )
