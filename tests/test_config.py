"""Tests for application configuration."""

import pytest

from commercelake.config import (
    get_environment,
    get_gcp_project_id,
)


def test_default_environment(monkeypatch):
    monkeypatch.delenv("APP_ENV", raising=False)
    assert get_environment() == "development"


def test_custom_environment(monkeypatch):
    monkeypatch.setenv("APP_ENV", "testing")
    assert get_environment() == "testing"


def test_missing_gcp_project_id(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT_ID", raising=False)

    with pytest.raises(ValueError, match="GCP_PROJECT_ID"):
        get_gcp_project_id()


def test_valid_gcp_project_id(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "commercelake-demo")
    assert get_gcp_project_id() == "commercelake-demo"
