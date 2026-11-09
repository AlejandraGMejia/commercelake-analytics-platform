"""Application configuration."""

import os


def get_environment() -> str:
    """Return the current application environment."""
    return os.getenv("APP_ENV", "development")


def get_gcp_project_id() -> str:
    """Return the configured GCP project ID."""
    project_id = os.getenv("GCP_PROJECT_ID", "").strip()

    if not project_id:
        raise ValueError("GCP_PROJECT_ID is not configured")

    return project_id
