"""Strict recognition of BigQuery CLI table-not-found responses."""

import re


def is_explicit_table_not_found(
    error_output: str,
    *,
    project_id: str,
    dataset: str,
    table: str,
) -> bool:
    """Recognize only an exact BigQuery table-not-found message."""
    if not isinstance(error_output, str):
        return False

    identifier = f"{project_id}:{dataset}.{table}"

    pattern = (
        r"(?:BigQuery error in show operation: )?"
        r"Not found: Table "
        + re.escape(identifier)
        + r"(?: was not found in location [A-Za-z0-9_-]+)?"
    )

    return (
        re.fullmatch(
            pattern,
            error_output.strip(),
            flags=re.IGNORECASE,
        )
        is not None
    )


def is_known_cli_warning(stderr: str) -> bool:
    """Accept only the observed two-line SDK dependency warning."""
    if not isinstance(stderr, str):
        return False

    lines = stderr.strip().splitlines()

    if len(lines) != 2:
        return False

    first_line = lines[0]
    second_line = lines[1]

    warning_pattern = (
        r".*[/\\]platform[/\\]bq[/\\]third_party[/\\]"
        r"requests[/\\]__init__\.py:\d+: "
        r"RequestsDependencyWarning: "
        r"urllib3 \([^)]+\) or chardet \([^)]+\)/"
        r"charset_normalizer \([^)]+\) "
        r"doesn't match a supported version!"
    )

    second_line_pattern = (
        r'\s*warnings\.warn\("urllib3 \(\{\}\) or chardet \(\{\}\)/'
        r"charset_normalizer \(\{\}\) doesn\'t match "
        r'(?:a )?supported "'
        r"\)?"
    )

    return (
        re.fullmatch(warning_pattern, first_line) is not None
        and re.fullmatch(second_line_pattern, second_line) is not None
    )


def is_confirmed_missing(
    stdout: str | None,
    stderr: str | None,
    *,
    project_id: str,
    dataset: str,
    table: str,
) -> bool:
    """Confirm absence only when output contains no conflicting error."""
    stdout_missing = is_explicit_table_not_found(
        stdout,
        project_id=project_id,
        dataset=dataset,
        table=table,
    )

    stderr_missing = is_explicit_table_not_found(
        stderr,
        project_id=project_id,
        dataset=dataset,
        table=table,
    )

    if stdout_missing:
        return stderr is None or not stderr.strip() or is_known_cli_warning(stderr)

    if stderr_missing:
        return stdout is None or not stdout.strip()

    return False
