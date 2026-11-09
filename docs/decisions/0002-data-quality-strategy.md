# ADR-0002: Data Quality and Preservation

Status: Proposed

## Context

The source contains missing identifiers,
duplicate rows, cancellations, negative
quantities, and pricing anomalies.

## Decision

Preserve all original source records in Raw.

Apply explicit validation and classification
rules in downstream layers.

## Principles

- Never silently discard source records.
- Preserve source provenance.
- Keep quality flags auditable.
- Separate business rules from source facts.
- Test transformations automatically.
- Document exclusions from analytical KPIs.

## Customer identifiers

Missing customer identifiers must not cause
transaction loss.

An unknown-customer dimension member may be
used in analytical models.

## Duplicate handling

Exact duplicates are retained in Raw.

Deduplication is deferred until business
grain and source semantics are validated.

## Financial metrics

Positive quantities and positive prices
alone do not establish approved revenue.

Cancellation and return semantics must be
investigated before defining net revenue.

## Consequences

Staging models require quality flags.

Analytical marts must document filtering
and aggregation rules.
