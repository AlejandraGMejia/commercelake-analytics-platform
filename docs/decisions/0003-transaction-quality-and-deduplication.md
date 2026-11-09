# ADR 0003: Transaction Quality and Deduplication

## Status

Accepted for initial analytical design.

## Context

The UCI Online Retail II dataset contains 1,067,371
transaction lines across two Excel worksheets.

Source profiling and transaction auditing identified:

- 12,133 within-sheet duplicate occurrences in the
  original independent worksheet profiling.
- 11,812 within-sheet duplicate occurrences according
  to the global first-seen fingerprint classification.
- 22,523 second-sheet occurrences matching fingerprints
  previously observed in the first worksheet.
- 19,494 cancellation-prefixed invoice lines.
- 22,950 negative-quantity lines.
- 3,457 negative-quantity lines without cancellation prefix.
- 1 cancellation-prefixed line without negative quantity.
- 6,202 zero-price lines.
- 5 negative-price lines.

The two duplicate-counting methods answer different
questions and should not be treated as interchangeable.

The global audit classifies repeated rows according
to the worksheet of their first observed fingerprint.
Consequently, some repeated rows within the second
worksheet are classified as cross-sheet matches.

## Decision

1. Preserve the original source records in the Raw layer.
2. Retain source worksheet and source row identifiers.
3. Generate a deterministic content fingerprint.
4. Maintain separate cancellation, quantity and price flags.
5. Distinguish independent within-sheet duplicate counts
   from globally classified repeated occurrences.
6. Do not automatically delete matching transaction lines.
7. Do not assume every negative quantity represents a
   confirmed customer return.
8. Defer final net-revenue definitions until analytical
   business rules have been explicitly validated.

## Proposed Staging Fields

- source_sheet
- source_row_number
- row_fingerprint
- is_cancellation_invoice
- is_negative_quantity
- is_zero_price
- is_negative_price

## Consequences

The platform will preserve auditability and allow
downstream models to implement explicit business rules.

Matching source rows will be flagged rather than
silently discarded.

Any deduplication applied to analytical models must
document its scope, rationale and financial impact.

## Validation

The transaction audit produced structured JSON and
Markdown reports.

The project test suite passed 22 tests after the
transaction-audit tests were introduced.

## Limitations

A matching fingerprint identifies equal normalized
row content, not necessarily an erroneous duplicate
business event.

The reported cross-sheet count is based on the
auditor's first-seen classification and does not
represent a complete pairwise overlap analysis.

The distinction between physical returns, adjustments,
cancellations and other negative movements remains
subject to further business interpretation.
