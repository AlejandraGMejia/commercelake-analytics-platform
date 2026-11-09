# Source Data Contract

## Primary source

Candidate: UCI Online Retail II.

Status: Pending dataset validation.

## Expected source fields

- Invoice
- StockCode
- Description
- Quantity
- InvoiceDate
- Price
- Customer ID
- Country

Column names and data types must be confirmed
against the actual source files.

## Business grain

One row represents a source invoice line.

The source may not provide a globally unique
line identifier. A deterministic technical
identifier will be designed after profiling.

## Initial validation rules

1. Invoice identifier must be present.
2. Product code must be present.
3. Invoice date must be parseable.
4. Quantity must be numeric.
5. Unit price must be numeric.
6. Duplicate handling must be documented.
7. Missing customer identifiers must be audited.
8. Returns and cancellations must be identified.

## Financial treatment

Gross sales, net sales, cancellations and
returns must be defined separately.

Negative quantities must not be silently
removed from the source.

## Data governance

- Preserve original raw files.
- Record source provenance.
- Avoid publishing customer-level personal data.
- Document dataset licensing and attribution.
