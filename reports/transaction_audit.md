# CommerceLake - Transaction Audit

## Summary

- Source rows: 1,067,371
- Within-sheet duplicates: 11,812
- Cross-sheet duplicates: 22,523
- Cancellation invoice lines: 19,494
- Negative quantity lines: 22,950
- Cancellation + negative quantity: 19,493
- Negative quantity without cancellation: 3,457
- Cancellation without negative quantity: 1
- Zero-price lines: 6,202
- Negative-price lines: 5

## Methodology

Rows are compared using SHA256 fingerprints of normalized source values.

The first occurrence of each fingerprint determines whether later matches are within-sheet or cross-sheet duplicates.

Source records are preserved without modification.

## Interpretation

Cancellation invoice prefixes and negative quantities are separate signals.

These counts do not establish final business definitions for returns or net revenue.
