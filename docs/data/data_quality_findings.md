# CommerceLake - Data Quality Findings

## Source

UCI Online Retail II

Period: December 2009 - December 2011

Source records: 1,067,371

## Observed quality findings

| Finding | Affected rows |
|---|---:|
| Missing customer identifiers | 243,007 |
| Exact duplicates within worksheets | 12,133 |
| Negative quantities | 22,950 |
| Cancellation invoice lines | 19,494 |
| Zero prices | 6,202 |
| Missing descriptions | 4,382 |
| Negative prices | 5 |

These categories are not mutually exclusive.

## Important limitations

Duplicate detection was performed separately
within each worksheet.

Cross-worksheet duplicates have not yet
been investigated.

Invoice identifiers beginning with C are
treated as cancellation candidates.

Negative quantities require further analysis
before being classified as returns.

The monetary figures produced during profiling
are diagnostics, not approved revenue KPIs.

## Data preservation

All original records will be retained in
the Raw layer.

Quality flags and classification rules will
be applied in downstream transformations.

## Next investigations

1. Cross-worksheet duplicate detection.
2. Cancellation and negative quantity overlap.
3. Zero-price transaction classification.
4. Customer identifier completeness.
5. Product description inconsistencies.
6. Approved financial metric definitions.
