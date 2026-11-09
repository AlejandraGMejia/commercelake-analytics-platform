# Online Retail II - Data Quality Profile

## Source

UCI Machine Learning Repository - Online Retail II.

## Summary

- Total source rows: 1,067,371
- Exact duplicate rows: 12,133

Duplicates are counted within each worksheet.
They are not automatically removed.

## Year 2009-2010

- Rows: 525,461
- Exact duplicates: 6,865
- Cancellation invoice lines: 10,206
- Negative quantities: 12,326
- Zero quantities: 0
- Negative prices: 3
- Zero prices: 3,687
- Invalid dates: 0
- Invalid quantities: 0
- Invalid prices: 0
- Earliest date: 2009-12-01
- Latest date: 2010-12-09
- Distinct countries: 40

### Missing values

| Column | Missing |
|---|---:|
| Invoice | 0 |
| StockCode | 0 |
| Description | 2,928 |
| Quantity | 0 |
| InvoiceDate | 0 |
| Price | 0 |
| Customer ID | 107,927 |
| Country | 0 |

### Top 10 countries by row count

| Country | Rows |
|---|---:|
| United Kingdom | 485,852 |
| EIRE | 9,670 |
| Germany | 8,129 |
| France | 5,772 |
| Netherlands | 2,769 |
| Spain | 1,278 |
| Switzerland | 1,187 |
| Portugal | 1,101 |
| Belgium | 1,054 |
| Channel Islands | 906 |

### Preliminary monetary diagnostics

- Positive quantity/price line amount: 10,306,265.59
- Negative quantity line amount: -630,228.94

These figures are diagnostics, not approved business revenue KPIs.

## Year 2010-2011

- Rows: 541,910
- Exact duplicates: 5,268
- Cancellation invoice lines: 9,288
- Negative quantities: 10,624
- Zero quantities: 0
- Negative prices: 2
- Zero prices: 2,515
- Invalid dates: 0
- Invalid quantities: 0
- Invalid prices: 0
- Earliest date: 2010-12-01
- Latest date: 2011-12-09
- Distinct countries: 38

### Missing values

| Column | Missing |
|---|---:|
| Invoice | 0 |
| StockCode | 0 |
| Description | 1,454 |
| Quantity | 0 |
| InvoiceDate | 0 |
| Price | 0 |
| Customer ID | 135,080 |
| Country | 0 |

### Top 10 countries by row count

| Country | Rows |
|---|---:|
| United Kingdom | 495,478 |
| Germany | 9,495 |
| France | 8,558 |
| EIRE | 8,196 |
| Spain | 2,533 |
| Netherlands | 2,371 |
| Belgium | 2,069 |
| Switzerland | 2,002 |
| Portugal | 1,519 |
| Australia | 1,259 |

### Preliminary monetary diagnostics

- Positive quantity/price line amount: 10,666,702.54
- Negative quantity line amount: -896,812.49

These figures are diagnostics, not approved business revenue KPIs.

## Pending modeling decisions

- Confirm cancellation and return semantics.
- Investigate duplicate source lines.
- Determine treatment of unknown customers.
- Validate product identifiers and descriptions.
- Define approved revenue calculations.
- Investigate anomalies before applying filters.
