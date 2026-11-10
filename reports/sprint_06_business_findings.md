# CommerceLake — Sprint 06 Business Findings

## Executive summary

CommerceLake's PostgreSQL analytical warehouse contains
1,067,371 transaction lines across 25 monthly periods,
from December 2009 through December 2011.

The validated net transaction amount is 19,445,926.7080
in source monetary units.

All financial aggregation paths reconcile exactly.

## 1. Transaction classification

| Transaction type | Lines | Net amount |
|---|---:|---:|
| Sale | 1,041,670 | 20,972,594.5680 |
| Cancellation | 19,494 | -1,526,667.8600 |
| Return | 3,457 | 0.0000 |
| Review | 2,750 | NULL |
| **Total** | **1,067,371** | **19,445,926.7080** |

Sales and cancellations determine the observed net amount.
Return lines have zero aggregate net amount under the
current transformation rules.

Review lines retain NULL net amounts and are excluded
from financial aggregation.

The source currency is not asserted.

## 2. Monthly performance

The highest monthly net amount occurred in November 2011:

- Period: 2011-11
- Net amount: 1,461,756.2500
- Sale lines: 83,369
- Month-over-month growth: 36.52%

November 2010 also showed strong activity:

- Period: 2010-11
- Net amount: 1,422,654.6420
- Sale lines: 76,464
- Month-over-month growth: 31.23%

These observations are compatible with increased activity
toward the final quarter, but the historical coverage is
insufficient to establish a stable seasonal pattern.

December 2011 is an incomplete reporting month.
Its apparent month-over-month decline of 70.33% must
not be interpreted as a full-month business decline.

## 3. Leading product and service codes

| Stock code | Description | Net amount |
|---|---|---:|
| 22423 | REGENCY CAKESTAND 3 TIER | 327,813.6500 |
| DOT | DOTCOM POSTAGE | 322,647.4700 |
| 85123A | WHITE HANGING HEART T-LIGHT HOLDER | 253,720.0200 |
| 85099B | JUMBO BAG RED RETROSPOT | 181,278.5100 |
| 47566 | PARTY BUNTING | 147,948.5000 |

The ranking includes postage-related codes.

Therefore, product-code performance should not be
presented as merchandise-only revenue without an
explicit product/service classification.

## 4. Geographic performance

| Country | Net amount |
|---|---:|
| United Kingdom | 16,541,260.0370 |
| EIRE | 615,519.5500 |
| Netherlands | 548,524.9500 |
| Germany | 417,988.5610 |
| France | 328,191.8000 |

The United Kingdom contributes approximately 85.1%
of the observed net amount.

This indicates substantial geographic concentration
in the historical dataset.

## 5. Leading identified customers

| Customer ID | Net amount |
|---|---:|
| 18102 | 598,215.2200 |
| 14646 | 523,342.0700 |
| 14156 | 296,564.6900 |
| 14911 | 270,248.5300 |
| 17450 | 233,579.3900 |

Customer rankings include only identified customers.

Because customer IDs are missing in many transaction
lines, these results must not be interpreted as a
complete analysis of customer concentration.

## 6. Data quality findings

| Indicator | Rows |
|---|---:|
| Missing customer ID | 243,007 |
| Non-positive price | 6,207 |
| Missing date | 0 |
| Review classification | 2,750 |
| NULL net amount | 2,750 |
| Repeated fingerprints | 34,335 |

Repeated fingerprints count occurrences beyond the
first occurrence of each fingerprint.

They are not automatically equivalent to confirmed
erroneous duplicates.

## 7. Cancellation monitoring

The monthly cancellation indicator is calculated as:

    cancellation_lines
    / (sale_lines + cancellation_lines) * 100

This is a line-based indicator, not an invoice-level
cancellation rate.

Observed examples:

| Period | Cancellation line rate |
|---|---:|
| 2009-12 | 2.26% |
| 2010-05 | 2.83% |
| 2010-11 | 1.54% |
| 2011-11 | 1.27% |

The metric can support operational monitoring,
but causal conclusions require additional analysis.

## 8. Analytical capabilities

The warehouse supports:

- Monthly financial monitoring
- Product and service-code performance
- Geographic concentration analysis
- Identified-customer performance
- Cancellation monitoring
- Data-quality reporting

The analytical layer consists of five reusable
PostgreSQL views and seven business queries.

## 9. Limitations

- The source data is historical.
- December 2011 is incomplete.
- The currency is not explicitly asserted.
- Some stock codes represent postage-related charges.
- Missing customer identifiers affect customer analysis.
- Return-line net amounts follow current transformation rules.
- Repeated fingerprints require contextual investigation.
- The warehouse is a full-snapshot implementation.
- Google Cloud deployment has not yet been completed.

## Conclusion

CommerceLake demonstrates an end-to-end analytical
workflow from raw source ingestion through PostgreSQL
business reporting.

The principal engineering strengths are:

- Preservation of source transaction records
- Explicit transaction classification
- Dimensional modeling
- Transactional warehouse publication
- Exact financial reconciliation
- Reproducible SQL validation
- Reusable analytical views

The warehouse provides a validated foundation
for future cloud deployment and BI integration.
