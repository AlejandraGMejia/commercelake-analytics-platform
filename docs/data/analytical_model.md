# CommerceLake — Gold Analytical Model

## Purpose and grain

The Gold model organizes validated Silver retail transaction lines for dimensional SQL analysis. **`fact_sales` has one row per validated Silver source line**, not one row per invoice or customer. The pipeline does not automatically deduplicate the source transaction population.

## Model inventory

| Table | Role | Rows |
|---|---|---:|
| `fact_sales` | Transaction fact | 1,067,371 |
| `dim_product` | Product dimension | 5,305 |
| `dim_customer` | Customer dimension | 5,943 |
| `dim_country` | Country dimension | 44 |
| `dim_date` | Calendar dimension | 739 |
| `mart_monthly_sales` | Monthly analytical mart | 25 |

## Fact: `fact_sales`

The fact retains transaction classifications, source lineage, dimensional relationships, and signed financial measures. It represents sales, cancellations, returns, and review records without dropping review lines from the analytical population.

### Classification totals

| Type | Rows |
|---|---:|
| Sale | 1,041,670 |
| Cancellation | 19,494 |
| Return | 3,457 |
| Review | 2,750 |
| **Total** | **1,067,371** |

Review records remain available for investigation but do not contribute to the published net financial total. Sales, cancellations, and returns follow the implemented signed-measure rules. Under these rules, return lines have a zero aggregate net amount.

### Financial meaning

The reconciled Gold fact net transaction amount is **`19,445,926.7080`**. This is an analytical signed transaction measure in **unverified source monetary units**, not audited revenue, recognized income, or profit.

## Dimensions

### `dim_product`

Represents distinct product/stock codes, including an unknown member where applicable. Source descriptions can differ for a given code; a deterministic canonical-description rule supports consistent dimension attributes. Product codes can also identify postage or services, not just merchandise.

### `dim_customer`

Represents known customer identifiers and an unknown member. **243,007 source transaction lines have missing customer IDs**, so customer-level findings must acknowledge incomplete attribution.

### `dim_country`

Represents transaction country. Country is attributed at the transaction level rather than assumed to be a permanent customer characteristic.

### `dim_date`

Provides calendar attributes for transaction-date analysis. The date dimension has **739** members in the published model.

## Mart: `mart_monthly_sales`

Contains **25** monthly reporting periods across December 2009–December 2011. Its aggregate net transaction amount reconciles exactly with the Gold fact total (`19,445,926.7080`). **December 2011 is incomplete** and must be labeled as a partial month in comparisons.

## Data quality and interpretability

| Observation | Source lines |
|---|---:|
| Missing customer ID | 243,007 |
| Exact within-worksheet duplicates | 12,133 |
| Nonpositive prices | 6,207 |
| Missing descriptions | 4,382 |

These groups overlap. A duplicate flag is not sufficient justification to delete a record. Invoice lines are not the same thing as distinct invoices; missing customer IDs can bias customer rankings; and postage/service codes may affect product rankings.

## Validation and reconciliation

Local Gold validation establishes fact-row preservation, dimensional integrity checks, and financial agreement between the fact and monthly mart. The initial BigQuery deployment confirmed matching row counts across six tables and a numerically equivalent fact net amount using `BIGNUMERIC` financial columns. **Independent full cross-system row-level equality is not claimed.**

## References

- [Gold analytical contract](../gold_analytical_contract.md)
- [Gold analytics report](../../reports/gold_analytics_report.md)
- [Business findings](../../reports/sprint_06_business_findings.md)
- [BigQuery validation](../../reports/sprint_07_bigquery_validation.md)
