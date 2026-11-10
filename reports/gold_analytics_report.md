# Gold Analytics — Sprint 05

## Executive summary

CommerceLake transforms the validated Silver dataset into a versioned Gold analytical layer.

- Gold run: `591c8097f0554414be18648e3587d2b3`
- Source Silver run: `f581b1e5aacb4cb3b16f72fa575dd9ed`
- Fact rows: 1,067,371
- Monthly periods: 25
- Net transaction amount: 19,445,926.7080
- Currency: not explicitly established by the analytical contract

## Dimensional model

| Table | Rows |
|---|---:|
| `dim_country` | 44 |
| `dim_customer` | 5,943 |
| `dim_date` | 739 |
| `dim_product` | 5,305 |

### Grain and business rules

- `fact_sales`: one row per Silver transaction line.
- All source rows are preserved.
- Product, customer, country and date dimensions support analytical joins.
- Unknown dimension members preserve referential integrity where applicable.
- Sales, cancellations and returns contribute signed financial amounts.
- Review transactions are excluded from net financial totals.
- Monthly customer counts exclude missing customer identifiers.
- Counts refer to transaction lines, not distinct invoices.

## Transaction classification

| Type | Lines |
|---|---:|
| sale | 1,041,670 |
| cancellation | 19,494 |
| return | 3,457 |
| review | 2,750 |

## Annual performance

| Year | Transaction lines | Net amount |
|---|---:|---:|
| 2009 | 45,228 | 799,847.1100 |
| 2010 | 522,714 | 9,625,146.5640 |
| 2011 | 499,429 | 9,020,933.0340 |

Annual figures should not be compared without considering coverage: 2009 contains only December, and the final month of 2011 is partially observed.

## Highest-net months

| Month | Net amount | Sale lines |
|---|---:|---:|
| 2011-11 | 1,461,756.2500 | 83,369 |
| 2010-11 | 1,422,654.6420 | 76,464 |
| 2010-12 | 1,126,445.4700 | 63,496 |
| 2010-10 | 1,084,094.2200 | 57,797 |
| 2011-10 | 1,070,704.6700 | 59,304 |

## Lowest-net months

| Month | Net amount | Sale lines |
|---|---:|---:|
| 2011-12 | 433,686.0100 | 25,112 |
| 2011-04 | 493,207.1210 | 29,096 |
| 2011-02 | 498,062.6500 | 27,105 |
| 2010-02 | 533,091.4260 | 28,281 |
| 2011-01 | 560,000.2600 | 34,306 |

## Analytical interpretation

- November 2010 and November 2011 are among the strongest observed months.
- This pattern warrants further seasonality analysis rather than establishing seasonality conclusively.
- December 2011 has partial coverage and must not be treated as a complete monthly performance observation.
- Net amounts are signed transaction measures, not verified accounting profit.

## Data quality and reconciliation

- Silver-to-Gold fact rows: 1,067,371
- Mart source rows: 1,067,371
- Fact net amount: 19,445,926.7080
- Mart net amount: 19,445,926.7080
- Row reconciliation: PASSED
- Financial reconciliation: PASSED
- Transaction classification: PASSED

## Versioning and reproducibility

- Each Gold build receives a unique run ID.
- The manifest inventories dimensions, fact partitions and analytical marts.
- The active version is referenced through `current.json`.
- The pointer is updated only after the build and validations succeed.
- Historical completed runs remain available for reproducibility.

## Next engineering steps

- Add SQL analytical queries and warehouse consumption.
- Introduce orchestration and incremental processing.
- Extend automated observability and operational monitoring.
