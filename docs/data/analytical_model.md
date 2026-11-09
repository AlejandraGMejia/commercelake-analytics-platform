# Analytical Data Model

## Fact table

fct_sales

Grain:
One validated source invoice line.

Candidate measures:
- quantity
- unit_price
- line_amount

Candidate foreign keys:
- customer_key
- product_key
- date_key
- country_key

## Dimensions

dim_customer
- customer_key
- customer_id
- country

dim_product
- product_key
- stock_code
- description

dim_date
- date_key
- full_date
- year
- quarter
- month

dim_country
- country_key
- country_name

## Design decisions pending

- Invoice line identifier
- Unknown customer handling
- Return and cancellation treatment
- Product description history
- Currency assumptions
- Duplicate invoice-line handling
- Customer-country relationship
- Exact sales metric definitions

## Modeling strategy

Use dbt staging, intermediate and mart layers.

Final dimensions and fact tables will be
implemented only after source profiling.
