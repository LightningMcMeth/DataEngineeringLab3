## Business domain

The business domain is an online store that sells products across multiple categories such as electronics, home goods, fashion, and beauty.

## Core entities

- customers
- categories
- products
- orders
- order items
- payments
- shipments
- reviews
- returns

## How to run

1. Create and activate a virtual environment.
2. Install `dbt-duckdb`.
3. Run the project with the local profile directory.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install dbt-duckdb
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```