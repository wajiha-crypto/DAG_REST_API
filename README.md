# DAG_REST_API — Airflow ELT Pipeline

Automates ingestion of product data from the DummyJSON REST API into PostgreSQL using Apache Airflow, replacing manual SQL execution with a scheduled, orchestrated pipeline.

## Files

- `docker-compose.yml` — local Airflow (webserver, scheduler) + Postgres metadata DB, run via Docker
- `dags/product_ingestion_dag.py` — the DAG: fetches products from the DummyJSON API and upserts them, then transforms into the warehouse layer
- `sql/transform_products.sql` — SQL transformation from `staging.api_products` into `warehouse.dim_api_products`

## Pipeline

```
DummyJSON API
      │
      ▼
staging.api_products        (raw ingested data, upserted on product_id)
      │
      ▼
warehouse.dim_api_products  (warehouse-ready dimension table)
```

## How to run

1. Make sure Docker Desktop is running.
2. In this folder, run:
   ```
   docker compose up -d
   ```
3. Open the Airflow UI at [http://localhost:8080](http://localhost:8080) (login: `admin` / `admin`).
4. Go to **Admin → Connections**, edit `postgres_default`:
   - Host: `host.docker.internal`
   - Database: `postgres`
   - Login / Password: your local PostgreSQL credentials
   - Port: `5432`
5. Unpause and trigger the `products_api_ingestion` DAG.

## Verify

```sql
SELECT COUNT(*) FROM staging.api_products;
SELECT COUNT(*) FROM warehouse.dim_api_products;
```

## Notes

- Tested with Apache Airflow 2.9.3 and Docker Desktop (WSL2 backend) on Windows.
- The `sql/transform_products.sql` file is loaded via Airflow's `template_searchpath`, so it must stay in a `sql/` folder alongside the `dags/` folder.
- `staging.api_products` and `warehouse.dim_api_products` are upserted on `product_id`, so re-running the DAG updates existing rows instead of duplicating them.
