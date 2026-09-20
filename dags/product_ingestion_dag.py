from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests

DUMMYJSON_URL = "https://dummyjson.com/products?limit=100"


def fetch_and_upsert_products():
    """Fetch products from the DummyJSON API and upsert them into staging.api_products."""
    response = requests.get(DUMMYJSON_URL, timeout=30)
    response.raise_for_status()
    products = response.json()["products"]

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS staging.api_products (
            product_id      INT PRIMARY KEY,
            title           VARCHAR(200),
            category        VARCHAR(100),
            price           NUMERIC(10,2),
            stock           INT,
            brand           VARCHAR(100),
            rating          NUMERIC(3,2)
        );
    """)

    for p in products:
        cursor.execute("""
            INSERT INTO staging.api_products
                (product_id, title, category, price, stock, brand, rating)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE SET
                title    = EXCLUDED.title,
                category = EXCLUDED.category,
                price    = EXCLUDED.price,
                stock    = EXCLUDED.stock,
                brand    = EXCLUDED.brand,
                rating   = EXCLUDED.rating;
        """, (
            p["id"], p["title"], p.get("category"), p.get("price"),
            p.get("stock"), p.get("brand"), p.get("rating"),
        ))

    conn.commit()
    cursor.close()
    conn.close()


default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="products_api_ingestion",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    template_searchpath=["/opt/airflow"],
) as dag:

    fetch_and_upsert = PythonOperator(
        task_id="fetch_and_upsert_products",
        python_callable=fetch_and_upsert_products,
    )

    transform_to_warehouse = PostgresOperator(
        task_id="transform_products_to_warehouse",
        postgres_conn_id="postgres_default",
        sql="sql/transform_products.sql",
    )

    fetch_and_upsert >> transform_to_warehouse
