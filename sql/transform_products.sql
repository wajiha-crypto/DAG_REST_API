CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_api_products (
    product_id      INT PRIMARY KEY,
    title           VARCHAR(200),
    category        VARCHAR(100),
    price           NUMERIC(10,2),
    stock           INT,
    brand           VARCHAR(100),
    rating          NUMERIC(3,2)
);

INSERT INTO warehouse.dim_api_products
    (product_id, title, category, price, stock, brand, rating)
SELECT product_id, title, category, price, stock, brand, rating
FROM staging.api_products
ON CONFLICT (product_id) DO UPDATE SET
    title    = EXCLUDED.title,
    category = EXCLUDED.category,
    price    = EXCLUDED.price,
    stock    = EXCLUDED.stock,
    brand    = EXCLUDED.brand,
    rating   = EXCLUDED.rating;
