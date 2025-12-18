-- Drop and recreate for full-refresh (SCD Type 1)
DROP TABLE IF EXISTS dim_product;

CREATE TABLE dim_product AS
SELECT
    ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,  -- surrogate key
    product_id,                                              -- business key
    product_name,
    product_type,
    price
FROM stage_products;
