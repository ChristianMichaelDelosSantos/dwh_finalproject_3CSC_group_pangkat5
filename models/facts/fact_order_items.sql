-- Drop and recreate for full-refresh
DROP TABLE IF EXISTS fact_order_items;

CREATE TABLE fact_order_items AS
SELECT
    oi.order_id,                         -- degenerate dimension

    p.product_key,
    c.customer_key,
    m.merchant_key,

    d.date_key AS order_date_key,

    CAST(oi.quantity AS INTEGER) AS quantity,
    oi.price AS unit_price,
    CAST(oi.quantity AS INTEGER) * oi.price AS revenue

FROM stage_order_items oi

JOIN dim_product p
    ON oi.product_id = p.product_id

JOIN stage_orders o
    ON oi.order_id = o.order_id

JOIN dim_customer c
    ON o.user_id = c.user_id

JOIN dim_merchant m
    ON o.merchant_id = m.merchant_id

JOIN dim_date d
    ON CAST(o.transaction_date AS DATE) = d.full_date;