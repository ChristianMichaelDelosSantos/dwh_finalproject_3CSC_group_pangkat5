CREATE OR REPLACE VIEW view_revenue_by_product AS
SELECT
    p.product_name,
    p.product_type,
    SUM(f.revenue) AS total_revenue,
    SUM(f.quantity) AS total_quantity
FROM fact_order_items f
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY
    p.product_name,
    p.product_type;
