CREATE OR REPLACE VIEW view_customer_lifetime_value AS
SELECT
    c.user_id,
    c.user_type,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(i.revenue) AS lifetime_revenue
FROM dim_customer c
JOIN fact_orders o
    ON c.customer_key = o.customer_key
JOIN fact_order_items i
    ON o.order_id = i.order_id
GROUP BY
    c.user_id,
    c.user_type;
