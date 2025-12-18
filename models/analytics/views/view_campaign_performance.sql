CREATE OR REPLACE VIEW view_campaign_performance AS
SELECT
    c.campaign_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(i.revenue) AS total_revenue
FROM fact_orders o
LEFT JOIN dim_campaign c
    ON o.campaign_key = c.campaign_key
LEFT JOIN fact_order_items i
    ON o.order_id = i.order_id
GROUP BY
    c.campaign_name;
