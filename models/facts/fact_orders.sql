-- Drop and recreate for full-refresh
DROP TABLE IF EXISTS fact_orders;

CREATE TABLE fact_orders AS
SELECT
    o.order_id,                               -- degenerate dimension

    c.customer_key,
    m.merchant_key,
    ca.campaign_key,

    d.date_key AS order_date_key,

    1 AS order_count,

    -- delivery delay in days (optional metric)
    DATE_DIFF('day',
        CAST(o.transaction_date AS DATE),
        CAST(o.estimated_arrival AS DATE)
    ) AS delivery_days

FROM stage_orders o

JOIN dim_customer c
    ON o.user_id = c.user_id

JOIN dim_merchant m
    ON o.merchant_id = m.merchant_id

LEFT JOIN dim_campaign ca
    ON o.campaign_id = ca.campaign_id

JOIN dim_date d
    ON CAST(o.transaction_date AS DATE) = d.full_date;
