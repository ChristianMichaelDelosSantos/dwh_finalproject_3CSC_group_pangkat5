-- Drop and recreate for full-refresh
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date AS
SELECT
    CAST(strftime('%Y%m%d', d) AS INTEGER) AS date_key,  -- surrogate date key
    d AS full_date,
    EXTRACT(YEAR FROM d) AS year,
    EXTRACT(MONTH FROM d) AS month,
    EXTRACT(DAY FROM d) AS day,
    EXTRACT(QUARTER FROM d) AS quarter,
    strftime('%A', d) AS weekday_name,
    EXTRACT(DOW FROM d) AS weekday_number,
    CASE
        WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM (
    SELECT generate_series(
        DATE '2000-01-01',
        DATE '2025-12-31',
        INTERVAL '1 day'
    ) AS d
);
