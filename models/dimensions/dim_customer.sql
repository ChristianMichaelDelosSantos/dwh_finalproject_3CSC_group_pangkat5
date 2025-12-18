-- Drop and recreate for full-refresh (SCD Type 1)
DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer AS
SELECT
    ROW_NUMBER() OVER (ORDER BY user_id) AS customer_key,   -- surrogate key
    user_id,                                                 -- business key
    name,
    gender,
    birthdate,
    city,
    state,
    country,
    user_type,
    job_title,
    job_level,
    creation_date
FROM stage_customers;
