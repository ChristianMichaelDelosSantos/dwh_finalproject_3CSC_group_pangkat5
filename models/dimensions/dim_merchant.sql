-- Drop and recreate for full-refresh (SCD Type 1)
DROP TABLE IF EXISTS dim_merchant;

CREATE TABLE dim_merchant AS
SELECT
    ROW_NUMBER() OVER (ORDER BY merchant_id) AS merchant_key, -- surrogate key
    merchant_id,                                              -- business key
    name,
    city,
    state,
    country,
    creation_date,
    contact_number
FROM stage_merchants;
