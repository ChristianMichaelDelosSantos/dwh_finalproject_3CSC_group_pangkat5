-- Drop and recreate for full-refresh (SCD Type 1)
DROP TABLE IF EXISTS dim_campaign;

CREATE TABLE dim_campaign AS
SELECT
    ROW_NUMBER() OVER (ORDER BY campaign_id) AS campaign_key, -- surrogate key
    campaign_id,                                              -- business key
    campaign_name,
    campaign_description,
    discount
FROM (
    SELECT DISTINCT
        campaign_id,
        campaign_name,
        campaign_description,
        discount
    FROM stage_campaigns
    WHERE campaign_id IS NOT NULL
) c;
