{{ config(
    materialized='table'
) }}

-- Generates 5-year date dimension spine
WITH date_series AS (
    SELECT day
    FROM UNNEST(GENERATE_DATE_ARRAY('2023-01-01', '2028-12-31', INTERVAL 1 DAY)) AS day
)

SELECT
    day AS date_day,
    EXTRACT(YEAR FROM day) AS year_actual,
    EXTRACT(QUARTER FROM day) AS quarter_actual,
    EXTRACT(MONTH FROM day) AS month_actual,
    FORMAT_DATE('%B', day) AS month_name,
    EXTRACT(DAY FROM day) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM day) AS day_of_week,
    FORMAT_DATE('%A', day) AS day_name,
    CASE WHEN EXTRACT(DAYOFWEEK FROM day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN EXTRACT(MONTH FROM day) >= 4 
         THEN EXTRACT(YEAR FROM day) 
         ELSE EXTRACT(YEAR FROM day) - 1 
    END AS fiscal_year_inr
FROM date_series
