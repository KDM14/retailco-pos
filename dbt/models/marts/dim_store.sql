{{ config(
    materialized='table'
) }}

WITH stores_deduped AS (
    SELECT 
        store_id,
        store_name,
        region,
        city,
        parsed_date,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY parsed_date DESC) AS rn
    FROM {{ ref('int_transaction_metrics') }}
    WHERE store_id IS NOT NULL
)

SELECT
    store_id,
    store_name,
    region,
    city,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM stores_deduped
WHERE rn = 1
