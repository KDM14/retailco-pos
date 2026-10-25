{{ config(
    materialized='table'
) }}

WITH products_deduped AS (
    SELECT 
        product_code,
        product_name,
        category,
        unit_price_inr,
        parsed_date,
        ROW_NUMBER() OVER (PARTITION BY product_code ORDER BY parsed_date DESC) AS rn
    FROM {{ ref('int_transaction_metrics') }}
    WHERE product_code IS NOT NULL
)

SELECT
    product_code,
    product_name,
    category,
    unit_price_inr AS current_price_inr,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM products_deduped
WHERE rn = 1
