{{ config(
    materialized='table'
) }}

WITH customers_deduped AS (
    SELECT 
        customer_id,
        parsed_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY parsed_date DESC) AS rn
    FROM {{ ref('int_transaction_metrics') }}
    WHERE customer_id IS NOT NULL
)

SELECT
    customer_id,
    -- Deterministic pseudonymization hash for analytics without PII leakage
    TO_HEX(SHA256(customer_id)) AS masked_customer_token,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM customers_deduped
WHERE rn = 1
