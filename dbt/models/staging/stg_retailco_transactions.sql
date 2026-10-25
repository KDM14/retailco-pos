{{ config(
    materialized='view'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('pos_raw', 'pos_transactions_landing') }}
),

cleaned AS (
    SELECT
        TRIM(transaction_id) AS transaction_id,
        TRIM(UPPER(store_id)) AS store_id,
        TRIM(store_name) AS store_name,
        COALESCE(
            SAFE.PARSE_DATE('%Y-%m-%d', transaction_date),
            SAFE.PARSE_DATE('%d-%m-%Y', transaction_date)
        ) AS parsed_date,
        TRIM(customer_id) AS customer_id,
        TRIM(UPPER(product_code)) AS product_code,
        TRIM(product_name) AS product_name,
        COALESCE(TRIM(category), 'General') AS category,
        CAST(quantity AS INT64) AS quantity,
        CAST(unit_price_inr AS NUMERIC) AS unit_price_inr,
        CAST(COALESCE(discount_pct, 0.0) AS NUMERIC) AS discount_pct,
        CAST(total_amount_inr AS NUMERIC) AS total_amount_inr,
        TRIM(payment_method) AS payment_method,
        TRIM(cashier_id) AS cashier_id,
        TRIM(region) AS region,
        TRIM(city) AS city
    FROM source_data
    WHERE transaction_id IS NOT NULL
      AND TRIM(transaction_id) != ''
)

SELECT * FROM cleaned
