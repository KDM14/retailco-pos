{{ config(
    materialized='ephemeral'
) }}

WITH staged AS (
    SELECT * FROM {{ ref('stg_retailco_transactions') }}
),

metrics_calculated AS (
    SELECT
        transaction_id,
        store_id,
        store_name,
        parsed_date,
        customer_id,
        product_code,
        product_name,
        category,
        quantity,
        unit_price_inr,
        discount_pct,
        -- Financial line-item metrics
        ROUND(quantity * unit_price_inr, 2) AS gross_amount_inr,
        ROUND((quantity * unit_price_inr) * (discount_pct / 100.0), 2) AS discount_amount_inr,
        ROUND((quantity * unit_price_inr) - ((quantity * unit_price_inr) * (discount_pct / 100.0)), 2) AS net_amount_inr,
        total_amount_inr,
        payment_method,
        cashier_id,
        region,
        city,
        CASE WHEN discount_pct > 0 THEN TRUE ELSE FALSE END AS is_discounted
    FROM staged
)

SELECT * FROM metrics_calculated
