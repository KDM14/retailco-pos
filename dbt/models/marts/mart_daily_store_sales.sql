{{ config(
    materialized='table'
) }}

WITH sales AS (
    SELECT * FROM {{ ref('fct_sales') }}
),

stores AS (
    SELECT * FROM {{ ref('dim_store') }}
)

SELECT
    s.store_id,
    COALESCE(st.store_name, 'Unknown Store') AS store_name,
    COALESCE(st.region, 'Unknown Region') AS region,
    COALESCE(st.city, 'Unknown City') AS city,
    s.parsed_date AS sales_date,
    COUNT(DISTINCT s.transaction_id) AS total_transactions,
    SUM(s.quantity) AS total_units_sold,
    ROUND(SUM(s.gross_amount_inr), 2) AS total_gross_sales_inr,
    ROUND(SUM(s.discount_amount_inr), 2) AS total_discount_given_inr,
    ROUND(SUM(s.net_amount_inr), 2) AS total_net_revenue_inr,
    ROUND(SAFE_DIVIDE(SUM(s.net_amount_inr), COUNT(DISTINCT s.transaction_id)), 2) AS average_basket_size_inr,
    CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM sales s
LEFT JOIN stores st ON s.store_id = st.store_id
GROUP BY 1, 2, 3, 4, 5
