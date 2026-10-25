{{ config(
    materialized='table'
) }}

WITH sales AS (
    SELECT * FROM {{ ref('fct_sales') }}
),

products AS (
    SELECT * FROM {{ ref('dim_product') }}
)

SELECT
    COALESCE(p.category, 'Uncategorized') AS category,
    s.parsed_date AS sales_date,
    COUNT(DISTINCT s.product_code) AS unique_skus_sold,
    SUM(s.quantity) AS total_units_sold,
    ROUND(SUM(s.gross_amount_inr), 2) AS category_gross_sales_inr,
    ROUND(SUM(s.net_amount_inr), 2) AS category_net_sales_inr,
    ROUND(SUM(s.discount_amount_inr), 2) AS total_promotional_discount_inr,
    ROUND(
        SAFE_DIVIDE(SUM(s.discount_amount_inr), SUM(s.gross_amount_inr)) * 100, 
        2
    ) AS discount_leakage_percentage,
    CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM sales s
LEFT JOIN products p ON s.product_code = p.product_code
GROUP BY 1, 2
