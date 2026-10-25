{{ config(
    materialized='incremental',
    unique_key='sales_key',
    partition_by={
      'field': 'parsed_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by=['store_id', 'product_code']
) }}

WITH source_metrics AS (
    SELECT *
    FROM {{ ref('int_transaction_metrics') }}
    {% if is_incremental() %}
    -- Idempotent 7-day lookback watermark handling late-arriving batch partitions
    WHERE parsed_date >= DATE_SUB(_dbt_max_partition, INTERVAL 7 DAY)
    {% endif %}
),

deduped_line_items AS (
    SELECT
        -- Surrogate Key: Unique hash per physical line item
        TO_HEX(MD5(CONCAT(transaction_id, '-', product_code))) AS sales_key,
        transaction_id,
        parsed_date,
        store_id,
        customer_id,
        product_code,
        quantity,
        unit_price_inr,
        discount_pct,
        gross_amount_inr,
        discount_amount_inr,
        net_amount_inr,
        total_amount_inr,
        payment_method,
        cashier_id,
        is_discounted,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id, product_code 
            ORDER BY parsed_date DESC
        ) AS rn
    FROM source_metrics
)

SELECT
    sales_key,
    transaction_id,
    parsed_date,
    store_id,
    customer_id,
    product_code,
    quantity,
    unit_price_inr,
    discount_pct,
    gross_amount_inr,
    discount_amount_inr,
    net_amount_inr,
    total_amount_inr,
    payment_method,
    cashier_id,
    is_discounted,
    CURRENT_TIMESTAMP() AS loaded_at
FROM deduped_line_items
WHERE rn = 1
