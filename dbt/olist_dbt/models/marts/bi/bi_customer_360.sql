{{ config(materialized='view') }}

SELECT
    customer_unique_id,
    first_order_date,
    last_order_date,
    total_orders,
    total_items,
    lifetime_value AS total_revenue,
    avg_order_value,
    CASE WHEN total_orders > 1 THEN 1 ELSE 0 END AS is_repeat_customer
FROM {{ ref('mart_customer_360') }}