{{ config(materialized='view') }}

SELECT
    seller_id,
    seller_city,
    seller_state,
    order_count AS total_orders,
    line_item_count AS total_items,
    gross_revenue AS total_revenue,
    avg_review_score
FROM {{ ref('mart_seller_performance') }}