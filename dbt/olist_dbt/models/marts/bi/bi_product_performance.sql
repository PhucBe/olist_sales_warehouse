{{ config(materialized='view') }}

SELECT
    product_id,
    product_category_name_en AS product_category_name,
    order_count,
    line_item_count AS total_quantity,
    product_revenue,
    freight_revenue,
    gross_revenue AS total_revenue
FROM {{ ref('mart_product_performance') }}