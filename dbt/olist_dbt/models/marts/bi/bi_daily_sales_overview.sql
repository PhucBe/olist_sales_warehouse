{{ config(materialized='view') }}

SELECT
    order_date,
    CAST(EXTRACT(year FROM order_date) AS VARCHAR(4)) AS order_year_text,
    lpad(CAST(EXTRACT(month FROM order_date) AS VARCHAR(2)), 2, '0') AS order_month_text,
    EXTRACT(year FROM order_date) AS order_year,
    EXTRACT(month FROM order_date) AS order_month,
    order_count,
    line_item_count,
    product_revenue,
    freight_revenue,
    gross_revenue AS total_revenue,
    avg_order_value
FROM {{ ref('mart_daily_sales') }}