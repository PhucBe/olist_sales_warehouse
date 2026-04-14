SELECT
    f.order_date_key,
    f.order_date,
    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(*) AS line_item_count,
    SUM(f.price) AS product_revenue,
    SUM(f.freight_value) AS freight_revenue,
    SUM(f.gross_item_amount) AS gross_revenue,
    ROUND(SUM(f.gross_item_amount) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS avg_order_value
FROM {{ ref('fact_order_items') }} f
GROUP BY
    f.order_date_key,
    f.order_date