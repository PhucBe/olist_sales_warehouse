SELECT
    p.product_key,
    p.product_id,
    p.product_category_name_en,
    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(*) AS line_item_count,
    SUM(f.price) AS product_revenue,
    SUM(f.freight_value) AS freight_revenue,
    SUM(f.gross_item_amount) AS gross_revenue
FROM {{ ref('fact_order_items') }} f
LEFT JOIN {{ ref('dim_product') }} p
    ON f.product_key = p.product_key
GROUP BY
    p.product_key,
    p.product_id,
    p.product_category_name_en