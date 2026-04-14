SELECT
    c.customer_unique_id,
    MIN(f.order_date) AS first_order_date,
    MAX(f.order_date) AS last_order_date,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(*) AS total_items,
    SUM(f.gross_item_amount) AS lifetime_value,
    ROUND(SUM(f.gross_item_amount) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS avg_order_value
FROM {{ ref('fact_order_items') }} f
LEFT JOIN {{ ref('dim_customer') }} c
    ON f.customer_key = c.customer_key
WHERE c.customer_unique_id IS NOT NULL
GROUP BY c.customer_unique_id