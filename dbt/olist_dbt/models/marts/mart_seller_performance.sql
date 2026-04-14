WITH seller_perf AS (
    SELECT
        s.seller_key,
        s.seller_id,
        s.seller_city,
        s.seller_state,
        COUNT(DISTINCT f.order_id) AS order_count,
        COUNT(*) AS line_item_count,
        SUM(f.gross_item_amount) AS gross_revenue
    FROM {{ ref('fact_order_items') }} f
    LEFT JOIN {{ ref('dim_seller') }} s
        ON f.seller_key = s.seller_key
    GROUP BY
        s.seller_key,
        s.seller_id,
        s.seller_city,
        s.seller_state
),

seller_orders AS (
    SELECT DISTINCT
        seller_key,
        order_id
    FROM {{ ref('fact_order_items') }}
    WHERE order_id IS NOT NULL
),

review_per_order AS (
    SELECT
        order_id,
        ROUND(AVG(CAST(review_score AS DECIMAL(10,2))), 2) AS order_avg_review_score
    FROM {{ ref('dim_review') }}
    WHERE order_id IS NOT NULL
        AND review_score IS NOT NULL
    GROUP BY order_id
),

seller_review AS (
    SELECT
        so.seller_key,
        ROUND(AVG(rpo.order_avg_review_score), 2) AS avg_review_score
    FROM seller_orders so
    LEFT JOIN review_per_order rpo
        ON so.order_id = rpo.order_id
    GROUP BY so.seller_key
)

SELECT
    sp.seller_key,
    sp.seller_id,
    sp.seller_city,
    sp.seller_state,
    sp.order_count,
    sp.line_item_count,
    sp.gross_revenue,
    sr.avg_review_score
FROM seller_perf sp
LEFT JOIN seller_review sr
    ON sp.seller_key = sr.seller_key