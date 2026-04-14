-- =====================================================
-- CHECK CORE LAYER
-- Default schema used in this file: olist_sales_core
-- =====================================================


-- 1) Row counts
SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count FROM olist_sales_core.dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM olist_sales_core.dim_product
UNION ALL
SELECT 'dim_seller', COUNT(*) FROM olist_sales_core.dim_seller
UNION ALL
SELECT 'dim_date', COUNT(*) FROM olist_sales_core.dim_date
UNION ALL
SELECT 'dim_payment', COUNT(*) FROM olist_sales_core.dim_payment
UNION ALL
SELECT 'dim_review', COUNT(*) FROM olist_sales_core.dim_review
UNION ALL
SELECT 'fact_order_items', COUNT(*) FROM olist_sales_core.fact_order_items;

-- 2) dim_customer uniqueness
SELECT customer_id, COUNT(*) AS cnt
FROM olist_sales_core.dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC, customer_id
LIMIT 100;

-- 3) dim_product uniqueness
SELECT product_id, COUNT(*) AS cnt
FROM olist_sales_core.dim_product
GROUP BY product_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC, product_id
LIMIT 100;

-- 4) dim_seller uniqueness
SELECT seller_id, COUNT(*) AS cnt
FROM olist_sales_core.dim_seller
GROUP BY seller_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC, seller_id
LIMIT 100;

-- 5) dim_date key check
SELECT *
FROM olist_sales_core.dim_date
WHERE date_day IS NOT NULL
    AND date_key <> CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INTEGER)
LIMIT 100;

-- 6) dim_payment logical grain check
SELECT order_id, payment_sequential, COUNT(*) AS cnt
FROM olist_sales_core.dim_payment
GROUP BY order_id, payment_sequential
HAVING COUNT(*) > 1
ORDER BY cnt DESC, order_id, payment_sequential
LIMIT 100;

-- 7) dim_payment invalid values
SELECT *
FROM olist_sales_core.dim_payment
WHERE (payment_value IS NOT NULL AND payment_value < 0)
    OR (payment_installments IS NOT NULL AND payment_installments < 0)
LIMIT 100;

-- 8) dim_review logical grain check
SELECT review_id, order_id, COUNT(*) AS cnt
FROM olist_sales_core.dim_review
GROUP BY review_id, order_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC, review_id, order_id
LIMIT 100;

-- 9) dim_review invalid score when present
SELECT *
FROM olist_sales_core.dim_review
WHERE review_score IS NOT NULL
    AND review_score NOT IN (1,2,3,4,5)
LIMIT 100;

-- 10) fact_order_items uniqueness
SELECT order_id, order_item_id, COUNT(*) AS cnt
FROM olist_sales_core.fact_order_items
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC, order_id, order_item_id
LIMIT 100;

-- 11) fact_order_items measures check
SELECT *
FROM olist_sales_core.fact_order_items
WHERE COALESCE(price, 0) < 0
    OR COALESCE(freight_value, 0) < 0
    OR COALESCE(gross_item_amount, 0) < 0
LIMIT 100;

-- 12) fact_order_items date key check
SELECT *
FROM olist_sales_core.fact_order_items
WHERE order_date IS NOT NULL
    AND order_date_key <> CAST(TO_CHAR(order_date, 'YYYYMMDD') AS INTEGER)
LIMIT 100;

-- 13) fact -> dims orphan checks
SELECT 'customer_orphans' AS check_name, COUNT(*) AS orphan_count
FROM olist_sales_core.fact_order_items f
LEFT JOIN olist_sales_core.dim_customer d ON f.customer_key = d.customer_key
WHERE d.customer_key IS NULL
UNION ALL
SELECT 'product_orphans', COUNT(*)
FROM olist_sales_core.fact_order_items f
LEFT JOIN olist_sales_core.dim_product d ON f.product_key = d.product_key
WHERE d.product_key IS NULL
UNION ALL
SELECT 'seller_orphans', COUNT(*)
FROM olist_sales_core.fact_order_items f
LEFT JOIN olist_sales_core.dim_seller d ON f.seller_key = d.seller_key
WHERE d.seller_key IS NULL
UNION ALL
SELECT 'date_orphans', COUNT(*)
FROM olist_sales_core.fact_order_items f
LEFT JOIN olist_sales_core.dim_date d ON f.order_date_key = d.date_key
WHERE d.date_key IS NULL;