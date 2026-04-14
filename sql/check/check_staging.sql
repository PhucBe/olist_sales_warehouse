-- =====================================================
-- CHECK STAGING LAYER
-- Default schemas used in this file:
-- raw_layer, olist_sales_staging
-- =====================================================


-- 1) Row counts: raw vs staging
SELECT 'raw_orders' AS table_name, COUNT(*) AS row_count FROM raw_layer.raw_orders
UNION ALL
SELECT 'stg_orders', COUNT(*) FROM olist_sales_staging.stg_orders
UNION ALL
SELECT 'raw_order_items', COUNT(*) FROM raw_layer.raw_order_items
UNION ALL
SELECT 'stg_order_items', COUNT(*) FROM olist_sales_staging.stg_order_items
UNION ALL
SELECT 'raw_customers', COUNT(*) FROM raw_layer.raw_customers
UNION ALL
SELECT 'stg_customers', COUNT(*) FROM olist_sales_staging.stg_customers
UNION ALL
SELECT 'raw_products', COUNT(*) FROM raw_layer.raw_products
UNION ALL
SELECT 'stg_products', COUNT(*) FROM olist_sales_staging.stg_products
UNION ALL
SELECT 'raw_sellers', COUNT(*) FROM raw_layer.raw_sellers
UNION ALL
SELECT 'stg_sellers', COUNT(*) FROM olist_sales_staging.stg_sellers
UNION ALL
SELECT 'raw_order_payments', COUNT(*) FROM raw_layer.raw_order_payments
UNION ALL
SELECT 'stg_payments', COUNT(*) FROM olist_sales_staging.stg_payments
UNION ALL
SELECT 'raw_order_reviews', COUNT(*) FROM raw_layer.raw_order_reviews
UNION ALL
SELECT 'stg_reviews', COUNT(*) FROM olist_sales_staging.stg_reviews;

-- 2) stg_orders: duplicate order_id
SELECT order_id, COUNT(*) AS cnt
FROM olist_sales_staging.stg_orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC, order_id
LIMIT 100;

-- 3) stg_orders: invalid timeline
SELECT *
FROM olist_sales_staging.stg_orders
WHERE order_delivered_customer_ts IS NOT NULL
    AND order_purchase_ts IS NOT NULL
    AND order_delivered_customer_ts < order_purchase_ts
LIMIT 100;

-- 4) stg_orders: invalid status values
SELECT order_status, COUNT(*) AS cnt
FROM olist_sales_staging.stg_orders
GROUP BY order_status
ORDER BY cnt DESC;

-- 5) stg_order_items: duplicate grain check
SELECT order_id, order_item_id, COUNT(*) AS cnt
FROM olist_sales_staging.stg_order_items
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC, order_id, order_item_id
LIMIT 100;

-- 6) stg_order_items: negative price/freight
SELECT *
FROM olist_sales_staging.stg_order_items
WHERE price < 0 OR freight_value < 0
LIMIT 100;

-- 7) stg_order_items -> stg_orders orphan check
SELECT COUNT(*) AS orphan_order_item_count
FROM olist_sales_staging.stg_order_items oi
LEFT JOIN olist_sales_staging.stg_orders o
    ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- 8) stg_customers: duplicate customer_id
SELECT customer_id, COUNT(*) AS cnt
FROM olist_sales_staging.stg_customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC, customer_id
LIMIT 100;

-- 9) stg_customers: blank customer_unique_id
SELECT *
FROM olist_sales_staging.stg_customers
WHERE customer_unique_id IS NULL OR TRIM(customer_unique_id) = ''
LIMIT 100;

-- 10) stg_products: duplicate product_id
SELECT product_id, COUNT(*) AS cnt
FROM olist_sales_staging.stg_products
GROUP BY product_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC, product_id
LIMIT 100;

-- 11) stg_products: invalid physical attributes
SELECT *
FROM olist_sales_staging.stg_products
WHERE (product_weight_g IS NOT NULL AND product_weight_g < 0)
    OR (product_length_cm IS NOT NULL AND product_length_cm < 0)
    OR (product_height_cm IS NOT NULL AND product_height_cm < 0)
    OR (product_width_cm IS NOT NULL AND product_width_cm < 0)
    OR (product_photos_qty IS NOT NULL AND product_photos_qty < 0)
LIMIT 100;

-- 12) stg_sellers: duplicate seller_id
SELECT seller_id, COUNT(*) AS cnt
FROM olist_sales_staging.stg_sellers
GROUP BY seller_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC, seller_id
LIMIT 100;

-- 13) stg_payments: duplicate logical grain
SELECT order_id, payment_sequential, COUNT(*) AS cnt
FROM olist_sales_staging.stg_payments
GROUP BY order_id, payment_sequential
HAVING COUNT(*) > 1
ORDER BY cnt DESC, order_id, payment_sequential
LIMIT 100;

-- 14) stg_payments: invalid values
SELECT *
FROM olist_sales_staging.stg_payments
WHERE (payment_value IS NOT NULL AND payment_value < 0)
    OR (payment_installments IS NOT NULL AND payment_installments < 0)
LIMIT 100;

-- 15) stg_reviews: invalid review score when present
SELECT *
FROM olist_sales_staging.stg_reviews
WHERE review_score IS NOT NULL
    AND review_score NOT IN (1,2,3,4,5)
LIMIT 100;