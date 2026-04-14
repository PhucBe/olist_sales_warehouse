-- =====================================================
-- CHECK MARTS LAYER
-- Default schema used in this file: olist_sales_marts
-- =====================================================


-- 1) Row counts
SELECT 'mart_daily_sales' AS table_name, COUNT(*) AS row_count FROM olist_sales_marts.mart_daily_sales
UNION ALL
SELECT 'mart_product_performance', COUNT(*) FROM olist_sales_marts.mart_product_performance
UNION ALL
SELECT 'mart_customer_360', COUNT(*) FROM olist_sales_marts.mart_customer_360
UNION ALL
SELECT 'mart_seller_performance', COUNT(*) FROM olist_sales_marts.mart_seller_performance;

-- 2) mart_daily_sales uniqueness + non-negative metrics
SELECT order_date_key, COUNT(*) AS cnt
FROM olist_sales_marts.mart_daily_sales
GROUP BY order_date_key
HAVING COUNT(*) > 1
ORDER BY cnt DESC, order_date_key
LIMIT 100;

SELECT *
FROM olist_sales_marts.mart_daily_sales
WHERE COALESCE(order_count, 0) < 0
    OR COALESCE(line_item_count, 0) < 0
    OR COALESCE(product_revenue, 0) < 0
    OR COALESCE(freight_revenue, 0) < 0
    OR COALESCE(gross_revenue, 0) < 0
    OR COALESCE(avg_order_value, 0) < 0
LIMIT 100;

-- 3) mart_product_performance uniqueness + non-negative metrics
SELECT product_key, COUNT(*) AS cnt
FROM olist_sales_marts.mart_product_performance
GROUP BY product_key
HAVING COUNT(*) > 1
ORDER BY cnt DESC, product_key
LIMIT 100;

SELECT *
FROM olist_sales_marts.mart_product_performance
WHERE COALESCE(order_count, 0) < 0
   OR COALESCE(line_item_count, 0) < 0
   OR COALESCE(product_revenue, 0) < 0
   OR COALESCE(freight_revenue, 0) < 0
   OR COALESCE(gross_revenue, 0) < 0
LIMIT 100;

-- 4) mart_customer_360 uniqueness + business checks
SELECT customer_unique_id, COUNT(*) AS cnt
FROM olist_sales_marts.mart_customer_360
GROUP BY customer_unique_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC, customer_unique_id
LIMIT 100;

SELECT *
FROM olist_sales_marts.mart_customer_360
WHERE first_order_date IS NULL
    OR last_order_date IS NULL
    OR last_order_date < first_order_date
    OR COALESCE(total_orders, 0) < 0
    OR COALESCE(total_items, 0) < 0
    OR COALESCE(lifetime_value, 0) < 0
    OR COALESCE(avg_order_value, 0) < 0
LIMIT 100;

-- 5) mart_seller_performance uniqueness + metrics
SELECT seller_key, COUNT(*) AS cnt
FROM olist_sales_marts.mart_seller_performance
GROUP BY seller_key
HAVING COUNT(*) > 1
ORDER BY cnt DESC, seller_key
LIMIT 100;

SELECT *
FROM olist_sales_marts.mart_seller_performance
WHERE COALESCE(order_count, 0) < 0
    OR COALESCE(line_item_count, 0) < 0
    OR COALESCE(gross_revenue, 0) < 0
LIMIT 100;

-- 6) mart_seller_performance review score range when present
SELECT *
FROM olist_sales_marts.mart_seller_performance
WHERE avg_review_score IS NOT NULL
    AND (avg_review_score < 1 OR avg_review_score > 5)
LIMIT 100;

-- 7) Cross-check with core counts (quick sanity)
SELECT
    (SELECT COUNT(*) FROM olist_sales_core.fact_order_items) AS fact_order_items_cnt,
    (SELECT COUNT(*) FROM olist_sales_marts.mart_daily_sales) AS mart_daily_sales_cnt,
    (SELECT COUNT(*) FROM olist_sales_marts.mart_product_performance) AS mart_product_performance_cnt,
    (SELECT COUNT(*) FROM olist_sales_marts.mart_customer_360) AS mart_customer_360_cnt,
    (SELECT COUNT(*) FROM olist_sales_marts.mart_seller_performance) AS mart_seller_performance_cnt;