-- =========================================================
-- check_bi.sql
-- Mục đích:
--   Kiểm tra 4 BI views đã được tạo đúng và dữ liệu ổn
-- Schema mặc định dưới đây: olist_sales_marts
-- Nếu schema của bạn là olist_dev_marts thì replace lại
-- =========================================================


-- =========================================================
-- 0) KIỂM TRA CÁC BI VIEWS CÓ TỒN TẠI KHÔNG
-- =========================================================
SELECT
    table_schema,
    table_name
FROM information_schema.views
WHERE table_schema = 'olist_sales_marts'
  AND table_name IN (
      'bi_customer_360',
      'bi_daily_sales_overview',
      'bi_product_performance',
      'bi_seller_performance'
  )
ORDER BY table_name;


-- =========================================================
-- 1) SO SÁNH ROW COUNT: BI VS MART GỐC
-- =========================================================
SELECT
    'bi_customer_360_row_count' AS check_name,
    (SELECT COUNT(*) FROM olist_sales_marts.bi_customer_360) AS bi_count,
    (SELECT COUNT(*) FROM olist_sales_marts.mart_customer_360) AS mart_count,
    CASE
        WHEN (SELECT COUNT(*) FROM olist_sales_marts.bi_customer_360)
           = (SELECT COUNT(*) FROM olist_sales_marts.mart_customer_360)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status

UNION ALL

SELECT
    'bi_daily_sales_overview_row_count' AS check_name,
    (SELECT COUNT(*) FROM olist_sales_marts.bi_daily_sales_overview) AS bi_count,
    (SELECT COUNT(*) FROM olist_sales_marts.mart_daily_sales) AS mart_count,
    CASE
        WHEN (SELECT COUNT(*) FROM olist_sales_marts.bi_daily_sales_overview)
           = (SELECT COUNT(*) FROM olist_sales_marts.mart_daily_sales)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status

UNION ALL

SELECT
    'bi_product_performance_row_count' AS check_name,
    (SELECT COUNT(*) FROM olist_sales_marts.bi_product_performance) AS bi_count,
    (SELECT COUNT(*) FROM olist_sales_marts.mart_product_performance) AS mart_count,
    CASE
        WHEN (SELECT COUNT(*) FROM olist_sales_marts.bi_product_performance)
           = (SELECT COUNT(*) FROM olist_sales_marts.mart_product_performance)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status

UNION ALL

SELECT
    'bi_seller_performance_row_count' AS check_name,
    (SELECT COUNT(*) FROM olist_sales_marts.bi_seller_performance) AS bi_count,
    (SELECT COUNT(*) FROM olist_sales_marts.mart_seller_performance) AS mart_count,
    CASE
        WHEN (SELECT COUNT(*) FROM olist_sales_marts.bi_seller_performance)
           = (SELECT COUNT(*) FROM olist_sales_marts.mart_seller_performance)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
;


-- =========================================================
-- 2) BI_CUSTOMER_360 CHECKS
-- =========================================================

-- 2.1 customer_unique_id KHÔNG ĐƯỢC NULL
SELECT
    'bi_customer_360_customer_unique_id_null' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_customer_360
WHERE customer_unique_id IS NULL;

-- 2.2 customer_unique_id KHÔNG ĐƯỢC DUPLICATE
SELECT
    'bi_customer_360_customer_unique_id_duplicate' AS check_name,
    COUNT(*) AS duplicate_groups
FROM (
    SELECT
        customer_unique_id
    FROM olist_sales_marts.bi_customer_360
    GROUP BY customer_unique_id
    HAVING COUNT(*) > 1
) t;

-- 2.3 first_order_date KHÔNG ĐƯỢC LỚN HƠN last_order_date
SELECT
    'bi_customer_360_invalid_order_date_range' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_customer_360
WHERE first_order_date IS NOT NULL
  AND last_order_date IS NOT NULL
  AND first_order_date > last_order_date;

-- 2.4 total_orders PHẢI > 0
SELECT
    'bi_customer_360_total_orders_le_zero' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_customer_360
WHERE total_orders <= 0
   OR total_orders IS NULL;

-- 2.5 total_items KHÔNG ĐƯỢC ÂM
SELECT
    'bi_customer_360_total_items_negative' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_customer_360
WHERE total_items < 0
   OR total_items IS NULL;

-- 2.6 total_revenue KHÔNG ĐƯỢC ÂM
SELECT
    'bi_customer_360_total_revenue_negative' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_customer_360
WHERE total_revenue < 0
   OR total_revenue IS NULL;

-- 2.7 avg_order_value KHÔNG ĐƯỢC ÂM
SELECT
    'bi_customer_360_avg_order_value_negative' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_customer_360
WHERE avg_order_value < 0
   OR avg_order_value IS NULL;

-- 2.8 is_repeat_customer PHẢI KHỚP VỚI total_orders
SELECT
    'bi_customer_360_repeat_flag_mismatch' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_customer_360
WHERE (total_orders > 1 AND is_repeat_customer <> 1)
   OR (total_orders <= 1 AND is_repeat_customer <> 0);


-- =========================================================
-- 3) BI_DAILY_SALES_OVERVIEW CHECKS
-- =========================================================

-- 3.1 order_date KHÔNG ĐƯỢC NULL
SELECT
    'bi_daily_sales_order_date_null' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_daily_sales_overview
WHERE order_date IS NULL;

-- 3.2 order_date PHẢI UNIQUE (DAILY GRAIN)
SELECT
    'bi_daily_sales_order_date_duplicate' AS check_name,
    COUNT(*) AS duplicate_groups
FROM (
    SELECT
        order_date
    FROM olist_sales_marts.bi_daily_sales_overview
    GROUP BY order_date
    HAVING COUNT(*) > 1
) t;

-- 3.3 order_year_text PHẢI KHỚP VỚI order_date
SELECT
    'bi_daily_sales_order_year_text_mismatch' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_daily_sales_overview
WHERE order_year_text <> CAST(EXTRACT(YEAR FROM order_date) AS VARCHAR(4));

-- 3.4 order_month_text PHẢI KHỚP VỚI order_date
SELECT
    'bi_daily_sales_order_month_text_mismatch' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_daily_sales_overview
WHERE order_month_text <> LPAD(CAST(EXTRACT(MONTH FROM order_date) AS VARCHAR(2)), 2, '0');

-- 3.5 order_year PHẢI KHỚP VỚI order_date
SELECT
    'bi_daily_sales_order_year_mismatch' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_daily_sales_overview
WHERE order_year <> EXTRACT(YEAR FROM order_date);

-- 3.6 order_month PHẢI KHỚP VỚI order_date
SELECT
    'bi_daily_sales_order_month_mismatch' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_daily_sales_overview
WHERE order_month <> EXTRACT(MONTH FROM order_date);

-- 3.7 total_revenue PHẢI BẰNG product_revenue + freight_revenue
SELECT
    'bi_daily_sales_total_revenue_mismatch' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_daily_sales_overview
WHERE COALESCE(total_revenue, 0)
    <> COALESCE(product_revenue, 0) + COALESCE(freight_revenue, 0);

-- 3.8 CÁC METRIC KHÔNG ĐƯỢC ÂM
SELECT
    'bi_daily_sales_negative_metrics' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_daily_sales_overview
WHERE order_count < 0
   OR line_item_count < 0
   OR product_revenue < 0
   OR freight_revenue < 0
   OR total_revenue < 0
   OR avg_order_value < 0;


-- =========================================================
-- 4) BI_PRODUCT_PERFORMANCE CHECKS
-- =========================================================

-- 4.1 product_id KHÔNG ĐƯỢC NULL
SELECT
    'bi_product_performance_product_id_null' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_product_performance
WHERE product_id IS NULL;

-- 4.2 product_id KHÔNG ĐƯỢC DUPLICATE (GRAIN PRODUCT)
SELECT
    'bi_product_performance_product_id_duplicate' AS check_name,
    COUNT(*) AS duplicate_groups
FROM (
    SELECT
        product_id
    FROM olist_sales_marts.bi_product_performance
    GROUP BY product_id
    HAVING COUNT(*) > 1
) t;

-- 4.3 product_category_name NULL BAO NHIÊU DÒNG (THÔNG TIN KIỂM TRA)
SELECT
    'bi_product_performance_category_null_info' AS check_name,
    COUNT(*) AS null_count
FROM olist_sales_marts.bi_product_performance
WHERE product_category_name IS NULL;

-- 4.4 total_revenue PHẢI BẰNG product_revenue + freight_revenue
SELECT
    'bi_product_performance_total_revenue_mismatch' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_product_performance
WHERE COALESCE(total_revenue, 0)
    <> COALESCE(product_revenue, 0) + COALESCE(freight_revenue, 0);

-- 4.5 quantity / orders / revenue KHÔNG ĐƯỢC ÂM
SELECT
    'bi_product_performance_negative_metrics' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_product_performance
WHERE total_quantity < 0
   OR order_count < 0
   OR product_revenue < 0
   OR freight_revenue < 0
   OR total_revenue < 0;


-- =========================================================
-- 5) BI_SELLER_PERFORMANCE CHECKS
-- =========================================================

-- 5.1 seller_id KHÔNG ĐƯỢC NULL
SELECT
    'bi_seller_performance_seller_id_null' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_seller_performance
WHERE seller_id IS NULL;

-- 5.2 seller_id KHÔNG ĐƯỢC DUPLICATE (GRAIN SELLER)
SELECT
    'bi_seller_performance_seller_id_duplicate' AS check_name,
    COUNT(*) AS duplicate_groups
FROM (
    SELECT
        seller_id
    FROM olist_sales_marts.bi_seller_performance
    GROUP BY seller_id
    HAVING COUNT(*) > 1
) t;

-- 5.3 total_orders / total_items / total_revenue KHÔNG ĐƯỢC ÂM
SELECT
    'bi_seller_performance_negative_metrics' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_seller_performance
WHERE total_orders < 0
   OR total_items < 0
   OR total_revenue < 0;

-- 5.4 avg_review_score NULL BAO NHIÊU DÒNG (MONITOR, KHÔNG FAIL CỨNG)
SELECT
    'bi_seller_performance_avg_review_score_null_info' AS check_name,
    COUNT(*) AS null_count
FROM olist_sales_marts.bi_seller_performance
WHERE avg_review_score IS NULL;

-- 5.5 avg_review_score NGOÀI RANGE 1-5
SELECT
    'bi_seller_performance_avg_review_score_out_of_range' AS check_name,
    COUNT(*) AS issue_count
FROM olist_sales_marts.bi_seller_performance
WHERE avg_review_score IS NOT NULL
  AND (avg_review_score < 1 OR avg_review_score > 5);


-- =========================================================
-- 6) SAMPLE PREVIEW ĐỂ EYEBALL NHANH
-- =========================================================
SELECT *
FROM olist_sales_marts.bi_customer_360
ORDER BY total_revenue DESC
LIMIT 10;

SELECT *
FROM olist_sales_marts.bi_daily_sales_overview
ORDER BY order_date DESC
LIMIT 10;

SELECT *
FROM olist_sales_marts.bi_product_performance
ORDER BY total_revenue DESC
LIMIT 10;

SELECT *
FROM olist_sales_marts.bi_seller_performance
ORDER BY total_revenue DESC
LIMIT 10;