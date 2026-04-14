-- =====================================================
-- CHECK STAGING LAYER
-- Default schemas used in this file:
-- raw_layer, olist_sales_staging
-- =====================================================


-- 1) Row counts: raw vs staging


-- 2) stg_orders: duplicate order_id


-- 3) stg_orders: invalid timeline


-- 4) stg_orders: invalid status values


-- 5) stg_order_items: duplicate grain check


-- 6) stg_order_items: negative price/freight


-- 7) stg_order_items -> stg_orders orphan check


-- 8) stg_customers: duplicate customer_id


-- 9) stg_customers: blank customer_unique_id


-- 10) stg_products: duplicate product_id


-- 11) stg_products: invalid physical attributes


-- 12) stg_sellers: duplicate seller_id


-- 13) stg_payments: duplicate logical grain


-- 14) stg_payments: invalid values


-- 15) stg_reviews: invalid review score when present
