WITH order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
)

SELECT
    md5(order_items.order_id || '-' || CAST(order_items.order_item_id AS VARCHAR)) AS order_item_key,
    order_items.order_id,
    order_items.order_item_id,
    orders.customer_id,
    order_items.product_id,
    order_items.seller_id,

    md5(orders.customer_id) AS customer_key,
    md5(order_items.product_id) AS product_key,
    md5(order_items.seller_id) AS seller_key,

    CAST(TO_CHAR(CAST(orders.order_purchase_ts AS DATE), 'YYYYMMDD') AS INTEGER) AS order_date_key,
    CAST(orders.order_purchase_ts AS DATE) AS order_date,
    orders.order_purchase_ts,
    orders.order_status,

    order_items.shipping_limit_ts,
    order_items.price,
    order_items.freight_value,
    (order_items.price + order_items.freight_value) AS gross_item_amount
FROM order_items
INNER JOIN orders
    ON order_items.order_id = orders.order_id