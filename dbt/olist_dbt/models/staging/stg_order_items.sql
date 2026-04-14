SELECT
    CAST(order_id AS VARCHAR(50)) AS order_id,
    CAST(order_item_id AS INTEGER) AS order_item_id,
    CAST(product_id AS VARCHAR(50)) AS product_id,
    CAST(seller_id AS VARCHAR(50)) AS seller_id,
    CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_ts,
    CAST(price AS NUMERIC(18,2)) AS price,
    CAST(freight_value AS NUMERIC(18,2)) AS freight_value
FROM {{ source('raw', 'raw_order_items') }}