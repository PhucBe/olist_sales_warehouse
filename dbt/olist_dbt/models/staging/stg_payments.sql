SELECT
    CAST(order_id AS VARCHAR(50)) AS order_id,
    CAST(payment_sequential AS INTEGER) AS payment_sequential,
    LOWER(TRIM(payment_type)) AS payment_type,
    CAST(payment_installments AS INTEGER) AS payment_installments,
    CAST(payment_value AS NUMERIC(18,2)) AS payment_value
FROM {{ source('raw', 'raw_order_payments') }}