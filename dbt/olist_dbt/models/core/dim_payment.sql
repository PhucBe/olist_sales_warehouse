SELECT
    md5(order_id || '-' || CAST(payment_sequential AS VARCHAR)) AS payment_key,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM {{ ref('stg_payments') }}
WHERE order_id IS NOT NULL