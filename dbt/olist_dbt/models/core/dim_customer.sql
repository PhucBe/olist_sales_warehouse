SELECT
    md5(customer_id) AS customer_key,
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM {{ ref('stg_customers') }}