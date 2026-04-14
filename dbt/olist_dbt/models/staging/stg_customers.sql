SELECT
    CAST(customer_id AS VARCHAR(50)) AS customer_id,
    CAST(customer_unique_id AS VARCHAR(50)) AS customer_unique_id,
    LOWER(TRIM(customer_city)) AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state
FROM {{ source('raw', 'raw_customers') }}