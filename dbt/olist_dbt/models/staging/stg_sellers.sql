SELECT
    CAST(seller_id AS VARCHAR(50)) AS seller_id,
    LOWER(TRIM(seller_city)) AS seller_city,
    UPPER(TRIM(seller_state)) AS seller_state
FROM {{ source('raw', 'raw_sellers') }}