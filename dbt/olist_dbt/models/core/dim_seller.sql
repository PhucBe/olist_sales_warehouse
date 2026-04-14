SELECT
    md5(seller_id) AS seller_key,
    seller_id,
    seller_city,
    seller_state
FROM {{ ref('stg_sellers') }}