SELECT
    CAST(review_id AS VARCHAR(50)) AS review_id,
    CAST(order_id AS VARCHAR(50)) AS order_id,
    CAST(review_score AS INTEGER) AS review_score,
    CAST(review_comment_title AS VARCHAR(255)) AS review_comment_title,
    CAST(review_comment_message AS VARCHAR(2000)) AS review_comment_message,
    CAST(review_creation_date AS TIMESTAMP) AS review_creation_ts,
    CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_ts
FROM {{ source('raw', 'raw_order_reviews') }}