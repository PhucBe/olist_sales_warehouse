WITH review_base AS (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_ts,
        review_answer_ts,
        ROW_NUMBER() OVER(
            PARTITION BY review_id, order_id
            ORDER BY
                review_answer_ts DESC nulls LAST,
                review_creation_ts DESC nulls LAST,
                review_score DESC nulls LAST
        ) AS rn
    FROM {{ ref('stg_reviews') }}
    WHERE review_id IS NOT NULL
        AND order_id IS NOT NULL
)

SELECT
    md5(review_id || '-' || order_id) AS review_key,
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_ts,
    review_answer_ts
FROM review_base
WHERE rn = 1