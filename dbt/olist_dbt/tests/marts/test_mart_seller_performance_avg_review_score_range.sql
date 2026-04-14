select *
from {{ ref('mart_seller_performance') }}
where avg_review_score is not null
  and (avg_review_score < 1 or avg_review_score > 5)
