select *
from {{ ref('stg_reviews') }}
where review_score is not null
  and (review_score < 1 or review_score > 5)