select *
from {{ ref('stg_reviews') }}
where review_score is not null
  and review_score not in (1, 2, 3, 4, 5)
