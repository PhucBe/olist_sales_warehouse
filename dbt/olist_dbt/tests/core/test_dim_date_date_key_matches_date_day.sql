select *
from {{ ref('dim_date') }}
where date_day is not null
  and date_key <> cast(to_char(date_day, 'YYYYMMDD') as integer)
