select *
from {{ ref('fact_order_items') }}
where order_date is not null
  and order_date_key <> cast(to_char(order_date, 'YYYYMMDD') as integer)
