select *
from {{ ref('stg_order_items') }}
where freight_value is not null
  and freight_value < 0