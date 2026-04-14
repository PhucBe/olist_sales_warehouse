select *
from {{ ref('stg_order_items') }}
where coalesce(price, 0) < 0
   or coalesce(freight_value, 0) < 0
