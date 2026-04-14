select *
from {{ ref('fact_order_items') }}
where coalesce(price, 0) < 0
   or coalesce(freight_value, 0) < 0
   or coalesce(gross_item_amount, 0) < 0
