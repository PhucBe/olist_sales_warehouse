select *
from {{ ref('fact_order_items') }}
where price < 0
   or freight_value < 0
   or gross_item_amount < 0