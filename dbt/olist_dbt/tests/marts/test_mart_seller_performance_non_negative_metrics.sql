select *
from {{ ref('mart_seller_performance') }}
where coalesce(order_count, 0) < 0
   or coalesce(line_item_count, 0) < 0
   or coalesce(gross_revenue, 0) < 0
