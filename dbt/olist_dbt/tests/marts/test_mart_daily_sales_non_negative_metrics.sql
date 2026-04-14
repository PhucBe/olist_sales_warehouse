select *
from {{ ref('mart_daily_sales') }}
where coalesce(order_count, 0) < 0
   or coalesce(line_item_count, 0) < 0
   or coalesce(product_revenue, 0) < 0
   or coalesce(freight_revenue, 0) < 0
   or coalesce(gross_revenue, 0) < 0
   or coalesce(avg_order_value, 0) < 0
