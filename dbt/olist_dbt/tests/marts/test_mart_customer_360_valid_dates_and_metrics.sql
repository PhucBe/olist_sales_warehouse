select *
from {{ ref('mart_customer_360') }}
where first_order_date is null
   or last_order_date is null
   or last_order_date < first_order_date
   or coalesce(total_orders, 0) < 0
   or coalesce(total_items, 0) < 0
   or coalesce(lifetime_value, 0) < 0
   or coalesce(avg_order_value, 0) < 0
