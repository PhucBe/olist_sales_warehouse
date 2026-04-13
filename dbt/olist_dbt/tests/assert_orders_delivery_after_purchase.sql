select *
from {{ ref('stg_orders') }}
where order_delivered_customer_ts is not null
  and order_purchase_ts is not null
  and order_delivered_customer_ts < order_purchase_ts