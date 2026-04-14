select *
from {{ ref('stg_orders') }}
where order_estimated_delivery_ts is not null
  and order_purchase_ts is not null
  and order_estimated_delivery_ts < order_purchase_ts
