select *
from {{ ref('stg_order_items') }}
where price < 0