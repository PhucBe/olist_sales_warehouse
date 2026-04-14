select *
from {{ ref('stg_customers') }}
where customer_unique_id is null
   or trim(customer_unique_id) = ''
