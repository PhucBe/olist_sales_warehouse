select *
from {{ ref('dim_customer') }}
where customer_unique_id is null
   or trim(customer_unique_id) = ''
