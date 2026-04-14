select *
from {{ ref('stg_payments') }}
where (payment_value is not null and payment_value < 0)
   or (payment_installments is not null and payment_installments < 0)
