select *
from {{ ref('stg_product_category_translation') }}
where product_category_name is null
   or trim(product_category_name) = ''
   or product_category_name_english is null
   or trim(product_category_name_english) = ''
