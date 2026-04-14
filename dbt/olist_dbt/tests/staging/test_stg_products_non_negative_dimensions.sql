select *
from {{ ref('stg_products') }}
where (product_weight_g is not null and product_weight_g < 0)
   or (product_length_cm is not null and product_length_cm < 0)
   or (product_height_cm is not null and product_height_cm < 0)
   or (product_width_cm is not null and product_width_cm < 0)
   or (product_photos_qty is not null and product_photos_qty < 0)
