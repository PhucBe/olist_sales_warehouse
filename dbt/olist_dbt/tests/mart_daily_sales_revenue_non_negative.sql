select *
from {{ ref('mart_daily_sales') }}
where gross_revenue < 0