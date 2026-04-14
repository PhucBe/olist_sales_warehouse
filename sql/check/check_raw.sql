--
select table_schema, table_name
from svv_tables
where table_schema = 'raw_layer'
order by table_name;


--
select count(*) as row_count from raw_layer.raw_orders;
select count(*) as row_count from raw_layer.raw_order_items;
select count(*) as row_count from raw_layer.raw_customers;
select count(*) as row_count from raw_layer.raw_products;
select count(*) as row_count from raw_layer.raw_sellers;
select count(*) as row_count from raw_layer.raw_order_payments;
select count(*) as row_count from raw_layer.raw_order_reviews;


--
select * from raw_layer.raw_orders limit 5;
select * from raw_layer.raw_order_items limit 5;


--
select count(*) from raw_layer.raw_sellers;