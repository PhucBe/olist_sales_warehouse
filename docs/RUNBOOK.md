# RUNBOOK
## 1. Mục đích
Tài liệu vận hành ngắn gọn cho project Olist Sales Analytics Warehouse.
---
## 2. Thứ tự chạy local

### Bước 1: Cài môi trường
```bash
python -m venv .venv
```
Windows:
```bash
.venv\Scripts\activate
```
Cài package:
```bash
pip install -r requirements.txt
```
---
### Bước 2: Chuẩn bị cấu hình
Tạo file `.env` và điền các biến cần thiết:
- `APP_ENV`
- `REDSHIFT_HOST`
- `REDSHIFT_PORT`
- `REDSHIFT_DATABASE`
- `REDSHIFT_USER`
- `REDSHIFT_PASSWORD`
- `REDSHIFT_IAM_ROLE_ARN`

Kiểm tra `configs/dev.yml`:
- `paths.local_data_dir`
- `paths.log_dir`
- `aws.region`
- `aws.bucket_name`
- `aws.s3_prefix_raw`
- `redshift.schema_raw`
- `ingestion.truncate_before_load`
---
### Bước 3: Chuẩn bị dữ liệu nguồn
Đặt các file CSV vào thư mục `data/raw/`:
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
---
### Bước 4: Chạy ingestion
```bash
python -m src.ingestion.run_ingestion
```
Kết quả mong đợi:
- file được upload lên S3 theo `load_date`
- schema `raw_layer` được tạo nếu chưa có
- raw tables được tạo nếu chưa có
- dữ liệu được `COPY` vào Redshift
---
### Bước 5: Chạy dbt
```bash
cd dbt/olist_dbt

dbt deps --profiles-dir .
dbt run --profiles-dir . --target dev
dbt test --profiles-dir . --target dev
dbt docs generate --profiles-dir . --target dev
```
Kết quả mong đợi:
- build xong `staging`
- build xong `core`
- build xong `marts`
- test pass
---
## 3. Thứ tự chạy chuẩn end-to-end
```text
1. Chuẩn bị .env + config
2. Kiểm tra source CSV
3. Chạy ingestion
4. Kiểm tra raw_layer trong Redshift
5. Chạy dbt deps
6. Chạy dbt run
7. Chạy dbt test
8. Generate dbt docs
9. Mở dashboard / BI
```
---
## 4. Kiểm tra nhanh sau mỗi bước
### Sau ingestion
Kiểm tra:
- file đã có trên S3
- raw tables đã có trong Redshift
- row count > 0

Ví dụ:
```sql
select count(*) from raw_layer.raw_orders;
select count(*) from raw_layer.raw_order_items;
select count(*) from raw_layer.raw_customers;
```
### Sau dbt
Kiểm tra:
```sql
select count(*) from olist_dev_core.fact_order_items;
select count(*) from olist_dev_marts.mart_daily_sales;
```
---
## 5. Airflow run
Airflow dùng để orchestration, không thay thế logic của ingestion và dbt.

Luồng DAG nên là:
```text
ingest raw
-> load Redshift raw
-> dbt run staging
-> dbt run core + marts
-> dbt test
```
Dùng khi:
- schedule hằng ngày
- retry job
- backfill
- theo dõi trạng thái run
---
## 6. Lỗi thường gặp
### Thiếu file CSV
Biểu hiện:
- báo `Missing local file`

Cách xử lý:
- kiểm tra đúng tên file
- kiểm tra đúng thư mục `data/raw`
- kiểm tra `paths.local_data_dir`
### Thiếu biến môi trường
Biểu hiện:
- báo `Missing required environment variable`

Cách xử lý:
- mở `.env`
- điền đủ các biến Redshift và `APP_ENV`
### COPY vào Redshift fail
Biểu hiện:
- lỗi ở bước load raw

Cách xử lý:
- kiểm tra IAM role có quyền đọc S3
- kiểm tra bucket / key S3 đúng
- kiểm tra region đúng
- kiểm tra security/network của Redshift
### dbt debug hoặc dbt run fail
Cách xử lý:
- kiểm tra `profiles.yml`
- kiểm tra schema names
- kiểm tra source/raw tables đã có dữ liệu chưa
- chạy lại từng layer: staging -> core -> marts
### Fact ra 0 dòng
Nguyên nhân thường gặp:
- join key sai
- source staging có dữ liệu nhưng join không match
- logic lọc quá chặt

Cách xử lý:
- test từng model trung gian
- đếm row trước và sau join
- kiểm tra `order_id`, `product_id`, `seller_id`, `customer_id`
---
## 7. Log và debug
- logs của Python ingestion nằm trong thư mục `logs/`
- xem log mới nhất theo ngày chạy
- khi lỗi, đọc từ bước đầu tiên fail thay vì nhìn dòng cuối cùng
---
## 8. Nguyên tắc vận hành
- chạy manual trước, Airflow sau
- kiểm tra raw trước khi kiểm tra mart
- fix từ upstream trước downstream
- không sửa mart khi gốc lỗi nằm ở staging hoặc core