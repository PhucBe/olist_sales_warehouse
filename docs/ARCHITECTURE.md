# ARCHITECTURE.md

## 1. Mục tiêu kiến trúc

Project này xây dựng một **analytics warehouse cho dữ liệu e-commerce Olist** trên Amazon Web Service:

- ingest nhiều file CSV nguồn
- lưu raw files ở **Amazon S3** để có thể replay / backfill
- nạp dữ liệu vào **Amazon Redshift** ở tầng raw
- transform bằng **dbt** theo các lớp `staging -> core -> marts`
- orchestration bằng **Airflow**
- phục vụ dashboard và phân tích BI
---
## 2. Kiến trúc tổng thể end-to-end

```text
              +---------------------------+
              |   Olist CSV source files  |
              |  orders, items, customers |
              |  products, sellers, etc.  |
              +-------------+-------------+
                            |
                            v
              +---------------------------+
              |     Python ingestion      |
              | validate / upload / load  |
              +-------------+-------------+
                            |
                            v
              +---------------------------+
              |         Amazon S3         |
              |         raw zone          |
              +-------------+-------------+
                            |
                            v
              +---------------------------+
              |      Amazon Redshift      |
              |         raw_layer         |
              +-------------+-------------+
                            |
                            v
              +---------------------------+
              |            dbt            |
              | staging -> core -> marts  |
              +-------------+-------------+
                            |
                            v
              +---------------------------+
              |          Airflow          |
              |schedule / retry / backfill|
              +-------------+-------------+
                            |
                            v
              +---------------------------+
              |      BI / Dashboards      |
              +---------------------------+
```
---
## 3. Vai trò từng thành phần
- **Python ingestion**: đọc CSV, kiểm tra file, upload S3, load vào Redshift raw.
- **Amazon S3**: lưu trữ dữ liệu thô để replay/backfill.
- **Amazon Redshift**: data warehouse trung tâm.
- **dbt**: transform dữ liệu theo các layer `staging -> core -> marts`.
- **Airflow**: điều phối pipeline, retry, schedule, backfill.
- **BI**: đọc mart để làm dashboard.
---
## 4. Luồng dữ liệu
### Bước 1: Ingestion
- Đọc file CSV từ local.
- Upload file lên S3 theo `load_date`.
- Tạo schema raw nếu chưa có.
- Tạo raw table từ header CSV nếu chưa có.
- COPY dữ liệu từ S3 vào Redshift.

### Bước 2: Transformation
- **staging**: chuẩn hóa tên cột, kiểu dữ liệu, xử lý null cơ bản.
- **core**: build dimension và fact chuẩn star schema.
- **marts**: tổng hợp dữ liệu phục vụ dashboard.

### Bước 3: Orchestration
Airflow chạy các bước theo thứ tự:
1. ingest raw
2. load raw Redshift
3. dbt run
4. dbt test
5. theo dõi trạng thái job
---
## 5. Data layers
```text
raw_layer -> staging -> core -> marts
```

- **raw_layer**: dữ liệu gần source nhất.
- **staging**: dữ liệu đã chuẩn hóa.
- **core**: dim/fact tái sử dụng.
- **marts**: bảng sẵn sàng cho BI.
---
## 6. Output chính
- `mart_daily_sales`
- `mart_product_performance`
- `mart_customer_360`
- `mart_seller_performance`
---
## 7. Tóm tắt giá trị kiến trúc
Kiến trúc này giúp project dễ mở rộng, dễ debug, dễ backfill và phù hợp với bài toán analytics warehouse cho e-commerce.