# DECISION

## 1. Scope V1
Project tập trung vào 4 bài toán chính:
- daily sales
- product / category performance
- customer 360
- seller performance
---
## 2. Chọn fact chính
**Fact chính:** `fact_order_items`
```text
1 row = 1 order_id + 1 order_item_id
```
Lý do:
- phù hợp bài toán e-commerce
- phân tích doanh thu theo sản phẩm / seller tự nhiên
- dễ build marts cho dashboard
---
## 3. Chọn layered model
```text
raw_layer -> staging -> core -> marts
```
Lý do:
- tách dữ liệu gốc và dữ liệu đã transform
- dễ debug
- dễ backfill
- dễ mở rộng thêm marts
---
## 4. Payments không join thẳng vào fact line-item
`order_payments` giữ ở grain order.
Lý do:
- 1 order có thể có nhiều payment lines
- join thẳng vào `fact_order_items` dễ làm nhân bản doanh thu
- chỉ nên aggregate hoặc enrich ở mart khi thật sự cần
---
## 5. Reviews không join trực tiếp vào fact line-item
`order_reviews` dùng cẩn thận ở mức order hoặc mart.
Lý do:
- join trực tiếp dễ nổ số dòng
- review phù hợp hơn cho seller / delivery analysis
---
## 6. Repeat customer dùng `customer_unique_id`
Không chỉ dựa vào `customer_id`.
Lý do:
- `customer_id` là record-level id trong dataset
- `customer_unique_id` phù hợp hơn để nhận diện cùng một khách quay lại
---
## 7. Raw layer ưu tiên ingest ổn định
Raw tables được tạo từ header CSV, cột để `varchar` trước.
Lý do:
- giảm lỗi ingest ban đầu
- type casting để dbt xử lý ở staging
- phù hợp hướng raw càng gần source càng tốt
---
## 8. Ingestion hiện theo full reload pattern
Khi `truncate_before_load = true`, raw load hoạt động gần với full reload.
Lý do:
- đơn giản cho V1
- dễ kiểm tra và debug
- đủ tốt cho portfolio project
---
## 9. Airflow chỉ orchestration
Business logic chính đặt ở Python ingestion và dbt, không nhét vào DAG.
Lý do:
- DAG gọn hơn
- dễ test từng phần
- đúng vai trò của orchestration layer