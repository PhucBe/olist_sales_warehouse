# STORYTELLING.md
## 1. Bối cảnh giả định
Một sàn thương mại điện tử quy mô vừa đang tăng số lượng đơn hàng nhưng đội vận hành và kinh doanh chưa có một nơi dữ liệu tập trung để theo dõi hiệu quả bán hàng.

Dữ liệu hiện có nằm rải rác theo nhiều file CSV:
- orders
- order_items
- customers
- products
- sellers
- payments
- reviews

Điều này làm cho việc trả lời các câu hỏi kinh doanh mất thời gian, dễ sai số và khó mở rộng.

---
## 2. Vấn đề business
Ban quản lý muốn trả lời nhanh các câu hỏi sau:

- Doanh thu đang tăng hay giảm theo ngày, tháng?
- Category nào bán tốt nhất?
- Seller nào đóng góp doanh thu cao nhất?
- Khách hàng quay lại mua nhiều hay ít?
- Giao hàng chậm có làm review thấp hơn không?

Nhưng dữ liệu gốc chưa sẵn sàng để phân tích trực tiếp vì:
- nhiều bảng riêng lẻ
- grain khác nhau
- dễ double count
- khó join đúng nếu làm thủ công
---
## 3. Mục tiêu của project
Xây một analytics warehouse để biến dữ liệu thô thành dữ liệu tin cậy, có cấu trúc, sẵn sàng cho dashboard và phân tích.

Kết quả mong muốn:
- có raw layer để lưu dữ liệu gốc
- có data model chuẩn để phân tích ổn định
- có marts phục vụ BI
- có pipeline có thể chạy lặp lại và kiểm tra chất lượng
---
## 4. Cách kể câu chuyện dự án
### Bước 1 — Thu thập dữ liệu
Project lấy dữ liệu Olist dạng CSV làm nguồn đầu vào mô phỏng tình huống doanh nghiệp nhận dữ liệu từ nhiều hệ thống khác nhau.
### Bước 2 — Lưu raw để có thể replay
Dữ liệu được upload lên S3 và load vào Redshift raw layer để giữ bản gốc, phục vụ reload hoặc backfill khi cần.
### Bước 3 — Chuẩn hóa và model hóa
Dùng dbt để chuẩn hóa dữ liệu trong staging, sau đó build các bảng dimension, fact và marts.

Trung tâm của model là:  `fact_order_items`

Grain: `1 row = 1 order_id + 1 order_item_id`

Grain này phù hợp nhất cho phân tích doanh thu, sản phẩm, seller và khách hàng.
### Bước 4 — Điều phối pipeline
Dùng Airflow để orchestration các bước ingest, load raw, run dbt và test.
### Bước 5 — Phục vụ dashboard
Từ marts, dashboard có thể hiển thị KPI và insight ổn định cho business.
---
## 5. Dữ liệu này dùng để tạo ra gì?
Project này dùng dữ liệu để tạo ra 4 nhóm đầu ra chính:
### 1) Daily Sales Overview
Theo dõi:
- revenue
- orders
- AOV
- trend theo ngày/tháng
### 2) Product / Category Performance
Theo dõi:
- top product
- top category
- doanh thu theo category
- sản phẩm đóng góp nhiều nhất
### 3) Customer 360
Theo dõi:
- total customers
- repeat customers
- repeat rate
- customer AOV
### 4) Seller Performance
Theo dõi:
- top sellers
- revenue theo seller
- review score trung bình
- delivery performance
---
## 6. Giá trị tạo ra cho business
Warehouse này giúp business:
- xem dữ liệu nhanh hơn
- giảm làm báo cáo thủ công
- thống nhất metric giữa các bộ phận
- dễ mở rộng dashboard và use case mới
- có nền tảng để ra quyết định dựa trên dữ liệu