# DATA_MODEL

## 1. Modeling approach

The warehouse uses a layered model:
```text
raw_layer -> staging -> core -> marts
```
- **raw_layer**: dữ liệu gốc từ source CSV
- **staging**: chuẩn hóa tên cột, kiểu dữ liệu, null handling cơ bản
- **core**: star schema dùng lại cho nhiều phân tích
- **marts**: bảng phục vụ dashboard / BI
---
## 2. Main fact grain
**Main fact:** `fact_order_items`
```text
1 row = 1 order_id + 1 order_item_id
```
Lý do chọn grain này:
- phù hợp bài toán e-commerce
- phân tích doanh thu theo sản phẩm / category / seller dễ hơn fact ở mức order
- join sang customer, product, seller, date tự nhiên
---
## 3. Raw layer

| Table | Grain | Notes |
|---|---|---|
| `raw_orders` | 1 row = 1 order | trạng thái đơn, thời gian mua, giao hàng, customer |
| `raw_order_items` | 1 row = 1 line item | sản phẩm, seller, price, freight |
| `raw_customers` | 1 row = 1 customer_id record | có cả `customer_id` và `customer_unique_id` |
| `raw_products` | 1 row = 1 product | category, kích thước, ảnh |
| `raw_sellers` | 1 row = 1 seller | city, state |
| `raw_order_payments` | 1 row = 1 payment line | 1 order có thể nhiều dòng |
| `raw_order_reviews` | 1 row = 1 review record | dùng cẩn thận khi join |
---
## 4. Staging layer

| Model | Built from | Purpose |
|---|---|---|
| `stg_orders` | `raw_orders` | chuẩn hóa order fields |
| `stg_order_items` | `raw_order_items` | chuẩn hóa line-item fields |
| `stg_customers` | `raw_customers` | chuẩn hóa customer fields |
| `stg_products` | `raw_products` | chuẩn hóa product fields |
| `stg_sellers` | `raw_sellers` | chuẩn hóa seller fields |
| `stg_payments` | `raw_order_payments` | chuẩn hóa payment fields |
| `stg_reviews` | `raw_order_reviews` | chuẩn hóa review fields |

Nguyên tắc staging:
- 1 model gần tương ứng 1 source chính
- chưa làm business logic nặng
- dùng `source()` để đọc từ raw layer
---
## 5. Core layer

### Dimensions

| Model | Grain | Main purpose |
|---|---|---|
| `dim_customer` | 1 row = 1 customer | thông tin khách hàng dùng cho phân tích customer |
| `dim_product` | 1 row = 1 product | thông tin sản phẩm / category |
| `dim_seller` | 1 row = 1 seller | thông tin seller |
| `dim_date` | 1 row = 1 date | calendar dimension |

### Fact

| Model | Grain | Main fields |
|---|---|---|
| `fact_order_items` | 1 row = 1 order item | order, customer, product, seller, price, freight, timestamps |

### Main join logic

```text
stg_order_items
  + stg_orders      by order_id
  + stg_customers   by customer_id
  + stg_products    by product_id
  + stg_sellers     by seller_id
  + dim_date        by purchase date
```

Lưu ý:
- `order_items` là gốc của fact
- `orders` bổ sung status và purchase / delivery timestamps
- `payments` và `reviews` ở grain order, không nhét trực tiếp vào line-item fact để tránh double count
- phân tích repeat customer nên dựa vào `customer_unique_id`
---
## 6. Mart layer

| Mart | Grain | Main use |
|---|---|---|
| `mart_daily_sales` | 1 row = 1 day | revenue, orders, AOV trend |
| `mart_product_performance` | 1 row = product / category / date aggregate | top products, top categories |
| `mart_customer_360` | 1 row = 1 customer | orders, spend, AOV, repeat behavior |
| `mart_seller_performance` | 1 row = 1 seller | revenue, orders, delivery / review oriented KPIs |
---
## 7. Relationship summary

```text
                        dim_customer
                             |
                             V
      dim_product --> fact_order_items <-- dim_date
                             ^
                             |
                         dim_seller
```

`fact_order_items` là trung tâm của star schema.
---
## 8. Key modeling decisions

- Fact chính chọn ở mức **order item**, không phải order
- `payments` giữ ở mức order để tránh nhân bản doanh thu
- `reviews` dùng thận trọng vì không nên join trực tiếp làm nổ số dòng
- `customer_unique_id` quan trọng cho bài toán khách quay lại
---
## 9. Scope of V1

Trong V1, data model tập trung vào:
- sales trend
- product / category performance
- customer 360
- seller performance