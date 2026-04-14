from __future__ import annotations
from pathlib import Path
from typing import Any
import csv

from src.common.config import load_app_config
from src.common.logger import get_logger
from src.common.utils import sanitize_identifier
from src.ingestion.datasets import DATASETS
from src.ingestion.load_redshift_raw import get_redshift_connection


# Khai báo các cột bắt buộc tối thiểu theo từng dataset
REQUIRED_COLUMNS: dict[str, list[str]] = {
    "orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
    "order_items": ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"],
    "customers": ["customer_id", "customer_unique_id", "customer_city", "customer_state"],
    "products": ["product_id", "product_category_name"],
    "sellers": ["seller_id", "seller_city", "seller_state"],
    "order_payments": ["order_id", "payment_type", "payment_value"],
    "order_reviews": ["review_id", "order_id", "review_score"],
}


# Đếm số dòng dữ liệu thực trong file CSV, Không tính dòng header
def count_csv_data_rows(local_csv_path: str | Path, encoding: str = "utf-8") -> int:
    local_csv_path = Path(local_csv_path) # Chuẩn hóa đường dẫn thành Path object

    with open(local_csv_path, "r", encoding=encoding, newline="") as f: # Mở file CSV để đọc
        reader = csv.reader(f) # Tạo CSV reader

        next(reader, None) # Bỏ qua dòng đầu tiên (header)

        return sum(1 for _ in reader) # Đếm số dòng còn lại


# Đọc dòng header của CSV rồi sanitize tên cột
def read_and_sanitize_headers(local_csv_path: str | Path, encoding: str = "utf-8") -> list[str]:
    local_csv_path = Path(local_csv_path) # Chuẩn hóa path

    with open(local_csv_path, "r", encoding=encoding, newline="") as f: # Mở file CSV
        reader = csv.reader(f) # Tạo reader

        headers = next(reader, None) # Đọc dòng đầu tiên

    if not headers:
        raise ValueError(f"CSV file has no header: {local_csv_path}") # Nếu file không có header thì báo lỗi

    return [sanitize_identifier(col) for col in headers] # Làm sạch từng tên cột trước khi trả về


# Kiểm tra xem sau khi sanitize có cột nào bị trùng tên không
def validate_no_duplicate_headers(headers: list[str], dataset_name: str) -> None:
    duplicates = sorted({col for col in headers if headers.count(col) > 1}) # Tạo danh sách các cột bị trùng

    if duplicates:
        raise ValueError(
            f"Dataset '{dataset_name}' has duplicated headers after sanitize: {duplicates}"
        ) # Nếu có cột trùng thì fail ngay


# Kiểm tra dataset có đủ các cột bắt buộc không
def validate_required_columns(headers: list[str], dataset_name: str) -> None:
    required = REQUIRED_COLUMNS.get(dataset_name, []) # Lấy danh sách cột bắt buộc của dataset hiện tại

    missing = [col for col in required if col not in headers] # Tìm các cột bị thiếu

    if missing:
        raise ValueError(
            f"Dataset '{dataset_name}' is missing required columns: {missing}"
        ) # Nếu thiếu thì fail


# Hàm validate một file CSV local
def validate_local_csv(
    dataset: dict[str, Any],
    local_csv_path: str | Path,
    encoding: str,
    logger,
) -> dict[str, Any]:
    local_csv_path = Path(local_csv_path) # Chuẩn hóa path

    dataset_name = dataset["name"] # Lấy tên dataset, ví dụ orders, customers...

    if not local_csv_path.exists():
        raise FileNotFoundError(f"Missing local file: {local_csv_path}") # Nếu file không tồn tại thì fail ngay

    headers = read_and_sanitize_headers(local_csv_path, encoding=encoding) # Đọc và làm sạch header

    validate_no_duplicate_headers(headers, dataset_name) # Kiểm tra header có bị trùng sau sanitize không

    validate_required_columns(headers, dataset_name) # Kiểm tra có đủ cột bắt buộc không

    row_count = count_csv_data_rows(local_csv_path, encoding=encoding) # Đếm số dòng dữ liệu

    if row_count <= 0:
        raise ValueError(f"Dataset '{dataset_name}' has no data rows: {local_csv_path}") # Nếu file không có data row nào thì fail

    result = {
        "dataset_name": dataset_name,         # tên dataset
        "file_path": str(local_csv_path),     # đường dẫn file
        "header_count": len(headers),         # số cột
        "data_row_count": row_count,          # số dòng dữ liệu
        "headers": headers,                   # danh sách header đã sanitize
    } # Gom kết quả validate thành dict để nơi khác có thể dùng tiếp

    logger.info(
        "Local raw validation passed | dataset=%s | rows=%s | columns=%s",
        dataset_name,
        row_count,
        len(headers),
    ) # Ghi log thành công

    return result # Trả kết quả validate


# Hàm lấy row count của một raw table trong Redshift
def get_redshift_table_row_count(config: dict[str, Any], raw_table: str) -> int:
    schema_name = config["redshift"]["schema_raw"] # Lấy tên schema raw từ config

    sql = f"select count(*) from {schema_name}.{raw_table};" # Tạo câu SQL đếm số dòng

    conn = get_redshift_connection(config) # Mở connection Redshift

    try:
        with conn.cursor() as cur: # Mở cursor để chạy SQL
            cur.execute(sql)

            result = cur.fetchone() # Lấy kết quả count(*)

            return int(result[0]) # Ép về int rồi trả ra
    finally:
        conn.close() # Luôn đóng connection dù thành công hay lỗi


# Hàm đối chiếu số dòng giữa local CSV và raw table trong Redshift
def validate_redshift_raw_count(
    config: dict[str, Any],
    raw_table: str,
    expected_rows: int,
    logger,
) -> int:
    actual_rows = get_redshift_table_row_count(config, raw_table) # Lấy số dòng thực tế ở Redshift

    if actual_rows != expected_rows:
        raise ValueError(
            f"Row count mismatch for {config['redshift']['schema_raw']}.{raw_table}: "
            f"expected={expected_rows}, actual={actual_rows}"
        ) # Nếu số dòng không khớp với kỳ vọng thì báo lỗi

    logger.info(
        "Redshift raw validation passed | table=%s.%s | rows=%s",
        config["redshift"]["schema_raw"],
        raw_table,
        actual_rows,
    ) # Nếu khớp thì ghi log thành công

    return actual_rows # Trả số dòng thực tế


# Hàm validate toàn bộ file raw local theo danh sách DATASETS
def validate_all_local_raw_files(config: dict[str, Any], logger) -> list[dict[str, Any]]:
    data_dir = Path(config["paths"]["local_data_dir"]) # Lấy thư mục chứa dữ liệu local

    encoding = config["ingestion"]["encoding"] # Lấy encoding từ config

    results: list[dict[str, Any]] = [] # Danh sách kết quả validate của từng dataset

    for dataset in DATASETS: # Lặp qua từng dataset
        local_path = data_dir / dataset["filename"] # Ghép đường dẫn local cho file CSV tương ứng

        result = validate_local_csv(
            dataset=dataset,
            local_csv_path=local_path,
            encoding=encoding,
            logger=logger,
        ) # Validate file CSV đó

        results.append(result) # Lưu kết quả vào list

    return results # Trả toàn bộ kết quả


# Hàm main để chạy file này độc lập
def main() -> None:
    config = load_app_config() # Load config project

    logger = get_logger(
        name="olist_validate_raw",
        log_dir=config["paths"]["log_dir"],
    ) # Tạo logger riêng cho validate raw

    results = validate_all_local_raw_files(config=config, logger=logger) # Validate toàn bộ local raw files

    logger.info("All local raw validations passed for %s dataset(s).", len(results)) # Log tổng kết
 

# Nếu chạy file trực tiếp bằng Python thì gọi main()
if __name__ == "__main__":
    main()