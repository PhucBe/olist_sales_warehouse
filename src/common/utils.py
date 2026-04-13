from datetime import date
from pathlib import Path
import re


# Hàm đảm bảo thư mục tồn tại
def ensure_directory(path: str | Path) -> None:
    Path(path).mkdir(parents=True, exist_ok=True) # Nếu chưa có thì tạo mới


# Hàm trả về ngày hôm nay dưới dạng chuỗi ISO "YYYY-MM-DD"
def today_str() -> str:
    return date.today().isoformat()


# Hàm tạo S3 key theo chuẩn đã định
def build_s3_key(prefix: str, dataset_name: str, filename: str, load_date: str) -> str:
    clean_prefix = prefix.strip("/") # Bỏ dấu "/" thừa ở đầu/cuối prefix
    return f"{clean_prefix}/{dataset_name}/load_date={load_date}/{filename}" # Trả về key hoàn chỉnh


# Hàm chuẩn hóa tên cột/ tên bảng để an toàn hơn khi tạo SQL identifier
def sanitize_identifier(value: str) -> str:
    value = value.strip().lower() # Bỏ khoảng trắng đầu/cuối và chuyển thành chữ thường
    value = re.sub(r"[^a-z0-9_]", "_", value) # Thay mọi ký tự không phải a-z, 0-9, _ thành dấu _
    value = re.sub(r"_+", "_", value).strip("_") # Gộp nhiều dấu _ liên tiếp thành 1 dấu _

    if value and value[0].isdigit():
        value = f"col_{value}" # Nếu tên bắt đâu bằng số thì thêm tiền tố "col_"

    return value # Trả về tên đã làm sạch