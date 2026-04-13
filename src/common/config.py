from __future__ import annotations
from dotenv import load_dotenv
from pathlib import Path
from typing import Any
import yaml
import os


# Xác định thư mục gốc của Project
ROOT_DIR = Path(__file__).resolve().parents[2] # .resolve() chuyển thành đường dẫn tuyệt đối, .parents[2] nghĩa là lùi lên 2 cấp thư mục


# Hàm lấy biến môi trường bắt buộc
def _required_env(name: str) -> str:
    value = os.getenv(name) # Lấy biến môi trường theo tên
    if not value:
        raise ValueError(f"Missing required environment variable: {name}") # Nếu không có giá trị thì raise lỗi để fail sớm
    return value


# Hàm kiểm tra một section trong YAML có tồn tại và có phải dict không
def _require_section(config: dict[str, Any], section_name: str) -> dict[str, Any]:
    section = config.get(section_name) # Lấy section theo tên
    if not isinstance(section, dict):
        raise ValueError(f"Missing or invalid '{section_name}' section in config file") # Nếu section không tồn tại hoặc không phải dict thì lỗi
    return section


# Hàm kiểm tra một key bắt buộc trong section
def _require_key(section: dict[str, Any], key_name: str, full_name: str) -> Any:
    if key_name not in section:
        raise ValueError(f"Missing '{full_name}' in config file") # Nếu key không có trong section thì báo lỗi
    return section[key_name]


# Hàm chính để load toàn bộ app config
def load_app_config(env_name: str | None = None) -> dict[str, Any]:
    load_dotenv(ROOT_DIR / ".env") # Nạp biến môi trường từ file .env ở thư mục gốc

    env_name = env_name or os.getenv("APP_ENV", "settings") # Ưu tiên env_name, nếu không thì đọc APP_ENV hoặc không mặc định là "settings"
    config_path = ROOT_DIR / "config" / f"{env_name}.yml" # Tạo đường dẫn tới file config tương ứng

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}") # Nếu file config không tồn tại thì báo lỗi
    
    # Mở file YAML để đọc nội dung
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise ValueError(f"Invalid YAML config format: {config_path}") # Đảm bảo nội dung YAML đọc ra là dict
    
    # Kiểm tra các section bắt buộc phải có
    paths_section = _require_section(config, "paths")
    aws_section = _require_section(config, "aws")
    redshift_section = _require_section(config, "redshift")
    ingestion_section = _require_section(config, "ingestion")

    # Kiểm tra các ket bắt buộc trong section paths
    local_data_dir = _require_key(paths_section, "local_data_dir", "paths.local_data_dir")
    log_dir = _require_key(paths_section, "log_dir", "paths.log_dir")

    # Kiểm tra các key bắt buộc trong section aws
    _require_key(aws_section, "region", "aws.region")
    _require_key(aws_section, "bucket_name", "aws.bucket_name")
    _require_key(aws_section, "s3_prefix_raw", "aws.s3_prefix_raw")

    # Kiểm tra các key bắt buộc trong section redshift
    _require_key(redshift_section, "schema_raw", "redshift.schema_raw")

    # Kiểm tra các key bắt buộc trong section ingestion
    _require_key(ingestion_section, "csv_delimiter", "ingestion.csv_delimiter")
    _require_key(ingestion_section, "encoding", "ingestion.encoding")
    _require_key(ingestion_section, "truncate_before_load", "ingestion.truncate_before_load")
    _require_key(ingestion_section, "fail_on_missing_file", "ingestion.fail_on_missing_file")

    config["runtime"] = {
        "env_name": env_name,
        "root_dir": str(ROOT_DIR),
    } # Thêm metadata runtime vào config để các module khác tiện dùng

    config["paths"]["local_data_dir"] = str(
        (ROOT_DIR / config["paths"]["local_data_dir"]).resolve()
    ) # Chuyển local_data_dir từ đường dẫn tương đối thành tuyệt đối

    config["paths"]["log_dir"] = str(
        (ROOT_DIR / config["paths"]["log_dir"]).resolve()
    ) # Chuyển log_dir từ tương đối sang tuyệt đối

    # Đọc thông tin kết nỗi Redshift từ biến môi trường
    config["redshift"]["host"] = _required_env("REDSHIFT_HOST")
    config["redshift"]["port"] = int(os.getenv("REDSHIFT_PORT", "5439"))
    config["redshift"]["database"] = _required_env("REDSHIFT_DATABASE")
    config["redshift"]["user"] = _required_env("REDSHIFT_USER")
    config["redshift"]["password"] = _required_env("REDSHIFT_PASSWORD")
    config["redshift"]["iam_role_arn"] = _required_env("REDSHIFT_IAM_ROLE_ARN")

    return config # Trả về dict config hoàn chỉnh để module khác dùng