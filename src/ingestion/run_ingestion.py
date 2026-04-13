from pathlib import Path
import os

from src.common.config import load_app_config
from src.common.logger import get_logger
from src.common.utils import build_s3_key, today_str
from src.ingestion.datasets import DATASETS
from src.ingestion.load_redshift_raw import load_csv_to_redshift_raw
from src.ingestion.upload_to_s3 import upload_file_to_s3


# Hàm xác định ngày load
def resolve_load_date() -> str:
    return os.getenv("LOAD_DATE", today_str())


# Hàm main là điểm bắt đầu của ingestion pipeline
def main() -> None:
    config = load_app_config() # Load config từ .env + configs/settings.yml
    load_date = resolve_load_date() # Xác định ngày load hiện tại

    logger = get_logger(
        name="olist_ingestion",
        log_dir=config["paths"]["log_dir"],
    ) # Tạo logger riêng cho ingestion pipeline

    logger.info(
        "Starting ingestion pipeline | env=%s | load_date=%s",
        config["runtime"]["env_name"],
        load_date,
    ) # Ghi log bắt đầu pipeline

    data_dir = Path(config["paths"]["local_data_dir"]) # Lấy thư mục chứa file CSV local
    bucket_name = config["aws"]["bucket_name"] # Lấy tên bucket S3
    raw_prefix = config["aws"]["s3_prefix_raw"] # Lấy prefix raw trên S3, ví dụ "raw"

    # Lặp qua từng dataset trông danh sách DATASETS
    for dataset in DATASETS:
        local_path = data_dir / dataset["filename"]

        # Nếu file local không tồn tại
        if not local_path.exists():
            message = f"Missing local file: {local_path}"

            if config["ingestion"]["fail_on_messing_file"]:
                raise FileNotFoundError(message) # Nếu config yêu cầu fail ngay khi thiếu file thì raise lỗi
            logger.warning(message) # Nếu không thì chỉ warning và bỏ qua dataset đó
            
            continue

        s3_key = build_s3_key(
            prefix=raw_prefix,
            dataset_name=dataset["name"],
            filename=dataset["filename"],
            load_date=load_date,
        ) # Tạo S3 key theo cấu trúc chuẩn

        # Bước 1: upload file local lên S3
        upload_file_to_s3(
            local_path=local_path,
            bucket_name=bucket_name,
            s3_key=s3_key,
            region=config["aws"]["region"],
            logger=logger,
        )

        # Bước 2: load từ S3 vào Redshift raw table
        load_csv_to_redshift_raw(
            config=config,
            local_csv_path=local_path,
            raw_table=dataset["raw_table"],
            bucket_name=bucket_name,
            s3_key=s3_key,
            logger=logger,
        )

    logger.info("Ingestion pipeline finished successfully.") # Nếu đi hết toàn bộ datasets mà không lỗi thì log  thành công


if __name__ == "__main__":
    main()