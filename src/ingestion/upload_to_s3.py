from pathlib import Path
import boto3


# Hàm upload 1 file local lên S3
def upload_file_to_s3(
        local_path: str | Path,
        bucket_name: str,
        s3_key: str,
        region: str,
        logger,
) -> str:
    local_path = Path(local_path) # Chuẩn hóa local_path thành Path object

    if not local_path.exists():
        raise FileNotFoundError(f"Local file not found: {local_path}") # Nếu file local không tồn tại thì báo lỗi
    
    session = boto3.Session(region_name=region) # Tạo session boto3 theo region chỉ định
    logger.info("Uploading %s to s3://%s/%s", local_path.name, bucket_name, s3_key) # Ghi log trước khi upload
    
    s3_client = session.client("s3") # Tạo S3 client từ session
    s3_client.upload_file(str(local_path), bucket_name, s3_key) # Upload file local lên bucket + key tương ứng
    s3_uri = f"s3://{bucket_name}/{s3_key}" # Tạo lại S3 URI đầy đủ để log và trả về

    logger.info("Upload completed: %s", s3_uri) # Ghi log sau khi upload thành công
    return s3_uri