import redshift_connector
from pathlib import Path
import csv

from src.common.utils import sanitize_identifier


# Hàm tạo kết nối tới Redshift dựa trên config đã load sẵn
def get_redshift_connection(config: dict):
    conn = redshift_connector.connect(
        host=config["redshift"]["host"],
        port=config["redshift"]["port"],
        database=config["redshift"]["database"],
        user=config["redshift"]["user"],
        password=config["redshift"]["password"],
    ) # Tạo connection bằng host, port, database, user, password

    conn.autocommit = True # Bật autocommit để mỗi lần SQL chạy xong được commi tngay
    return conn


# Hàm đọc dòng header của file CSV
def read_csv_headers(local_csv_path: str | Path, encoding: str = "utf-8") -> list[str]:
    local_csv_path = Path(local_csv_path) # Chuẩn hóa local_csv_path thành Path object

    # Mở file csv để đọc
    with open(local_csv_path, "r", encoding=encoding, newline="") as f:
        reader = csv.reader(f)
        headers = next(reader) # Làm sạch toàn bộ header trước khi trả về
    
    return [sanitize_identifier(col) for col in headers]


# Hàm đảm bảo schema raw tồn tại trong Redshift
def create_schema_if_not_exists(conn, schema_name: str, logger) -> None:
    schema_name = sanitize_identifier(schema_name) # Làm sạch tên schema để an toàn hơn

    logger.info("Ensuring schema exists: %s", schema_name) # Ghi log để biết đang làm gì
    
    sql = f"create schema if not exists {schema_name};" # Tạo câu SQL create schema nếu chưa tồn tại
    with conn.cursor() as cur:
        cur.execute(sql) # Mở cursor để chạy SQL


# Hàm tạo bảng raw dựa trên header CSV
def create_table_from_csv_header(
        conn,
        schema_name: str,
        table_name: str,
        local_csv_path: str | Path,
        encoding: str,
        logger,
) -> None:
    schema_name = sanitize_identifier(schema_name) # Làm sạch tên schema
    table_name = sanitize_identifier(table_name) # Làm sạch tên table

    headers = read_csv_headers(local_csv_path=local_csv_path, encoding=encoding) # Đọc danh sách header đã sanitize từ file CSV
    columns_sql = ",\n    ".join([f'"{col}" varchar(65535)' for col in headers]) # Biến từ header thành định nghĩa cột SQL

    sql = f"""
    create table if not exists {schema_name}.{table_name} (
        {columns_sql}
    );
    """

    logger.info("Ensuring raw table exists: %s.%s", schema_name, table_name) # Ghi log
    
    with conn.cursor() as cur:
        cur.execute(sql) # Thực thi SQL


# Hàm xóa toàn bộ dữ liệu cũ trong bảng raw
def truncate_table(conn, schema_name: str, table_name: str, logger) -> None:
    schema_name = sanitize_identifier(schema_name) # Làm sạch tên schema
    table_name = sanitize_identifier(table_name) # Làm sạch tên table

    sql = f"truncate table {schema_name}.{table_name};" # Tạo SQL truncate table

    logger.info("Truncating table: %s.%s", schema_name, table_name) # Ghi log

    with conn.cursor() as cur:
        cur.execute(sql) # Chạy SQL


# Hàm COPY dữ liệu từ S3 vào Redshift
def copy_from_s3_to_redshift(
    conn,
    schema_name: str,
    table_name: str,
    bucket_name: str,
    s3_key: str,
    iam_role_arn: str,
    region: str,
    delimiter: str,
    logger,
) -> None:
    schema_name = sanitize_identifier(schema_name)
    table_name = sanitize_identifier(table_name)
    s3_uri = f"s3://{bucket_name}/{s3_key}" # Ghép thành S3 URI đầy đủ

    sql = f"""
    copy {schema_name}.{table_name}
    from '{s3_uri}'
    iam_role '{iam_role_arn}'
    region '{region}'
    csv
    ignoreheader 1
    delimiter '{delimiter}'
    emptyasnull
    blanksasnull
    truncatecolumns
    acceptinvchars
    dateformat 'auto'
    timeformat 'auto';
    """
    # Tạo câu lệnh COPY của Redshift
    # csv              : file nguồn là CSV
    # ignoreheader 1   : bỏ qua dòng header đầu tiên
    # delimiter        : dấu phân cách cột, ví dụ ","
    # emptyasnull      : chuỗi rỗng -> NULL
    # blanksasnull     : chuỗi toàn khoảng trắng -> NULL
    # truncatecolumns  : nếu giá trị quá dài thì cắt bớt
    # acceptinvchars   : chấp nhận ký tự không hợp lệ thay vì fail ngay
    # dateformat/timeformat auto: để Redshift tự nhận diện ngày giờ

    logger.info("COPY into %s.%s from %s", schema_name, table_name, s3_uri) # Ghi log
    
    with conn.cursor() as cur:
        cur.execute(sql) # Chạy câu COPY


# Hàm orchestration nhỏ để load 1 file CSV vào 1 bảng raw của Redshift
def load_csv_to_redshift_raw(
    config: dict,
    local_csv_path: str | Path,
    raw_table: str,
    bucket_name: str,
    s3_key: str,
    logger,
) -> None:
    conn = get_redshift_connection(config) # Mở kết nối Redshift

    try:
        # Lấy các config cần dùng
        schema_name = config["redshift"]["schema_raw"]
        encoding = config["ingestion"]["encoding"]
        delimiter = config["ingestion"]["csv_delimiter"]

        # Bước 1: Đảm bảo schema raw tồn tại
        create_schema_if_not_exists(conn, schema_name=schema_name, logger=logger)

        # Bước 2: Đảm bảo bảng raw tồn tại theo header CSV
        create_table_from_csv_header(
            conn=conn,
            schema_name=schema_name,
            table_name=raw_table,
            local_csv_path=local_csv_path,
            encoding=encoding,
            logger=logger,
        )

        # Bước 3: Nếu config yêu cầu thì xóa dữ liệu cũ trước khi load mới
        if config["ingestion"]["truncate_before_load"]:
            truncate_table(conn, schema_name=schema_name, table_name=raw_table, logger=logger)

        # Bước 4: COPY dữ liệu từ S3 vào bảng raw
        copy_from_s3_to_redshift(
            conn=conn,
            schema_name=schema_name,
            table_name=raw_table,
            bucket_name=bucket_name,
            s3_key=s3_key,
            iam_role_arn=config["redshift"]["iam_role_arn"],
            region=config["aws"]["region"],
            delimiter=delimiter,
            logger=logger,
        )
    finally:
        conn.close() # Dù thành công hay lỗi, vẫn đóng connection để tránh leak tài nguyên