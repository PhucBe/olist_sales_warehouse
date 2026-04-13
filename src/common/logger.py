from datetime import datetime
from pathlib import Path
import logging


# Hàm tạo và trả về logger
def get_logger(name: str = "olist_project", log_dir: str = "logs") -> logging.Logger:
    logger = logging.getLogger(name) # Lấy logger theo tên

    if logger.handlers:
        return logger # Nếu logger đã có handler rồi thì trả luôn
    
    logger.setLevel(logging.INFO) # Đặt mức log tối thiểu là INFO
    logger.propagate = False # Tắt progagate để log không bị đẩy ngược lên root logger

    log_dir_path = Path(log_dir) # Tạo đối tượng Path cho thư mục log
    log_dir_path.mkdir(parents=True, exist_ok=True) # Tạo tên file log theo format <logger_name>_YYYYMMDD.log
    log_file = log_dir_path / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ) # Tạo formatter để định dạng mỗi dòng log

    console_handler = logging.StreamHandler() # Tạo handler để log ra console
    console_handler.setFormatter(formatter) # Gắn formatter cho console handler

    file_handler = logging.FileHandler(log_file, encoding="utf-8") # Tạo handler để log ra file
    file_handler.setFormatter(formatter) # Gắn formatter cho file handler

    logger.addHandler(console_handler) # Gắn console handler vào logger
    logger.addHandler(file_handler) # Gắn file handler vào logger

    return logger # Trả logger đã cấu hình xong