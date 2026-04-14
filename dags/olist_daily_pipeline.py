from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
import subprocess


PROJECT_ROOT = Path("/opt/airflow/project") # Đường dẫn root project trong container Airflow
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "olist_dbt" # Thư mục chứa project dbt
DBT_PROFILES_DIR = PROJECT_ROOT / "dbt" / "profiles" # Thư mục chứa profiles.yml của dbt
ROOT_ENV_FILE = PROJECT_ROOT / ".env" # File .env ở root project


# Hàm chạy ingestion
def run_ingestion_task() -> None:
    # Import main trong function thay vì import ở đầu file
    # Mục đích: tránh parse-time error khi scheduler Airflow đọc DAG
    # Nếu import ngay từ đâu mà thiếu package/path thì DAG có thể bị lỗi parse
    from src.ingestion.run_ingestion import main
    main()


# Hàm chạy dbt linh hoạt cho cả run và test
def run_dbt_task(command: str, select: str | None = None, full_refresh: bool = False) -> None:
    from dotenv import load_dotenv # Import load_dotenv ngay trong function

    load_dotenv(ROOT_ENV_FILE, override=True) # dbt profile thường dùng env_var(), nên load .env của repo root trước

    cmd = [
        "dbt", # chương trình dbt
        command, # dbt run hoặc dbt test
        "--project-dir", # thư mục project dbt
        str(DBT_PROJECT_DIR),
        "--profiles-dir", # thư mục profiles
        str(DBT_PROFILES_DIR),
        "--target", # target dev
        "dev",
    ] # Tạo command dbt cơ bản

    if select:
        cmd.extend(["--select", select]) # Nếu có truyền select thì thêm --select vào command

    if command == "run" and full_refresh:
        cmd.append("--full-refresh") # Nếu là dbt run và yêu cầu full_refresh thì thêm flag tương ứng

    print("Running command:", " ".join(cmd)) # In command ra log để dễ debug
    
    subprocess.run(cmd, check=True, cwd=str(DBT_PROJECT_DIR)) # Thực thi command


# Cấu hình mặc định cho các task trong DAG
default_args = {
    "owner": "phucvu", # tên owner của DAG
    "depends_on_past": False, # run hôm nay không phụ thuộc hôm qua
    "retries": 1, # retry 1 lần nếu fail
    "retry_delay": timedelta(minutes=5), # sau 5 phút mới retry
}


# Định nghĩa DAG daily pipeline
with DAG(
    dag_id="olist_daily_pipeline", # tên DAG trong Airflow
    description="Daily pipeline: ingestion -> dbt staging -> core -> marts -> test", # mô tả DAG
    default_args=default_args, # áp cấu hình mạc định
    start_date=datetime(2026, 4, 1), # ngày bắt đầu lịch
    schedule="@daily", # chạy lỗi ngày 1 lần
    catchup=False, # không chạy bù lịch cũ
    tags=["olist", "daily", "redshift", "dbt"], # tags hiển thị trên UI
) as dag:
    start = EmptyOperator(task_id="start") # Task start để làm điểm bắt đầu trực quan

    run_ingestion = PythonOperator(
        task_id="run_ingestion",
        python_callable=run_ingestion_task,
    ) # Task chạy pipeline ingestion: local CSV -> S3 -> Redshift raw

    dbt_run_staging = PythonOperator(
        task_id="dbt_run_staging",
        python_callable=run_dbt_task,
        op_kwargs={
            "command": "run",
            "select": "path:models/staging",
        },
    ) # Task chạy dbt cho layer staging

    dbt_run_core = PythonOperator(
        task_id="dbt_run_core",
        python_callable=run_dbt_task,
        op_kwargs={
            "command": "run",
            "select": "path:models/core",
        },
    ) # Task chạy dt cho layer core

    dbt_run_marts = PythonOperator(
        task_id="dbt_run_marts",
        python_callable=run_dbt_task,
        op_kwargs={
            "command": "run",
            "select": "path:models/marts",
        },
    ) # Task chạy dbt cho layer marts

    dbt_test_all = PythonOperator(
        task_id="dbt_test_all",
        python_callable=run_dbt_task,
        op_kwargs={
            "command": "test",
            "select": "path:models",
        },
    ) # Task chạy toàn bộ test sau khi build xong hết

    end = EmptyOperator(task_id="end") # Task end để làm mốc kết thúc DAG

    # Khai báo dependency giữa các task:
    # start -> ingestion -> dbt staging -> dbt core -> dbt marts -> dbt test -> end
    start >> run_ingestion >> dbt_run_staging >> dbt_run_core >> dbt_run_marts >> dbt_test_all >> end