from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
import subprocess


PROJECT_ROOT = Path("/opt/airflow/project") # Khai báo thư mục gốc của project bên trong container Airflow
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "olist_dbt" # Thư mục project dbt
DBT_PROFILES_DIR = PROJECT_ROOT / "dbt" / "profiles" # Thư mục chứa profiles.yml cho dbt
ROOT_ENV_FILE = PROJECT_ROOT / ".env" # File .env ở root project, dùng để nạp biến môi trường trước khi chạy dbt


# Hàm chạy dbt run cho backfill
def run_dbt_backfill(**context) -> None:
    from dotenv import load_dotenv # Import load_dotenv ngay trong function

    load_dotenv(ROOT_ENV_FILE, override=True) # Nạp biến môi trường từ file .env

    dag_run = context.get("dag_run") # Lấy đối tượng dag_run từ context của Airflow
    
    conf = dag_run.conf if dag_run and dag_run.conf else {} # Nếu người dùng trigger DAG và truyền conf JSON thì lấy conf đó

    dbt_select = conf.get("select", "path:models") # Lấy selector dbt từ conf
    
    full_refresh = conf.get("full_refresh", False) # Lấy cờ full_refresh từ conf

    
    cmd = [
        "dbt", # chương trình cần chạy
        "run", # command dbt: build model
        "--project-dir", # chỉ rõ thư mục project dbt
        str(DBT_PROJECT_DIR),
        "--profiles-dir", # chỉ rõ nơi chứa profiles.yml
        str(DBT_PROFILES_DIR),
        "--target", # chọn target trong profiles.yml
        "dev",
        "--select", # chọn model/path/tag cần build
        dbt_select,
    ] # Tạo câu lệnh dbt run dưới dạng list

    if full_refresh:
        cmd.append("--full-refresh") # Nếu người dùng bật full_refresh thì thêm flag này vào lệnh dbt

    print("Backfill run command:", " ".join(cmd)) # In ra command để dễ debug trong Airflow logs
    
    subprocess.run(cmd, check=True, cwd=str(DBT_PROJECT_DIR)) # Chạy lệnh dbt


# Hàm chạy dbt test cho backfill
def test_dbt_backfill(**context) -> None:
    from dotenv import load_dotenv # Import trong function giống như trên

    load_dotenv(ROOT_ENV_FILE, override=True) # Nạp .env trước khi chạy dbt

    dag_run = context.get("dag_run") # Lấy dag_run từ context
    
    conf = dag_run.conf if dag_run and dag_run.conf else {} # Lấy conf JSON nếu có, không thì dùng dict rỗng

    dbt_select = conf.get("select", "path:models") # Lấy selector dbt từ conf

    cmd = [
        "dbt", # chương trình dbt
        "test", # command test
        "--project-dir", # thư mục project dbt
        str(DBT_PROJECT_DIR),
        "--profiles-dir", # thư mục profiles
        str(DBT_PROFILES_DIR),
        "--target", # target dev
        "dev",
        "--select", # vùng model/test được chọn
        dbt_select,
    ] # Tạo câu lệnh dbt test

    print("Backfill test command:", " ".join(cmd)) # In command ra log để dễ xem
    
    subprocess.run(cmd, check=True, cwd=str(DBT_PROJECT_DIR)) # Chạy dbt test


# Default_args là bộ tham số mặc định áp cho các task trong DAG
default_args = {
    "owner": "phucvu",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Khởi tạo DAG bằng context manager "with DAG(...) as dag"
with DAG(
    dag_id="olist_backfill_pipeline",
    description="Manual backfill pipeline for selected dbt models",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "backfill", "redshift", "dbt"],
) as dag:
    start = EmptyOperator(task_id="start") # Task mở đầu, không làm gì, chi làm mốc start

    dbt_run_backfill = PythonOperator(
        task_id="dbt_run_backfill",
        python_callable=run_dbt_backfill,
    ) # Task chạy dbt run backfill

    dbt_test_backfill = PythonOperator(
        task_id="dbt_test_backfill",
        python_callable=test_dbt_backfill,
    ) # Task chạy dbt test sau khi run xong

    end = EmptyOperator(task_id="end") # Task kết thúc, chỉ để làm mốc end

    # Thiết lập thứ tự task:
    # start -> dbt_run_backfill -> dbt_test_backfill -> end
    start >> dbt_run_backfill >> dbt_test_backfill >> end