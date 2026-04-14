Step 1:
py -3.12 -m venv .venv
-->

Step 2:
source .venv/Scripts/activate
-->

Step 3:
python -m pip install --upgrade pip
--> 

Step 4:
pip install dbt-core dbt-redshift
-->

Step 5:
requirements.txt
-->

Step 6:
pip install -r requirements.txt
-->

Step 7:
config/settings.yml
-->

Step 8:
.env.example
-->

Step 9:
src/common/config.py
src/common/logger.py
src/common/utils.py
-->

Step 10:
src/ingestion/datasets.py
src/ingestion/upload_to_s3.py
src/ingestion/load_redshift_raw.py
src/ingestion/run_ingestion.py
-->

Step 11:
python -m src.ingestion.run_ingestion
-->

Step 12:
dbt_project.yml + packages.yml + dbt/seeds/product_category_name_translation.csv
-->

Step 13:
dbt/olist_dbt/models/staging/{_sources.yml,schema.yml}
dbt/olist_dbt/models/staging/{stg_*.sql}
-->

Step 14:
dbt/olist_dbt/models/core/{schema.yml,dim_*.sql,fact_order_items.sql}
-->

Step 15:
dbt/olist_dbt/models/marts/{schema.yml,mart_*.sql}
-->

Step 16:
dbt/profiles/profiles.yml
-->

Step 17:
dbt debug --profiles-dir ../profiles
dbt deps --profiles-dir ../profiles
dbt seed --profiles-dir ../profiles
-->

Step 18:
dbt run --select staging --profiles-dir ../profiles
dbt test --select staging --profiles-dir ../profiles
-->

Step 19:
dbt run --select core --profiles-dir ../profiles
dbt test --select core --profiles-dir ../profiles
-->

Step 20:
dbt run --select marts --profiles-dir ../profiles
dbt test --select marts --profiles-dir ../profiles
-->

Step 21:
dbt/tests/{staging.sql,core.sql,marts.sql}
-->

Step 22:
dbt test --select path:tests/staging --profiles-dir ../profiles
dbt test --select path:tests/core --profiles-dir ../profiles
dbt test --select path:tests/marts --profiles-dir ../profiles
-->

Step 23:
sql/check/{check_raw, check_staging, check_core, check_marts}
-->

Step 24:

-->

Step 25:

-->
