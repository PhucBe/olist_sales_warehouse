WITH base_dates AS (
    SELECT DISTINCT CAST(order_purchase_ts AS DATE) AS date_day
    FROM {{ ref('stg_orders') }}
    WHERE order_purchase_ts IS NOT NULL
)

SELECT
    CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INTEGER) AS date_key,
    date_day,
    EXTRACT(year FROM date_day) AS year,
    EXTRACT(quarter FROM date_day) AS quarter,
    EXTRACT(month FROM date_day) AS month,
    EXTRACT(day FROM date_day) AS day,
    EXTRACT(week FROM date_day) AS week_of_year,
    TRIM(TO_CHAR(date_day, 'Day')) AS day_name
FROM base_dates