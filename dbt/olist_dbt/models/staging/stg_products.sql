WITH products AS (
    SELECT
        CAST(product_id AS VARCHAR(50)) AS product_id,
        CAST(product_category_name AS VARCHAR(255)) AS product_category_name_pt,
        CAST(product_name_lenght AS INTEGER) AS product_name_length,
        CAST(product_description_lenght AS INTEGER) AS product_description_length,
        CAST(product_photos_qty AS INTEGER) AS product_photos_qty,
        CAST(product_weight_g AS INTEGER) AS product_weight_g,
        CAST(product_length_cm AS INTEGER) AS product_length_cm,
        CAST(product_height_cm AS INTEGER) AS product_height_cm,
        CAST(product_width_cm AS INTEGER) AS product_width_cm
    FROM {{ source('raw', 'raw_products') }}
),

category_translation AS (
    SELECT
        product_category_name,
        product_category_name_english
    FROM {{ ref('stg_product_category_translation') }}
)

SELECT
    p.product_id,
    p.product_category_name_pt,
    COALESCE(t.product_category_name_english, 'unknown') AS product_category_name_en,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM products p
left join category_translation t
    on p.product_category_name_pt = t.product_category_name