SELECT
    CAST(product_category_name AS VARCHAR(255)) AS product_category_name,
    CAST(product_category_name_english AS VARCHAR(255)) AS product_category_name_english
FROM {{ ref('product_category_name_translation') }}