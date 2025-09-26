-- Pseudocode Logic:
-- 1. Source raw menu data
-- 2. Clean menu item names and categories
-- 3. Validate pricing data (fix VARIANT casting issues)
-- 4. Parse health metrics JSON properly

{{ config(
    materialized='view',
    tags=['staging']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'raw_menu') }}
),

cleaned_menu AS (
    SELECT
        menu_id,
        menu_type_id,
        UPPER(TRIM(menu_type)) AS menu_type_name,
        UPPER(TRIM(truck_brand_name)) AS truck_brand_name,
        menu_item_id,
        UPPER(TRIM(menu_item_name)) AS menu_item_name,
        
        -- Standardize categories using multiIf
        multiIf(
            UPPER(TRIM(item_category)) IN ('MAIN', 'ENTREE', 'ENTREES'), 'Main Dish',
            UPPER(TRIM(item_category)) IN ('SIDE', 'SIDES'), 'Side Dish',
            UPPER(TRIM(item_category)) IN ('DRINK', 'DRINKS', 'BEVERAGE'), 'Beverage',
            UPPER(TRIM(item_category)) IN ('DESSERT', 'DESSERTS'), 'Dessert',
            UPPER(TRIM(item_category)) IN ('APPETIZER', 'APPETIZERS', 'STARTER'), 'Appetizer',
            UPPER(TRIM(item_category))
        ) AS item_category,
        
        -- Clean pricing
        ifNull(cost_of_goods_usd, 0) AS cost_of_goods_usd,
        ifNull(sale_price_usd, 0) AS sale_price_usd,
        
        -- Calculate profit margin
        if(sale_price_usd > 0,
           ((sale_price_usd - ifNull(cost_of_goods_usd, 0)) / sale_price_usd) * 100,
           0) AS profit_margin_pct,
        
        -- Handle JSON fields appropriately for ClickHouse
        JSONExtractFloat(menu_item_health_metrics_obj, 'calories') AS calories,
        JSONExtractFloat(menu_item_health_metrics_obj, 'protein_g') AS protein_grams,
        JSONExtractFloat(menu_item_health_metrics_obj, 'carbs_g') AS carbs_grams,
        JSONExtractFloat(menu_item_health_metrics_obj, 'fat_g') AS fat_grams,
        JSONExtractFloat(menu_item_health_metrics_obj, 'sodium_mg') AS sodium_mg,
        
        -- Dietary flags
        (JSONExtractBool(menu_item_health_metrics_obj, 'vegetarian') = 1 
         OR match(UPPER(menu_item_name), '(VEGGIE|VEGETARIAN)')) AS is_vegetarian,
        
        (JSONExtractBool(menu_item_health_metrics_obj, 'vegan') = 1 
         OR match(UPPER(menu_item_name), 'VEGAN')) AS is_vegan,
        
        (JSONExtractBool(menu_item_health_metrics_obj, 'gluten_free') = 1 
         OR match(UPPER(menu_item_name), '(GLUTEN FREE|GF)')) AS is_gluten_free,
        
        -- Pricing validation flags
        (sale_price_usd > 0 
         AND cost_of_goods_usd >= 0 
         AND sale_price_usd > cost_of_goods_usd) AS has_valid_pricing,
        
        now() AS created_ts,
        now() AS updated_ts
        
    FROM source_data
    WHERE menu_item_id IS NOT NULL
        AND menu_item_name IS NOT NULL
        AND TRIM(menu_item_name) != ''
)

SELECT * FROM cleaned_menu