
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
        
        -- Clean menu type
        INITCAP(TRIM(menu_type)) AS menu_type_name,
        
        -- Clean truck brand
        INITCAP(TRIM(truck_brand_name)) AS truck_brand_name,
        
        menu_item_id,
        
        -- Clean menu item name
        INITCAP(TRIM(menu_item_name)) AS menu_item_name,
        
        -- Standardize categories
        CASE 
            WHEN UPPER(TRIM(item_category)) IN ('MAIN', 'ENTREE', 'ENTREES') THEN 'Main Dish'
            WHEN UPPER(TRIM(item_category)) IN ('SIDE', 'SIDES') THEN 'Side Dish'
            WHEN UPPER(TRIM(item_category)) IN ('DRINK', 'DRINKS', 'BEVERAGE') THEN 'Beverage'
            WHEN UPPER(TRIM(item_category)) IN ('DESSERT', 'DESSERTS') THEN 'Dessert'
            WHEN UPPER(TRIM(item_category)) IN ('APPETIZER', 'APPETIZERS', 'STARTER') THEN 'Appetizer'
            ELSE INITCAP(TRIM(item_category))
        END AS item_category,
        
        INITCAP(TRIM(item_subcategory)) AS item_subcategory,
        
        -- Clean pricing (handle potential VARIANT issues)
        COALESCE(cost_of_goods_usd, 0) AS cost_of_goods_usd,
        COALESCE(sale_price_usd, 0) AS sale_price_usd,
        
        -- Calculate profit margin
        CASE 
            WHEN sale_price_usd > 0 
            THEN ((sale_price_usd - COALESCE(cost_of_goods_usd, 0)) / sale_price_usd) * 100
            ELSE 0
        END AS profit_margin_pct,
        
        -- Parse health metrics (handle VARIANT properly)
        menu_item_health_metrics_obj AS health_metrics,
        
        -- Extract specific health metrics if available (fix VARIANT casting)
        CASE 
            WHEN menu_item_health_metrics_obj:calories IS NOT NULL 
            THEN menu_item_health_metrics_obj:calories::NUMBER
            ELSE NULL
        END AS calories,
        
        CASE 
            WHEN menu_item_health_metrics_obj:protein_g IS NOT NULL 
            THEN menu_item_health_metrics_obj:protein_g::NUMBER
            ELSE NULL
        END AS protein_grams,
        
        CASE 
            WHEN menu_item_health_metrics_obj:carbs_g IS NOT NULL 
            THEN menu_item_health_metrics_obj:carbs_g::NUMBER
            ELSE NULL
        END AS carbs_grams,
        
        CASE 
            WHEN menu_item_health_metrics_obj:fat_g IS NOT NULL 
            THEN menu_item_health_metrics_obj:fat_g::NUMBER
            ELSE NULL
        END AS fat_grams,
        
        CASE 
            WHEN menu_item_health_metrics_obj:sodium_mg IS NOT NULL 
            THEN menu_item_health_metrics_obj:sodium_mg::NUMBER
            ELSE NULL
        END AS sodium_mg,
        
        -- Add dietary flags (handle VARIANT boolean extraction properly)
        CASE 
            WHEN menu_item_health_metrics_obj:vegetarian = 'true' 
            OR menu_item_health_metrics_obj:vegetarian = true
            OR UPPER(menu_item_name) LIKE '%VEGGIE%'
            OR UPPER(menu_item_name) LIKE '%VEGETARIAN%'
            THEN TRUE
            ELSE FALSE
        END AS is_vegetarian,
        
        CASE 
            WHEN menu_item_health_metrics_obj:vegan = 'true'
            OR menu_item_health_metrics_obj:vegan = true
            OR UPPER(menu_item_name) LIKE '%VEGAN%'
            THEN TRUE
            ELSE FALSE
        END AS is_vegan,
        
        CASE 
            WHEN menu_item_health_metrics_obj:gluten_free = 'true'
            OR menu_item_health_metrics_obj:gluten_free = true
            OR UPPER(menu_item_name) LIKE '%GLUTEN FREE%'
            OR UPPER(menu_item_name) LIKE '%GF%'
            THEN TRUE
            ELSE FALSE
        END AS is_gluten_free,
        
        -- Pricing validation flags
        CASE 
            WHEN sale_price_usd > 0 AND cost_of_goods_usd >= 0 
            AND sale_price_usd > cost_of_goods_usd
            THEN TRUE
            ELSE FALSE
        END AS has_valid_pricing,
        
        CURRENT_TIMESTAMP() AS created_ts,
        CURRENT_TIMESTAMP() AS updated_ts
        
    FROM source_data
    WHERE menu_item_id IS NOT NULL
        AND menu_item_name IS NOT NULL
        AND TRIM(menu_item_name) != ''
)

SELECT * FROM cleaned_menu