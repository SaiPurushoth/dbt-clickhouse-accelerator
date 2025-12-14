{{ config(
    materialized='table',
    tags=['mart']
) }}

WITH menu_item_current AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['menu_item_id']) }} AS menu_item_sk,
        menu_item_id,
        menu_id,
        menu_type_id,
        menu_type_name,
        truck_brand_name,
        menu_item_name,
        item_category,
        cost_of_goods_usd,
        sale_price_usd,
        profit_margin_pct,
        calories,
        protein_grams,
        carbs_grams,
        fat_grams,
        sodium_mg,
        is_vegetarian,
        is_vegan,
        is_gluten_free,
        has_valid_pricing,
        
        -- Add from profitability analysis
        sales_performance_tier,
        profitability_tier,
        trend_status,
        peak_meal_period,
        
        -- Add menu optimization flags
        CASE 
            WHEN profit_margin_pct >= 60 AND sales_performance_tier = 'High Performer' THEN 'Star'
            WHEN profit_margin_pct >= 60 AND sales_performance_tier IN ('Medium Performer', 'Low Performer') THEN 'Cash Cow'
            WHEN profit_margin_pct < 40 AND sales_performance_tier = 'High Performer' THEN 'Loss Leader'
            WHEN profit_margin_pct < 40 AND sales_performance_tier IN ('Low Performer', 'New/Inactive') THEN 'Dog'
            ELSE 'Question Mark'
        END AS menu_matrix_classification,
        
        -- Dietary summary score
        (CASE WHEN is_vegetarian THEN 1 ELSE 0 END +
         CASE WHEN is_vegan THEN 1 ELSE 0 END +
         CASE WHEN is_gluten_free THEN 1 ELSE 0 END) AS dietary_options_score,
        
        TRUE AS is_active,
        created_ts,
        now() AS updated_ts
        
    FROM {{ ref('int_menu_profitability') }}
    WHERE menu_item_id IS NOT NULL
    
    QUALIFY ROW_NUMBER() OVER (PARTITION BY menu_item_id ORDER BY updated_ts DESC) = 1
)

SELECT * FROM menu_item_current
