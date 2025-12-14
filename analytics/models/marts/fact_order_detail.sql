{{ config(
    materialized='incremental',
    unique_key='order_detail_id',
    on_schema_change='sync_all_columns',
    tags=['core', 'fact']
) }}

WITH order_details_base AS (
    SELECT * FROM {{ ref('stg_order_details') }}
    {% if is_incremental() %}
        -- Standard dbt incremental logic (ClickHouse compatible)
        WHERE updated_ts > (SELECT MAX(updated_ts) FROM {{ this }})
    {% endif %}
),

fact_order_details AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['od.order_detail_id']) }} AS order_detail_sk,
        od.order_detail_id,
        
        COALESCE(fo.order_sk, '-1') AS order_sk,
        COALESCE(dm.menu_item_sk, '-1') AS menu_item_sk,
        
        -- Line item attributes
        od.discount_id,
        od.line_number,
        od.quantity,
        od.unit_price,
        od.line_total,
        od.line_discount_amount,
        od.net_line_total,
        od.line_discount_percentage,
        
        -- Cost and profit calculations
        (od.quantity * COALESCE(dm.cost_of_goods_usd, 0)) AS line_cost_of_goods,
        (od.net_line_total - (od.quantity * COALESCE(dm.cost_of_goods_usd, 0))) AS line_profit,
        
        CASE 
            WHEN od.net_line_total > 0
            THEN ((od.net_line_total - (od.quantity * COALESCE(dm.cost_of_goods_usd, 0))) / od.net_line_total) * 100
            ELSE 0
        END AS line_profit_margin_pct,
        
        -- Menu item context
        dm.item_category,
        dm.item_subcategory,
        dm.truck_brand_name,
        dm.is_vegetarian,
        dm.is_vegan,
        dm.is_gluten_free,
        
        -- Data quality flags
        od.is_valid_line_item,
        od.price_quantity_consistent,
        
        od.created_ts,
        now() AS updated_ts
        
    FROM order_details_base od
    
    LEFT JOIN {{ ref('fact_order') }} fo
        ON od.order_id = fo.order_id
        
    LEFT JOIN {{ ref('dim_menu_item') }} dm
        ON od.menu_item_id = dm.menu_item_id
        
    WHERE od.is_valid_line_item = TRUE
)

SELECT * FROM fact_order_details