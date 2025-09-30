{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='sync_all_columns',
    tags=['mart']
) }}

WITH enriched_orders AS (
    SELECT *
    FROM {{ ref('int_orders_enriched') }}
    {% if is_incremental() %}
        WHERE updated_ts > (SELECT MAX(updated_ts) FROM {{ this }})
    {% endif %}
),

fact_orders AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} AS order_sk,
        o.order_id,
        
        -- Dimension foreign keys (COALESCE is standard SQL and ClickHouse compatible)
        COALESCE(dt.truck_sk, '-1') AS truck_sk,
        COALESCE(dl.location_sk, '-1') AS location_sk,
        COALESCE(dc.customer_sk, '-1') AS customer_sk,
        COALESCE(dd.date_sk, '-1') AS order_date_sk,
        COALESCE(dtime.time_sk, '-1') AS order_time_sk, 
        
        -- Order attributes
        o.shift_id,
        o.shift_start_time,
        o.shift_end_time,
        o.order_channel,
        o.order_timestamp,
        o.served_timestamp,
        o.order_currency,
        o.meal_period,
        o.order_hour,
        o.order_day_of_week,
        
        -- Financial measures
        o.order_amount,
        o.order_tax_amount,
        o.order_discount_amount,
        o.order_total,
        o.total_cost_of_goods,
        o.order_profit,
        o.order_profit_margin_pct,
        
        -- Operational measures
        o.processing_time_minutes,
        o.line_item_count,
        o.total_items_ordered,
        o.avg_item_price,
        
        -- Classifications
        o.order_size_category,
        o.service_speed_category,
        o.revenue_category,
        o.discount_percentage,
        
        -- Business context
        o.category_count,
        o.categories_ordered,
        
        o.created_ts,
        now() AS updated_ts
        
    FROM enriched_orders o
    
    -- Join with dimensions
    LEFT JOIN {{ ref('dim_truck') }} dt
        ON o.truck_id = dt.truck_id
    
    LEFT JOIN {{ ref('dim_location') }} dl
        ON o.location_id = dl.location_id
        
    LEFT JOIN {{ ref('dim_customer') }} dc
        ON o.customer_id = dc.customer_id
        
    LEFT JOIN {{ ref('dim_date') }} dd
        ON toDate(o.order_timestamp) = dd.date_actual 
        
    LEFT JOIN {{ ref('dim_time') }} dtime
        ON toDateTime(o.order_timestamp) = dtime.time_actual 
)

SELECT * FROM fact_orders