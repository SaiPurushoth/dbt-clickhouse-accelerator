-- Pseudocode Logic:
-- 1. Source raw order header data
-- 2. Parse and validate timestamps (fix timestamp casting issues)
-- 3. Clean currency and amount fields
-- 4. Add derived business metrics

{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail',
    tags=['staging', 'pos', 'orders']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'raw_order_header') }}
    {% if is_incremental() %}
        WHERE order_ts > (SELECT MAX(order_timestamp) FROM {{ this }})
    {% endif %}
),

cleaned_orders AS (
    SELECT
        order_id,
        truck_id,
        location_id,
        customer_id,
        
        -- Clean discount ID
        NULLIF(TRIM(discount_id), '') AS discount_id,
        
        shift_id,
        shift_start_time,
        shift_end_time,
        
        -- Standardize order channel
        CASE 
            WHEN UPPER(TRIM(order_channel)) IN ('MOBILE', 'APP', 'MOBILE APP') THEN 'Mobile App'
            WHEN UPPER(TRIM(order_channel)) IN ('WALK-UP', 'WALKUP', 'WALK_UP', 'WALKIN') THEN 'Walk-up'
            WHEN UPPER(TRIM(order_channel)) IN ('ONLINE', 'WEB', 'WEBSITE') THEN 'Online'
            WHEN UPPER(TRIM(order_channel)) IN ('PHONE', 'CALL') THEN 'Phone'
            WHEN UPPER(TRIM(order_channel)) IN ('KIOSK', 'SELF-SERVICE') THEN 'Kiosk'
            ELSE 'Other'
        END AS order_channel,
        
        -- Parse timestamps (fix timestamp casting - direct assignment for already timestamp fields)
        order_ts AS order_timestamp,
        
        -- Handle served_ts properly (might be string or timestamp)
        CASE 
            WHEN served_ts IS NOT NULL AND TRIM(served_ts::STRING) != ''
            THEN CAST(served_ts AS TIMESTAMP_NTZ)
            ELSE NULL
        END AS served_timestamp,
        
        -- Clean currency
        COALESCE(UPPER(TRIM(order_currency)), 'USD') AS order_currency,
        
        -- Clean amounts (handle string/null values properly)
        COALESCE(order_amount, 0) AS order_amount,
        
        CASE 
            WHEN order_tax_amount IS NOT NULL 
            THEN CAST(order_tax_amount AS DECIMAL(10,4))
            ELSE 0
        END AS order_tax_amount,
        
        CASE 
            WHEN order_discount_amount IS NOT NULL 
            THEN CAST(order_discount_amount AS DECIMAL(10,4))
            ELSE 0
        END AS order_discount_amount,
        
        COALESCE(order_total, 0) AS order_total,
        
        -- Calculate processing time in minutes (fix timestamp handling)
        CASE 
            WHEN served_ts IS NOT NULL 
                AND TRIM(served_ts::STRING) != ''
                AND order_ts IS NOT NULL
            THEN DATEDIFF('minute', order_ts, CAST(served_ts AS TIMESTAMP_NTZ))
            ELSE NULL
        END AS processing_time_minutes,
        
        -- Calculate discount percentage
        CASE 
            WHEN order_amount > 0 AND order_discount_amount IS NOT NULL
            THEN (CAST(order_discount_amount AS DECIMAL(10,4)) / order_amount) * 100
            ELSE 0
        END AS discount_percentage,
        
        -- Add time-based classifications
        EXTRACT(HOUR FROM order_ts) AS order_hour,
        EXTRACT(DOW FROM order_ts) AS order_day_of_week,
        
        -- Meal period classification
        CASE 
            WHEN EXTRACT(HOUR FROM order_ts) BETWEEN 6 AND 10 THEN 'Breakfast'
            WHEN EXTRACT(HOUR FROM order_ts) BETWEEN 11 AND 14 THEN 'Lunch'
            WHEN EXTRACT(HOUR FROM order_ts) BETWEEN 15 AND 17 THEN 'Snack'
            WHEN EXTRACT(HOUR FROM order_ts) BETWEEN 18 AND 21 THEN 'Dinner'
            ELSE 'Late Night'
        END AS meal_period,
        
        CURRENT_TIMESTAMP() AS created_ts,
        CURRENT_TIMESTAMP() AS updated_ts
        
    FROM source_data
    WHERE order_id IS NOT NULL
        AND truck_id IS NOT NULL
        AND order_total > 0
        AND order_ts IS NOT NULL
)

SELECT * FROM cleaned_orders