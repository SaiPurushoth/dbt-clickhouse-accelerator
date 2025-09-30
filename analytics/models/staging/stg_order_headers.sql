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
        nullIf(TRIM(discount_id), '') AS discount_id,
        
        shift_id,
        shift_start_time,
        shift_end_time,
        
        -- Standardize order channel
        multiIf(
            UPPER(TRIM(order_channel)) IN ('MOBILE', 'APP', 'MOBILE APP'), 'Mobile App',
            UPPER(TRIM(order_channel)) IN ('WALK-UP', 'WALKUP', 'WALK_UP', 'WALKIN'), 'Walk-up',
            UPPER(TRIM(order_channel)) IN ('ONLINE', 'WEB', 'WEBSITE'), 'Online',
            UPPER(TRIM(order_channel)) IN ('PHONE', 'CALL'), 'Phone',
            UPPER(TRIM(order_channel)) IN ('KIOSK', 'SELF-SERVICE'), 'Kiosk',
            'Other'
        ) AS order_channel,
        
        -- Parse timestamps
        order_ts AS order_timestamp,
        served_ts AS served_timestamp,
        
        -- Clean currency and amounts
        ifNull(UPPER(TRIM(order_currency)), 'USD') AS order_currency,
        ifNull(order_amount, 0) AS order_amount,
        ifNull(order_tax_amount, 0) AS order_tax_amount,
        ifNull(order_discount_amount, 0) AS order_discount_amount,
        ifNull(order_total, 0) AS order_total,
        
        -- Calculate processing time in minutes
        if(served_ts IS NOT NULL AND order_ts IS NOT NULL,
           dateDiff('minute', order_ts, served_ts),
           NULL) AS processing_time_minutes,
        
        -- Calculate discount percentage
        if(order_amount > 0 AND order_discount_amount IS NOT NULL,
           (order_discount_amount / order_amount) * 100,
           0) AS discount_percentage,
        
        -- Add time-based classifications
        toHour(order_ts) AS order_hour,
        toDayOfWeek(order_ts) AS order_day_of_week,
        
        -- Meal period classification
        multiIf(
            toHour(order_ts) BETWEEN 6 AND 10, 'Breakfast',
            toHour(order_ts) BETWEEN 11 AND 14, 'Lunch',
            toHour(order_ts) BETWEEN 15 AND 17, 'Snack',
            toHour(order_ts) BETWEEN 18 AND 21, 'Dinner',
            'Late Night'
        ) AS meal_period,
        
        now() AS created_ts,
        now() AS updated_ts
        
    FROM source_data
    WHERE order_id IS NOT NULL
        AND truck_id IS NOT NULL
        AND order_total > 0
        AND order_ts IS NOT NULL
)

SELECT * FROM cleaned_orders