-- Pseudocode Logic:
-- 1. Source raw order detail data
-- 2. Clean line item information
-- 3. Validate quantities and pricing
-- 4. Calculate line-level metrics

{{ config(
    materialized='incremental',
    unique_key='order_detail_id',
    on_schema_change='fail',
    tags=['staging', 'pos', 'orders']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'raw_order_detail') }}
    {% if is_incremental() %}
        WHERE order_detail_id > (SELECT MAX(order_detail_id) FROM {{ this }})
    {% endif %}
),

cleaned_order_details AS (
    SELECT
        order_detail_id,
        order_id,
        menu_item_id,
        
        -- Clean discount ID
        NULLIF(TRIM(discount_id), '') AS discount_id,
        
        line_number,
        
        -- Validate quantity
        CASE 
            WHEN quantity > 0 AND quantity <= 99 THEN quantity
            WHEN quantity > 99 THEN 99  -- Cap at reasonable maximum
            ELSE 1  -- Default to 1 if invalid
        END AS quantity,
        
        -- Clean pricing
        COALESCE(unit_price, 0) AS unit_price,
        COALESCE(price, 0) AS line_total,
        COALESCE(TRY_CAST(order_item_discount_amount AS DECIMAL(10,4)), 0) AS line_discount_amount,
        
        -- Calculate derived metrics
        CASE 
            WHEN quantity > 0 AND price > 0
            THEN price / quantity
            ELSE unit_price
        END AS calculated_unit_price,
        
        -- Calculate line discount percentage
        CASE 
            WHEN price > 0 
            THEN (TRY_CAST(order_item_discount_amount AS DECIMAL(10,4)) / price) * 100
            ELSE 0
        END AS line_discount_percentage,
        
        -- Calculate net line total (after discount)
        price - COALESCE(TRY_CAST(order_item_discount_amount AS DECIMAL(10,4)), 0) AS net_line_total,
        
        -- Data quality flags
        CASE 
            WHEN quantity > 0 AND unit_price >= 0 AND price >= 0 
            THEN TRUE
            ELSE FALSE
        END AS is_valid_line_item,
        
        CASE 
            WHEN ABS(price - (quantity * unit_price)) <= 0.01  -- Allow for rounding
            THEN TRUE
            ELSE FALSE
        END AS price_quantity_consistent,
        
        CURRENT_TIMESTAMP() AS created_ts,
        CURRENT_TIMESTAMP() AS updated_ts
        
    FROM source_data
    WHERE order_detail_id IS NOT NULL
        AND order_id IS NOT NULL
        AND menu_item_id IS NOT NULL
        AND quantity > 0
)

SELECT * FROM cleaned_order_details