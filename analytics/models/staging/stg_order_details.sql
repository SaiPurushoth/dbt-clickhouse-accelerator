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
        nullIf(TRIM(discount_id), '') AS discount_id,
        
        line_number,
        
        -- Validate quantity
        multiIf(
            quantity > 0 AND quantity <= 99, quantity,
            quantity > 99, 99,  -- Cap at reasonable maximum
            1  -- Default to 1 if invalid
        ) AS quantity,
        
        -- Clean pricing
        ifNull(unit_price, 0) AS unit_price,
        ifNull(price, 0) AS line_total,
        ifNull(toDecimal64(order_item_discount_amount, 4), 0) AS line_discount_amount,
        
        -- Calculate derived metrics
        if(
            quantity > 0 AND price > 0,
            price / quantity,
            unit_price
        ) AS calculated_unit_price,
        
        -- Calculate line discount percentage
        if(
            price > 0,
            (toDecimal64(order_item_discount_amount, 4) / price) * 100,
            0
        ) AS line_discount_percentage,
        
        -- Calculate net line total (after discount)
        price - ifNull(toDecimal64(order_item_discount_amount, 4), 0) AS net_line_total,
        
        -- Data quality flags
        (quantity > 0 AND unit_price >= 0 AND price >= 0) AS is_valid_line_item,
        
        (abs(price - (quantity * unit_price)) <= 0.01) AS price_quantity_consistent,
        
        now() AS created_ts,
        now() AS updated_ts
        
    FROM source_data
    WHERE order_detail_id IS NOT NULL
        AND order_id IS NOT NULL
        AND menu_item_id IS NOT NULL
        AND quantity > 0
)

SELECT * FROM cleaned_order_details