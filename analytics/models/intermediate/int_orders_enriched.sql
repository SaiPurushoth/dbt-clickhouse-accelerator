-- Pseudocode Logic:
-- 1. Join order headers with order details
-- 2. Add menu item information
-- 3. Calculate order-level aggregations
-- 4. Add business context

{{ config(
    materialized='table',
    tags=['intermediate']
) }}

WITH order_headers AS (
    SELECT * FROM {{ ref('stg_pos_order_headers') }}
),

order_details AS (
    SELECT * FROM {{ ref('stg_pos_order_details') }}
),

menu_items AS (
    SELECT * FROM {{ ref('stg_pos_menu') }}
),

order_detail_aggregated AS (
    SELECT
        order_id,
        COUNT(*) AS line_item_count,
        SUM(quantity) AS total_items_ordered,
        SUM(net_line_total) AS calculated_order_total,
        AVG(unit_price) AS avg_item_price,
        SUM(line_discount_amount) AS total_line_discounts,
        
        -- Category mix
        COUNT(DISTINCT m.item_category) AS category_count,
        LISTAGG(DISTINCT m.item_category, ', ') WITHIN GROUP (ORDER BY m.item_category) AS categories_ordered,
        
        -- Menu item details
        SUM(od.quantity * m.cost_of_goods_usd) AS total_cost_of_goods,
        SUM(od.net_line_total) - SUM(od.quantity * m.cost_of_goods_usd) AS order_profit
        
    FROM order_details od
    LEFT JOIN menu_items m ON od.menu_item_id = m.menu_item_id
    WHERE od.is_valid_line_item = TRUE
    GROUP BY order_id
),

enriched_orders AS (
    SELECT
        oh.*,
        oda.line_item_count,
        oda.total_items_ordered,
        oda.calculated_order_total,
        oda.avg_item_price,
        oda.total_line_discounts,
        oda.category_count,
        oda.categories_ordered,
        oda.total_cost_of_goods,
        oda.order_profit,
        
        -- Calculate profit margin
        CASE 
            WHEN oh.order_total > 0
            THEN (oda.order_profit / oh.order_total) * 100
            ELSE 0
        END AS order_profit_margin_pct,
        
        -- Add order size classification
        CASE 
            WHEN oda.total_items_ordered = 1 THEN 'Single Item'
            WHEN oda.total_items_ordered <= 3 THEN 'Small'
            WHEN oda.total_items_ordered <= 6 THEN 'Medium'
            WHEN oda.total_items_ordered <= 10 THEN 'Large'
            ELSE 'Extra Large'
        END AS order_size_category,
        
        -- Service speed classification
        CASE 
            WHEN oh.processing_time_minutes <= 3 THEN 'Very Fast'
            WHEN oh.processing_time_minutes <= 5 THEN 'Fast'
            WHEN oh.processing_time_minutes <= 8 THEN 'Standard'
            WHEN oh.processing_time_minutes <= 12 THEN 'Slow'
            ELSE 'Very Slow'
        END AS service_speed_category,
        
        -- Revenue classification
        CASE 
            WHEN oh.order_total < 10 THEN 'Low Value'
            WHEN oh.order_total < 25 THEN 'Medium Value'
            WHEN oh.order_total < 50 THEN 'High Value'
            ELSE 'Premium Value'
        END AS revenue_category
        
    FROM order_headers oh
    LEFT JOIN order_detail_aggregated oda ON oh.order_id = oda.order_id
)

SELECT * FROM enriched_orders