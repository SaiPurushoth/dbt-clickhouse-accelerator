-- Pseudocode Logic:
-- 1. Start with menu items
-- 2. Add sales performance metrics
-- 3. Calculate profitability analytics
-- 4. Add competitive analysis

{{ config(
    materialized='table',
    tags=['intermediate']
) }}

WITH menu_base AS (
    SELECT * FROM {{ ref('stg_menu') }}
    WHERE has_valid_pricing = TRUE
),

menu_sales_performance AS (
    SELECT
        menu_item_id,
        COUNT(DISTINCT od.order_id) AS orders_containing_item,
        SUM(od.quantity) AS total_quantity_sold,
        SUM(od.net_line_total) AS total_revenue,
        AVG(od.unit_price) AS avg_selling_price,
        SUM(od.line_discount_amount) AS total_discounts_given,
        
        -- Recent performance (last 30 days)
        SUM(CASE 
            WHEN o.order_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '30 days' 
            THEN od.quantity 
            ELSE 0 
        END) AS quantity_sold_30_days,
        
        SUM(CASE 
            WHEN o.order_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '30 days' 
            THEN od.net_line_total 
            ELSE 0 
        END) AS revenue_30_days,
        
        -- Performance by channel
        SUM(CASE WHEN o.order_channel = 'Mobile App' THEN od.quantity ELSE 0 END) AS mobile_sales,
        SUM(CASE WHEN o.order_channel = 'Walk-up' THEN od.quantity ELSE 0 END) AS walkin_sales,
        
        -- Performance by meal period
        SUM(CASE WHEN o.meal_period = 'Breakfast' THEN od.quantity ELSE 0 END) AS breakfast_sales,
        SUM(CASE WHEN o.meal_period = 'Lunch' THEN od.quantity ELSE 0 END) AS lunch_sales,
        SUM(CASE WHEN o.meal_period = 'Dinner' THEN od.quantity ELSE 0 END) AS dinner_sales
        
    FROM {{ ref('stg_pos_order_details') }} od
    JOIN {{ ref('stg_pos_order_headers') }} o ON od.order_id = o.order_id
    WHERE od.is_valid_line_item = TRUE
    GROUP BY menu_item_id
),

menu_profitability AS (
    SELECT
        mb.*,
        COALESCE(msp.orders_containing_item, 0) AS orders_containing_item,
        COALESCE(msp.total_quantity_sold, 0) AS total_quantity_sold,
        COALESCE(msp.total_revenue, 0) AS total_revenue,
        COALESCE(msp.avg_selling_price, mb.sale_price_usd) AS avg_selling_price,
        COALESCE(msp.total_discounts_given, 0) AS total_discounts_given,
        COALESCE(msp.quantity_sold_30_days, 0) AS quantity_sold_30_days,
        COALESCE(msp.revenue_30_days, 0) AS revenue_30_days,
        
        -- Calculate profitability metrics
        (msp.total_revenue - (msp.total_quantity_sold * mb.cost_of_goods_usd)) AS total_profit,
        
        CASE 
            WHEN msp.total_revenue > 0
            THEN ((msp.total_revenue - (msp.total_quantity_sold * mb.cost_of_goods_usd)) / msp.total_revenue) * 100
            ELSE mb.profit_margin_pct
        END AS actual_profit_margin_pct,
        
        -- Performance classifications
        CASE 
            WHEN msp.total_quantity_sold >= 1000 THEN 'High Performer'
            WHEN msp.total_quantity_sold >= 100 THEN 'Medium Performer'
            WHEN msp.total_quantity_sold >= 10 THEN 'Low Performer'
            ELSE 'New/Inactive'
        END AS sales_performance_tier,
        
        CASE 
            WHEN mb.profit_margin_pct >= 70 THEN 'High Margin'
            WHEN mb.profit_margin_pct >= 50 THEN 'Medium Margin'
            WHEN mb.profit_margin_pct >= 30 THEN 'Low Margin'
            ELSE 'Unprofitable'
        END AS profitability_tier,
        
        -- Popularity metrics
        CASE 
            WHEN msp.quantity_sold_30_days > msp.total_quantity_sold * 0.3 THEN 'Trending Up'
            WHEN msp.quantity_sold_30_days > msp.total_quantity_sold * 0.1 THEN 'Stable'
            WHEN msp.quantity_sold_30_days > 0 THEN 'Declining'
            ELSE 'Inactive'
        END AS trend_status,
        
        -- Channel performance
        COALESCE(msp.mobile_sales, 0) AS mobile_sales,
        COALESCE(msp.walkin_sales, 0) AS walkin_sales,
        
        -- Meal period analysis
        COALESCE(msp.breakfast_sales, 0) AS breakfast_sales,
        COALESCE(msp.lunch_sales, 0) AS lunch_sales,
        COALESCE(msp.dinner_sales, 0) AS dinner_sales,
        
        -- Peak meal period
        CASE 
            WHEN GREATEST(
                COALESCE(msp.breakfast_sales, 0), 
                COALESCE(msp.lunch_sales, 0), 
                COALESCE(msp.dinner_sales, 0)
            ) = COALESCE(msp.breakfast_sales, 0) THEN 'Breakfast'
            WHEN GREATEST(
                COALESCE(msp.breakfast_sales, 0), 
                COALESCE(msp.lunch_sales, 0), 
                COALESCE(msp.dinner_sales, 0)
            ) = COALESCE(msp.lunch_sales, 0) THEN 'Lunch'
            WHEN GREATEST(
                COALESCE(msp.breakfast_sales, 0), 
                COALESCE(msp.lunch_sales, 0), 
                COALESCE(msp.dinner_sales, 0)
            ) = COALESCE(msp.dinner_sales, 0) THEN 'Dinner'
            ELSE 'No Clear Peak'
        END AS peak_meal_period
        
    FROM menu_base mb
    LEFT JOIN menu_sales_performance msp ON mb.menu_item_id = msp.menu_item_id
)

SELECT * FROM menu_profitability