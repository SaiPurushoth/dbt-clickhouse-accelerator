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
    WHERE has_valid_pricing = 1
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
            WHEN o.order_timestamp >= subtractDays(now(), 30)
            THEN od.quantity 
            ELSE 0 
        END) AS quantity_sold_30_days,
        
        SUM(CASE 
            WHEN o.order_timestamp >= subtractDays(now(), 30)
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
        
    FROM {{ ref('stg_order_details') }} od
    JOIN {{ ref('stg_order_headers') }} o ON od.order_id = o.order_id
    WHERE od.is_valid_line_item = 1
    GROUP BY menu_item_id
),

menu_profitability AS (
    SELECT
        mb.*,
        ifNull(msp.orders_containing_item, 0) AS orders_containing_item,
        ifNull(msp.total_quantity_sold, 0) AS total_quantity_sold,
        ifNull(msp.total_revenue, 0) AS total_revenue,
        ifNull(msp.avg_selling_price, mb.sale_price_usd) AS avg_selling_price,
        ifNull(msp.total_discounts_given, 0) AS total_discounts_given,
        ifNull(msp.quantity_sold_30_days, 0) AS quantity_sold_30_days,
        ifNull(msp.revenue_30_days, 0) AS revenue_30_days,
        
        -- Calculate profitability metrics
        (msp.total_revenue - (msp.total_quantity_sold * mb.cost_of_goods_usd)) AS total_profit,
        
        multiIf(
            msp.total_revenue > 0,
            ((msp.total_revenue - (msp.total_quantity_sold * mb.cost_of_goods_usd)) / msp.total_revenue) * 100,
            mb.profit_margin_pct
        ) AS actual_profit_margin_pct,
        
        -- Performance classifications
        multiIf(
            msp.total_quantity_sold >= 1000, 'High Performer',
            msp.total_quantity_sold >= 100, 'Medium Performer',
            msp.total_quantity_sold >= 10, 'Low Performer',
            'New/Inactive'
        ) AS sales_performance_tier,
        
        multiIf(
            mb.profit_margin_pct >= 70, 'High Margin',
            mb.profit_margin_pct >= 50, 'Medium Margin',
            mb.profit_margin_pct >= 30, 'Low Margin',
            'Unprofitable'
        ) AS profitability_tier,
        
        -- Popularity metrics
        multiIf(
            msp.quantity_sold_30_days > msp.total_quantity_sold * 0.3, 'Trending Up',
            msp.quantity_sold_30_days > msp.total_quantity_sold * 0.1, 'Stable',
            msp.quantity_sold_30_days > 0, 'Declining',
            'Inactive'
        ) AS trend_status,
        
        -- Channel performance
        ifNull(msp.mobile_sales, 0) AS mobile_sales,
        ifNull(msp.walkin_sales, 0) AS walkin_sales,
        
        -- Meal period analysis
        ifNull(msp.breakfast_sales, 0) AS breakfast_sales,
        ifNull(msp.lunch_sales, 0) AS lunch_sales,
        ifNull(msp.dinner_sales, 0) AS dinner_sales,
        
        -- Peak meal period
        multiIf(
            greatest(
                ifNull(msp.breakfast_sales, 0), 
                ifNull(msp.lunch_sales, 0), 
                ifNull(msp.dinner_sales, 0)
            ) = ifNull(msp.breakfast_sales, 0), 'Breakfast',
            greatest(
                ifNull(msp.breakfast_sales, 0), 
                ifNull(msp.lunch_sales, 0), 
                ifNull(msp.dinner_sales, 0)
            ) = ifNull(msp.lunch_sales, 0), 'Lunch',
            greatest(
                ifNull(msp.breakfast_sales, 0), 
                ifNull(msp.lunch_sales, 0), 
                ifNull(msp.dinner_sales, 0)
            ) = ifNull(msp.dinner_sales, 0), 'Dinner',
            'No Clear Peak'
        ) AS peak_meal_period
        
    FROM menu_base mb
    LEFT JOIN menu_sales_performance msp ON mb.menu_item_id = msp.menu_item_id
)

SELECT * FROM menu_profitability