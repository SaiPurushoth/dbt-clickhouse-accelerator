-- Pseudocode Logic:
-- 1. Start with cleaned customer data
-- 2. Add order history and behavior metrics
-- 3. Calculate customer lifetime value
-- 4. Apply advanced segmentation logic

{{ config(
    materialized='table',
    tags=['intermediate']
) }}

WITH customer_base AS (
    SELECT * FROM {{ ref('stg_customer_loyalty') }}
),

customer_order_history AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(order_total) AS total_spent,
        AVG(order_total) AS avg_order_value,
        MIN(DATE(order_timestamp)) AS first_order_date,
        MAX(DATE(order_timestamp)) AS last_order_date,
        
        -- Calculate frequency metrics
        CASE 
            WHEN COUNT(DISTINCT order_id) > 1
            THEN DATEDIFF('day', MIN(DATE(order_timestamp)), MAX(DATE(order_timestamp))) / (COUNT(DISTINCT order_id) - 1)
            ELSE NULL
        END AS avg_days_between_orders,
        
        DATEDIFF('day', MAX(DATE(order_timestamp)), CURRENT_DATE()) AS days_since_last_order,
        
        -- Behavioral patterns
        COUNT(DISTINCT CASE WHEN order_channel = 'Mobile App' THEN order_id END) AS mobile_orders,
        COUNT(DISTINCT CASE WHEN meal_period = 'Lunch' THEN order_id END) AS lunch_orders,
        COUNT(DISTINCT CASE WHEN discount_percentage > 0 THEN order_id END) AS discounted_orders,
        
        -- Recent activity (last 90 days)
        COUNT(DISTINCT CASE 
            WHEN DATE(order_timestamp) >= CURRENT_DATE() - 90 
            THEN order_id 
        END) AS orders_last_90_days,
        
        SUM(CASE 
            WHEN DATE(order_timestamp) >= CURRENT_DATE() - 90 
            THEN order_total 
            ELSE 0 
        END) AS spent_last_90_days
        
    FROM {{ ref('int_orders_enriched') }}
    GROUP BY customer_id
),

segmented_customers AS (
    SELECT
        cb.*,
        COALESCE(coh.total_orders, 0) AS total_orders,
        COALESCE(coh.total_spent, 0) AS total_spent,
        COALESCE(coh.avg_order_value, 0) AS avg_order_value,
        coh.first_order_date,
        coh.last_order_date,
        coh.avg_days_between_orders,
        COALESCE(coh.days_since_last_order, 9999) AS days_since_last_order,
        
        -- Behavioral metrics
        COALESCE(coh.mobile_orders, 0) AS mobile_orders,
        COALESCE(coh.lunch_orders, 0) AS lunch_orders,
        COALESCE(coh.discounted_orders, 0) AS discounted_orders,
        COALESCE(coh.orders_last_90_days, 0) AS orders_last_90_days,
        COALESCE(coh.spent_last_90_days, 0) AS spent_last_90_days,
        
        -- Calculate customer lifetime value
        CASE 
            WHEN coh.avg_days_between_orders IS NOT NULL AND coh.avg_days_between_orders > 0
            THEN (coh.avg_order_value * (365 / coh.avg_days_between_orders)) * 2
            WHEN coh.total_orders > 0
            THEN coh.total_spent * 1.5
            ELSE 0
        END AS estimated_lifetime_value,
        
        -- Advanced customer segmentation using RFM methodology
        CASE 
            -- VIP customers: High recency, frequency, monetary
            WHEN coh.days_since_last_order <= 30 
                AND coh.total_orders >= 10 
                AND coh.total_spent >= 200 
            THEN 'VIP'
            
            -- Loyal customers: Good frequency and monetary, may have moderate recency
            WHEN coh.total_orders >= 5 
                AND coh.total_spent >= 100 
                AND coh.days_since_last_order <= 60 
            THEN 'Loyal'
            
            -- Regular customers: Consistent but moderate activity
            WHEN coh.total_orders >= 3 
                AND coh.days_since_last_order <= 90 
            THEN 'Regular'
            
            -- At-risk customers: Good history but recent decline
            WHEN coh.total_orders >= 3 
                AND coh.total_spent >= 50 
                AND coh.days_since_last_order > 90 
            THEN 'At Risk'
            
            -- New customers: Recent signup or first purchase
            WHEN cb.days_since_signup <= 30 
                OR coh.total_orders <= 2 
            THEN 'New'
            
            -- Churned customers: No recent activity
            WHEN coh.days_since_last_order > 180 
            THEN 'Churned'
            
            ELSE 'Occasional'
        END AS customer_segment,
        
        -- Digital engagement level
        CASE 
            WHEN COALESCE(coh.mobile_orders, 0) * 100.0 / NULLIF(coh.total_orders, 0) >= 80 
            THEN 'Digital Native'
            WHEN COALESCE(coh.mobile_orders, 0) * 100.0 / NULLIF(coh.total_orders, 0) >= 50 
            THEN 'Digital Friendly'
            WHEN COALESCE(coh.mobile_orders, 0) * 100.0 / NULLIF(coh.total_orders, 0) >= 20 
            THEN 'Mixed Channel'
            ELSE 'Traditional'
        END AS digital_engagement_level,
        
        -- Price sensitivity
        CASE 
            WHEN COALESCE(coh.discounted_orders, 0) * 100.0 / NULLIF(coh.total_orders, 0) >= 50 
            THEN 'High Price Sensitivity'
            WHEN COALESCE(coh.discounted_orders, 0) * 100.0 / NULLIF(coh.total_orders, 0) >= 25 
            THEN 'Moderate Price Sensitivity'
            ELSE 'Low Price Sensitivity'
        END AS price_sensitivity,
        
        -- Activity status
        CASE 
            WHEN coh.orders_last_90_days > 0 THEN TRUE
            ELSE FALSE
        END AS is_active_customer
        
    FROM customer_base cb
    LEFT JOIN customer_order_history coh ON cb.customer_id = coh.customer_id
)

SELECT * FROM segmented_customers