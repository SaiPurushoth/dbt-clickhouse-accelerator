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
        MIN(toDate(order_timestamp)) AS first_order_date,
        MAX(toDate(order_timestamp)) AS last_order_date,
        
        -- Calculate frequency metrics
        multiIf(
            COUNT(DISTINCT order_id) > 1,
            dateDiff('day', MIN(toDate(order_timestamp)), MAX(toDate(order_timestamp))) / (COUNT(DISTINCT order_id) - 1),
            NULL
        ) AS avg_days_between_orders,
        
        dateDiff('day', MAX(toDate(order_timestamp)), today()) AS days_since_last_order,
        
        -- Behavioral patterns
        countDistinct(if(order_channel = 'Mobile App', order_id, NULL)) AS mobile_orders,
        countDistinct(if(meal_period = 'Lunch', order_id, NULL)) AS lunch_orders,
        countDistinct(if(discount_percentage > 0, order_id, NULL)) AS discounted_orders,
        
        -- Recent activity (last 90 days)
        countDistinct(if(
            toDate(order_timestamp) >= subtractDays(today(), 90),
            order_id,
            NULL
        )) AS orders_last_90_days,
        
        SUM(if(
            toDate(order_timestamp) >= subtractDays(today(), 90),
            order_total,
            0
        )) AS spent_last_90_days
        
    FROM {{ ref('int_orders_enriched') }}
    GROUP BY customer_id
),

segmented_customers AS (
    SELECT
        cb.*,
        ifNull(coh.total_orders, 0) AS total_orders,
        ifNull(coh.total_spent, 0) AS total_spent,
        ifNull(coh.avg_order_value, 0) AS avg_order_value,
        coh.first_order_date,
        coh.last_order_date,
        coh.avg_days_between_orders,
        ifNull(coh.days_since_last_order, 9999) AS days_since_last_order,
        
        -- Behavioral metrics
        ifNull(coh.mobile_orders, 0) AS mobile_orders,
        ifNull(coh.lunch_orders, 0) AS lunch_orders,
        ifNull(coh.discounted_orders, 0) AS discounted_orders,
        ifNull(coh.orders_last_90_days, 0) AS orders_last_90_days,
        ifNull(coh.spent_last_90_days, 0) AS spent_last_90_days,
        
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
            WHEN coh.days_since_last_order <= 30 
                AND coh.total_orders >= 10 
                AND coh.total_spent >= 200 
            THEN 'VIP'
            WHEN coh.total_orders >= 5 
                AND coh.total_spent >= 100 
                AND coh.days_since_last_order <= 60 
            THEN 'Loyal'
            WHEN coh.total_orders >= 3 
                AND coh.days_since_last_order <= 90 
            THEN 'Regular'
            WHEN coh.total_orders >= 3 
                AND coh.total_spent >= 50 
                AND coh.days_since_last_order > 90 
            THEN 'At Risk'
            WHEN cb.days_since_signup <= 30 
                OR coh.total_orders <= 2 
            THEN 'New'
            WHEN coh.days_since_last_order > 180 
            THEN 'Churned'
            ELSE 'Occasional'
        END AS customer_segment,
        
        -- Digital engagement level
        CASE 
            WHEN ifNull(coh.mobile_orders, 0) * 100.0 / nullIf(coh.total_orders, 0) >= 80 
            THEN 'Digital Native'
            WHEN ifNull(coh.mobile_orders, 0) * 100.0 / nullIf(coh.total_orders, 0) >= 50 
            THEN 'Digital Friendly'
            WHEN ifNull(coh.mobile_orders, 0) * 100.0 / nullIf(coh.total_orders, 0) >= 20 
            THEN 'Mixed Channel'
            ELSE 'Traditional'
        END AS digital_engagement_level,
        
        -- Price sensitivity
        CASE 
            WHEN ifNull(coh.discounted_orders, 0) * 100.0 / nullIf(coh.total_orders, 0) >= 50 
            THEN 'High Price Sensitivity'
            WHEN ifNull(coh.discounted_orders, 0) * 100.0 / nullIf(coh.total_orders, 0) >= 25 
            THEN 'Moderate Price Sensitivity'
            ELSE 'Low Price Sensitivity'
        END AS price_sensitivity,
        
        -- Activity status
        coh.orders_last_90_days > 0 AS is_active_customer
        
    FROM customer_base cb
    LEFT JOIN customer_order_history coh ON cb.customer_id = coh.customer_id
)

SELECT * FROM segmented_customers