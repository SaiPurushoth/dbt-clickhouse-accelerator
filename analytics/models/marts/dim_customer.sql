{{ config(
    materialized='table',
    tags=['mart']
) }}

WITH customer_current AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_sk,
        customer_id,
        customer_name,
        first_name,
        last_name,
        city_name,
        country_name,
        postal_code,
        preferred_language,
        gender,
        favourite_brand,
        marital_status,
        children_count,
        sign_up_date,
        birthday_date,
        email,
        phone_number,
        age,
        age_group,
        days_since_signup,
        customer_segment,
        digital_engagement_level,
        price_sensitivity,
        estimated_lifetime_value,
        is_active_customer,
        
        -- Add customer value tier
        CASE 
            WHEN estimated_lifetime_value >= 500 THEN 'High Value'
            WHEN estimated_lifetime_value >= 200 THEN 'Medium Value'
            WHEN estimated_lifetime_value >= 50 THEN 'Low Value'
            ELSE 'Minimal Value'
        END AS customer_value_tier,
        
        -- Add communication preference
        CASE 
            WHEN digital_engagement_level = 'Digital Native' THEN 'Digital Only'
            WHEN digital_engagement_level = 'Digital Friendly' THEN 'Digital Preferred'
            WHEN digital_engagement_level = 'Mixed Channel' THEN 'Multi-Channel'
            ELSE 'Traditional Preferred'
        END AS communication_preference,
        
        has_complete_name,
        has_valid_email,
        has_valid_birthday,
        TRUE AS is_active,
        created_ts,
        now() AS updated_ts
        
    FROM {{ ref('int_customers_segmented') }}
    WHERE customer_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_ts DESC) = 1
)

SELECT * FROM customer_current