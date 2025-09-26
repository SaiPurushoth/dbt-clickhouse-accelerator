-- Pseudocode Logic:
-- 1. Source raw customer loyalty data
-- 2. Clean personal information
-- 3. Standardize demographic data
-- 4. Add customer segmentation attributes

{{ config(
    materialized='view',
    tags=['staging']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'raw_customer_loyalty') }}
),

cleaned_customers AS (
    SELECT
        customer_id,
        
        -- Clean name fields
        INITCAP(TRIM(first_name)) AS first_name,
        INITCAP(TRIM(last_name)) AS last_name,
        
        -- Combine full name
        TRIM(CONCAT(
            COALESCE(INITCAP(TRIM(first_name)), ''),
            ' ',
            COALESCE(INITCAP(TRIM(last_name)), '')
        )) AS customer_name,
        
        -- Clean location fields
        INITCAP(TRIM(city)) AS city_name,
        INITCAP(TRIM(country)) AS country_name,
        UPPER(TRIM(postal_code)) AS postal_code,
        
        -- Standardize language
        CASE 
            WHEN UPPER(TRIM(preferred_language)) IN ('EN', 'ENG', 'ENGLISH') THEN 'English'
            WHEN UPPER(TRIM(preferred_language)) IN ('ES', 'ESP', 'SPANISH') THEN 'Spanish'
            WHEN UPPER(TRIM(preferred_language)) IN ('FR', 'FRA', 'FRENCH') THEN 'French'
            WHEN UPPER(TRIM(preferred_language)) IN ('DE', 'GER', 'GERMAN') THEN 'German'
            WHEN UPPER(TRIM(preferred_language)) IN ('JA', 'JAP', 'JAPANESE') THEN 'Japanese'
            ELSE INITCAP(TRIM(preferred_language))
        END AS preferred_language,
        
        -- Standardize gender
        CASE 
            WHEN UPPER(TRIM(gender)) IN ('M', 'MALE') THEN 'Male'
            WHEN UPPER(TRIM(gender)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gender)) IN ('O', 'OTHER') THEN 'Other'
            WHEN UPPER(TRIM(gender)) IN ('N', 'PREFER NOT TO SAY') THEN 'Prefer not to say'
            ELSE 'Not specified'
        END AS gender,
        
        -- Clean favorite brand
        INITCAP(TRIM(favourite_brand)) AS favourite_brand,
        
        -- Standardize marital status
        CASE 
            WHEN UPPER(TRIM(marital_status)) IN ('S', 'SINGLE') THEN 'Single'
            WHEN UPPER(TRIM(marital_status)) IN ('M', 'MARRIED') THEN 'Married'
            WHEN UPPER(TRIM(marital_status)) IN ('D', 'DIVORCED') THEN 'Divorced'
            WHEN UPPER(TRIM(marital_status)) IN ('W', 'WIDOWED') THEN 'Widowed'
            ELSE 'Not specified'
        END AS marital_status,
        
        -- Clean children count
        COALESCE(TRY_CAST(children_count AS NUMBER), 0) AS children_count,
        
        sign_up_date,
        birthday_date,
        
        -- Clean contact information
        CASE 
            WHEN e_mail RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
            THEN LOWER(TRIM(e_mail))
            ELSE NULL
        END AS email,
        
        REGEXP_REPLACE(TRIM(phone_number), '[^0-9+]', '') AS phone_number,
        
        -- Calculate age
        CASE 
            WHEN birthday_date IS NOT NULL
            THEN DATEDIFF('year', birthday_date, CURRENT_DATE())
            ELSE NULL
        END AS age,
        
        -- Add age group classification
        CASE 
            WHEN DATEDIFF('year', birthday_date, CURRENT_DATE()) < 25 THEN 'Gen Z'
            WHEN DATEDIFF('year', birthday_date, CURRENT_DATE()) < 40 THEN 'Millennial'
            WHEN DATEDIFF('year', birthday_date, CURRENT_DATE()) < 55 THEN 'Gen X'
            WHEN DATEDIFF('year', birthday_date, CURRENT_DATE()) >= 55 THEN 'Boomer+'
            ELSE 'Unknown'
        END AS age_group,
        
        -- Calculate customer tenure
        DATEDIFF('day', sign_up_date, CURRENT_DATE()) AS days_since_signup,
        
        -- Add initial customer segment (will be enhanced in intermediate layer)
        CASE 
            WHEN DATEDIFF('day', sign_up_date, CURRENT_DATE()) <= 30 THEN 'New'
            WHEN DATEDIFF('day', sign_up_date, CURRENT_DATE()) <= 90 THEN 'Recent'
            WHEN DATEDIFF('day', sign_up_date, CURRENT_DATE()) <= 365 THEN 'Established'
            ELSE 'Veteran'
        END AS initial_segment,
        
        -- Data quality flags
        CASE 
            WHEN first_name IS NOT NULL AND last_name IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_complete_name,
        
        CASE 
            WHEN e_mail RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}'
            THEN TRUE
            ELSE FALSE
        END AS has_valid_email,
        
        CASE 
            WHEN birthday_date IS NOT NULL 
            AND birthday_date <= CURRENT_DATE() 
            AND birthday_date >= '1900-01-01'
            THEN TRUE
            ELSE FALSE
        END AS has_valid_birthday,
        
        CURRENT_TIMESTAMP() AS created_ts,
        CURRENT_TIMESTAMP() AS updated_ts
        
    FROM source_data
    WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned_customers