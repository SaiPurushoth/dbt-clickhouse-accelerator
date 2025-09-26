-- Pseudocode Logic:
-- 1. Source raw franchise data
-- 2. Combine first and last names
-- 3. Standardize contact information
-- 4. Validate email and phone formats

{{ config(
    materialized='view',
    tags=['staging']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'raw_franchise') }}
),

cleaned_franchises AS (
    SELECT
        franchise_id,
        
        -- Combine and clean names
        TRIM(CONCAT(
            COALESCE(INITCAP(TRIM(first_name)), ''),
            ' ',
            COALESCE(INITCAP(TRIM(last_name)), '')
        )) AS franchise_owner_name,
        
        first_name,
        last_name,
        
        -- Clean location fields
        INITCAP(TRIM(city)) AS city_name,
        INITCAP(TRIM(country)) AS country_name,
        
        -- Clean and validate email
        CASE 
            WHEN e_mail RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
            THEN LOWER(TRIM(e_mail))
            ELSE NULL
        END AS email,
        
        -- Clean phone number (remove special characters)
        REGEXP_REPLACE(
            TRIM(phone_number), 
            '[^0-9+]', 
            ''
        ) AS phone_number,
        
        -- Add validation flags
        CASE 
            WHEN first_name IS NOT NULL AND last_name IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_complete_name,
        
        CASE 
            WHEN e_mail RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
            THEN TRUE
            ELSE FALSE
        END AS has_valid_email,
        
        CURRENT_TIMESTAMP() AS created_ts,
        CURRENT_TIMESTAMP() AS updated_ts
        
    FROM source_data
    WHERE franchise_id IS NOT NULL
)

SELECT * FROM cleaned_franchises