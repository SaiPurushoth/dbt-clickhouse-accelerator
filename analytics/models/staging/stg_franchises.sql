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
        concat(
            ifNull(UPPER(TRIM(first_name)), ''),
            ' ',
            ifNull(UPPER(TRIM(last_name)), '')
        ) AS franchise_owner_name,
        
        first_name,
        last_name,
        
        -- Clean location fields
        INITCAP(TRIM(city)) AS city_name,
        INITCAP(TRIM(country)) AS country_name,
        
        -- Clean and validate email
        if(match(e_mail, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'),
           LOWER(TRIM(e_mail)),
           NULL) AS email,
        
        -- Clean phone number (remove special characters)
        replaceRegexpAll(
            TRIM(phone_number), 
            '[^0-9+]', 
            ''
        ) AS phone_number,
        
        -- Add validation flags
        (first_name IS NOT NULL AND last_name IS NOT NULL) AS has_complete_name,
        match(e_mail, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') AS has_valid_email,
        
        now() AS created_ts,
        now() AS updated_ts
        
    FROM source_data
    WHERE franchise_id IS NOT NULL
)

SELECT * FROM cleaned_franchises