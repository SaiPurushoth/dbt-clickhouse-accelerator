-- Pseudocode Logic:
-- 1. Source raw country data
-- 2. Clean and standardize country names
-- 3. Validate ISO codes
-- 4. Add data quality flags

{{ config(
    materialized='view',
    tags=['staging']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'raw_country') }}
),

cleaned_countries AS (
    SELECT
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['country_id']) }} AS country_sk,
        country_id,
        city_id,
        city as city_name,
        city_population as population,
        
        -- Clean country name
        CASE 
            WHEN TRIM(UPPER(country)) = 'UNITED STATES' THEN 'United States'
            WHEN TRIM(UPPER(country)) = 'UK' THEN 'United Kingdom'
            ELSE INITCAP(TRIM(country))
        END AS country_name,
        
        -- Validate and clean ISO codes
        CASE 
            WHEN LENGTH(TRIM(iso_currency)) = 3 THEN UPPER(TRIM(iso_currency))
            ELSE 'USD'  -- Default currency
        END AS iso_currency,
        
        CASE 
            WHEN LENGTH(TRIM(iso_country)) = 2 THEN UPPER(TRIM(iso_country))
            ELSE NULL
        END AS iso_country,
        
        -- Add region mapping
        CASE 
            WHEN iso_country IN ('US', 'CA') THEN 'North America'
            WHEN iso_country IN ('GB', 'FR', 'PL') THEN 'Europe'
            WHEN iso_country IN ('IN', 'JP', 'KR') THEN 'Asia Pacific'
            WHEN iso_country = 'AU' THEN 'Australia'
            ELSE 'Other'
        END AS region_name,
        
        -- Data quality flags
        CASE 
            WHEN country IS NULL OR TRIM(country) = '' THEN FALSE
            WHEN iso_country IS NULL THEN FALSE
            ELSE TRUE
        END AS is_valid_record,
        
        CURRENT_TIMESTAMP() AS created_ts,
        CURRENT_TIMESTAMP() AS updated_ts
        
    FROM source_data
    WHERE country_id IS NOT NULL
)

SELECT * FROM cleaned_countries
WHERE is_valid_record = TRUE