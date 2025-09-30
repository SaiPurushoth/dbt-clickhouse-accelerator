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
        multiIf(
            TRIM(UPPER(country)) = 'UNITED STATES', 'United States',
            TRIM(UPPER(country)) = 'UK', 'United Kingdom',
            UPPER(TRIM(country))
        ) AS country_name,
        
        -- Validate and clean ISO codes
        if(LENGTH(TRIM(iso_currency)) = 3,
           UPPER(TRIM(iso_currency)),
           'USD') AS iso_currency,
        
        if(LENGTH(TRIM(iso_country)) = 2,
           UPPER(TRIM(iso_country)),
           NULL) AS iso_country,
        
        -- Add region mapping
        multiIf(
            iso_country IN ('US', 'CA'), 'North America',
            iso_country IN ('GB', 'FR', 'PL'), 'Europe',
            iso_country IN ('IN', 'JP', 'KR'), 'Asia Pacific',
            iso_country = 'AU', 'Australia',
            'Other'
        ) AS region_name,
        
        -- Data quality flags
        (country IS NOT NULL AND TRIM(country) != '' AND iso_country IS NOT NULL) AS is_valid_record,
        
        now() AS created_ts,
        now() AS updated_ts
        
    FROM source_data
    WHERE country_id IS NOT NULL
)

SELECT * FROM cleaned_countries
WHERE is_valid_record = 1