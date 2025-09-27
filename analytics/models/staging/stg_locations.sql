-- Pseudocode Logic:
-- 1. Source raw location data
-- 2. Standardize location names and addresses
-- 3. Clean geographic fields
-- 4. Add location type classification

{{ config(
    materialized='view',
    tags=['staging']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'raw_location') }}
),

cleaned_locations AS (
    SELECT
        location_id,
        
        -- Clean placekey
        TRIM(placekey) AS placekey,
        
        -- Clean location description
        UPPER(TRIM(location)) AS location_name,
        
        -- Clean geographic fields
        UPPER(TRIM(city)) AS city_name,
        UPPER(TRIM(region)) AS region_name,
        UPPER(TRIM(iso_country_code)) AS iso_country_code,
        UPPER(TRIM(country)) AS country_name,
        
        -- Add location type classification
        multiIf(
            match(UPPER(location), 'PARK'), 'Park',
            match(UPPER(location), '(DOWNTOWN|CITY CENTER)'), 'Downtown',
            match(UPPER(location), '(BUSINESS|OFFICE)'), 'Business District',
            match(UPPER(location), '(UNIVERSITY|COLLEGE)'), 'University',
            match(UPPER(location), '(MARKET|FARMER)'), 'Market',
            match(UPPER(location), '(FESTIVAL|EVENT)'), 'Event',
            'Other'
        ) AS location_type,
        
        -- Add foot traffic estimation
        multiIf(
            match(UPPER(location), '(DOWNTOWN|CITY CENTER)'), 'High',
            match(UPPER(location), '(BUSINESS|UNIVERSITY)'), 'Medium',
            match(UPPER(location), '(PARK|MARKET)'), 'Medium',
            'Low'
        ) AS estimated_foot_traffic,
        
        now() AS created_ts,
        now() AS updated_ts
        
    FROM source_data
    WHERE location_id IS NOT NULL
        AND location IS NOT NULL
        AND TRIM(location) != ''
)

SELECT * FROM cleaned_locations