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
        INITCAP(TRIM(location)) AS location_name,
        
        -- Clean geographic fields
        INITCAP(TRIM(city)) AS city_name,
        INITCAP(TRIM(region)) AS region_name,
        UPPER(TRIM(iso_country_code)) AS iso_country_code,
        INITCAP(TRIM(country)) AS country_name,
        
        -- Add location type classification
        CASE 
            WHEN UPPER(location) LIKE '%PARK%' THEN 'Park'
            WHEN UPPER(location) LIKE '%DOWNTOWN%' OR UPPER(location) LIKE '%CITY CENTER%' THEN 'Downtown'
            WHEN UPPER(location) LIKE '%BUSINESS%' OR UPPER(location) LIKE '%OFFICE%' THEN 'Business District'
            WHEN UPPER(location) LIKE '%UNIVERSITY%' OR UPPER(location) LIKE '%COLLEGE%' THEN 'University'
            WHEN UPPER(location) LIKE '%MARKET%' OR UPPER(location) LIKE '%FARMER%' THEN 'Market'
            WHEN UPPER(location) LIKE '%FESTIVAL%' OR UPPER(location) LIKE '%EVENT%' THEN 'Event'
            ELSE 'Other'
        END AS location_type,
        
        -- Add foot traffic estimation
        CASE 
            WHEN UPPER(location) LIKE '%DOWNTOWN%' OR UPPER(location) LIKE '%CITY CENTER%' THEN 'High'
            WHEN UPPER(location) LIKE '%BUSINESS%' OR UPPER(location) LIKE '%UNIVERSITY%' THEN 'Medium'
            WHEN UPPER(location) LIKE '%PARK%' OR UPPER(location) LIKE '%MARKET%' THEN 'Medium'
            ELSE 'Low'
        END AS estimated_foot_traffic,
        
        CURRENT_TIMESTAMP() AS created_ts,
        CURRENT_TIMESTAMP() AS updated_ts
        
    FROM source_data
    WHERE location_id IS NOT NULL
        AND location IS NOT NULL
        AND TRIM(location) != ''
)

SELECT * FROM cleaned_locations