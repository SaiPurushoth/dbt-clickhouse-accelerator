-- Pseudocode Logic:
-- 1. Source raw truck data
-- 2. Clean vehicle information
-- 3. Standardize location data
-- 4. Add business classifications

{{ config(
    materialized='view',
    tags=['staging']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'raw_truck') }}
),

cleaned_trucks AS (
    SELECT
        truck_id,
        menu_type_id,
        franchise_id,
        
        -- Clean location fields
        INITCAP(TRIM(primary_city)) AS primary_city,
        INITCAP(TRIM(region)) AS region_name,
        UPPER(TRIM(iso_region)) AS iso_region,
        INITCAP(TRIM(country)) AS country_name,
        UPPER(TRIM(iso_country_code)) AS iso_country_code,
        
        -- Convert franchise flag to boolean
        CASE WHEN franchise_flag = 1 THEN TRUE ELSE FALSE END AS is_franchise,
        
        -- Clean vehicle information
        year AS vehicle_year,
        INITCAP(TRIM(make)) AS vehicle_make,
        INITCAP(TRIM(model)) AS vehicle_model,
        
        -- Convert EV flag to boolean
        CASE WHEN ev_flag = 1 THEN TRUE ELSE FALSE END AS is_electric_vehicle,
        
        truck_opening_date,
        
        -- Add vehicle age category
        CASE 
            WHEN year >= YEAR(CURRENT_DATE()) - 2 THEN 'New'
            WHEN year >= YEAR(CURRENT_DATE()) - 5 THEN 'Standard'
            WHEN year >= YEAR(CURRENT_DATE()) - 10 THEN 'Older'
            ELSE 'Legacy'
        END AS vehicle_age_category,
        
        -- Add operational status
        CASE 
            WHEN truck_opening_date > CURRENT_DATE() THEN 'Planned'
            WHEN truck_opening_date <= CURRENT_DATE() THEN 'Active'
            ELSE 'Unknown'
        END AS operational_status,
        
        -- Calculate days in operation
        CASE 
            WHEN truck_opening_date <= CURRENT_DATE()
            THEN DATEDIFF('day', truck_opening_date, CURRENT_DATE())
            ELSE 0
        END AS days_in_operation,
        
        -- Add truck maturity classification
        CASE 
            WHEN truck_opening_date > CURRENT_DATE() THEN 'Pre-Launch'
            WHEN DATEDIFF('day', truck_opening_date, CURRENT_DATE()) <= 90 THEN 'New Truck'
            WHEN DATEDIFF('day', truck_opening_date, CURRENT_DATE()) <= 365 THEN 'Established'
            ELSE 'Mature'
        END AS truck_maturity,
        
        CURRENT_TIMESTAMP() AS created_ts,
        CURRENT_TIMESTAMP() AS updated_ts
        
    FROM source_data
    WHERE truck_id IS NOT NULL
)

SELECT * FROM cleaned_trucks