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
        
        UPPER(TRIM(primary_city)) AS primary_city,
        UPPER(TRIM(region)) AS region_name,
        UPPER(TRIM(iso_region)) AS iso_region,
        UPPER(TRIM(country)) AS country_name,
        UPPER(TRIM(iso_country_code)) AS iso_country_code,
        
        -- Convert flags to 0/1
        franchise_flag AS is_franchise,
        ev_flag AS is_electric_vehicle,
        
        year as vehicle_year,
        UPPER(TRIM(make)) AS vehicle_make,
        UPPER(TRIM(model)) AS vehicle_model,
        truck_opening_date,
        
        -- Vehicle age category
        multiIf(
            year >= year(today()) - 2, 'New',
            year >= year(today()) - 5, 'Standard',
            year >= year(today()) - 10, 'Older',
            'Legacy'
        ) AS vehicle_age_category,
        
        -- Operational status
        multiIf(
            truck_opening_date > today(), 'Planned',
            truck_opening_date <= today(), 'Active',
            'Unknown'
        ) AS operational_status,
        
        -- Days in operation
        if(truck_opening_date <= today(),
           dateDiff('day', truck_opening_date, today()),
           0) AS days_in_operation,
        
        -- Truck maturity
        multiIf(
            truck_opening_date > today(), 'Pre-Launch',
            dateDiff('day', truck_opening_date, today()) <= 90, 'New Truck',
            dateDiff('day', truck_opening_date, today()) <= 365, 'Established',
            'Mature'
        ) AS truck_maturity,
        
        now() AS created_ts,
        now() AS updated_ts
        
    FROM source_data
    WHERE truck_id IS NOT NULL
)

SELECT * FROM cleaned_trucks
