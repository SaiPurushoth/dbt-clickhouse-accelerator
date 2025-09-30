{{ config(
    materialized='table',
    tags=['mart']
) }}

WITH truck_with_franchise AS (
    SELECT
        t.*,
        f.franchise_sk
    FROM {{ ref('stg_trucks') }} t
    LEFT JOIN {{ ref('dim_franchise') }} f 
        ON t.franchise_id = f.franchise_id
        AND t.is_franchise = TRUE
),

truck_current AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['truck_id']) }} AS truck_sk,
        truck_id,
        menu_type_id,
        franchise_sk,
        primary_city,
        region_name,
        country_name,
        iso_country_code,
        is_franchise,
        vehicle_year,
        vehicle_make,
        vehicle_model,
        is_electric_vehicle,
        truck_opening_date,
        vehicle_age_category,
        operational_status,
        days_in_operation,
        truck_maturity,
        
        -- Add sustainability score
        CASE 
            WHEN is_electric_vehicle = TRUE THEN 100
            -- Adjusted: Use ClickHouse native functions toYear(today())
            WHEN vehicle_year >= toYear(today()) - 3 THEN 80
            WHEN vehicle_year >= toYear(today()) - 7 THEN 60
            ELSE 40
        END AS sustainability_score,
        
        -- Add operational efficiency category
        CASE 
            WHEN days_in_operation >= 365 AND operational_status = 'Active' THEN 'Established Performer'
            WHEN days_in_operation >= 90 AND operational_status = 'Active' THEN 'Growing Operation'
            WHEN days_in_operation > 0 AND operational_status = 'Active' THEN 'New Launch'
            WHEN operational_status = 'Planned' THEN 'Pre-Launch'
            ELSE 'Inactive'
        END AS operational_category,
        
        operational_status AS truck_status,
        TRUE AS is_active,
        created_ts,
        now() AS updated_ts
        
    FROM truck_with_franchise
    WHERE truck_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY truck_id ORDER BY updated_ts DESC) = 1
)

SELECT * FROM truck_current