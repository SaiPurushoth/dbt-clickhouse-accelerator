{{ config(
    materialized='table',
    tags=['mart']
) }}

WITH franchise_current AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['franchise_id']) }} AS franchise_sk,
        franchise_id,
        franchise_owner_name,
        first_name,
        last_name,
        city_name,
        country_name,
        email,
        phone_number,
        has_complete_name,
        has_valid_email,
        
        -- Add franchise classification
        CASE 
            WHEN has_complete_name AND has_valid_email THEN 'Complete Profile'
            WHEN has_complete_name THEN 'Basic Profile'
            ELSE 'Incomplete Profile'
        END AS profile_completeness,
        
        TRUE AS is_active,
        created_ts,
        now() AS updated_ts
        
    FROM {{ ref('stg_franchises') }}
    WHERE franchise_id IS NOT NULL
    
    QUALIFY ROW_NUMBER() OVER (PARTITION BY franchise_id ORDER BY updated_ts DESC) = 1
)

SELECT * FROM franchise_current