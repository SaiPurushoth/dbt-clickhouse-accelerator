{{ config(
    materialized='table',
    tags=['mart']
) }}

WITH country_current AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['country_id']) }} AS country_sk,
        country_id,
        country_name,
        iso_currency,
        iso_country,
        region_name,
        TRUE AS is_active,
        created_ts,
        now() AS updated_ts
        
    FROM {{ ref('stg_countries') }}
    

    QUALIFY ROW_NUMBER() OVER (PARTITION BY country_id ORDER BY updated_ts DESC) = 1
)

SELECT * FROM country_current