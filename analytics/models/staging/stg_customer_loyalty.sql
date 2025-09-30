-- Pseudocode Logic:
-- 1. Source raw customer loyalty data
-- 2. Clean personal information
-- 3. Standardize demographic data
-- 4. Add customer segmentation attributes

{{ config(
    materialized='view',
    tags=['staging']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'raw_customer_loyalty') }}
),

cleaned_customers AS (
    SELECT
        customer_id,
        
        -- Clean name fields
        UPPER(TRIM(first_name)) AS first_name,
        UPPER(TRIM(last_name)) AS last_name,
        
        -- Fix concat syntax
        concat(
            ifNull(UPPER(TRIM(first_name)), ''),
            ' ',
            ifNull(UPPER(TRIM(last_name)), '')
        ) AS customer_name,
        
        -- Location fields
        UPPER(TRIM(city)) AS city_name,
        UPPER(TRIM(country)) AS country_name,
        UPPER(TRIM(postal_code)) AS postal_code,
        
        -- Convert CASE to multiIf for better performance
        multiIf(
            UPPER(TRIM(preferred_language)) IN ('EN', 'ENG', 'ENGLISH'), 'English',
            UPPER(TRIM(preferred_language)) IN ('ES', 'ESP', 'SPANISH'), 'Spanish',
            UPPER(TRIM(preferred_language)) IN ('FR', 'FRA', 'FRENCH'), 'French',
            UPPER(TRIM(preferred_language)) IN ('DE', 'GER', 'GERMAN'), 'German',
            UPPER(TRIM(preferred_language)) IN ('JA', 'JAP', 'JAPANESE'), 'Japanese',
            UPPER(TRIM(preferred_language))
        ) AS preferred_language,
        
        multiIf(
            UPPER(TRIM(gender)) IN ('M', 'MALE'), 'Male',
            UPPER(TRIM(gender)) IN ('F', 'FEMALE'), 'Female',
            UPPER(TRIM(gender)) IN ('O', 'OTHER'), 'Other',
            UPPER(TRIM(gender)) IN ('N', 'PREFER NOT TO SAY'), 'Prefer not to say',
            'Not specified'
        ) AS gender,
        
        UPPER(TRIM(favourite_brand)) AS favourite_brand,
        
        multiIf(
            UPPER(TRIM(marital_status)) IN ('S', 'SINGLE'), 'Single',
            UPPER(TRIM(marital_status)) IN ('M', 'MARRIED'), 'Married',
            UPPER(TRIM(marital_status)) IN ('D', 'DIVORCED'), 'Divorced',
            UPPER(TRIM(marital_status)) IN ('W', 'WIDOWED'), 'Widowed',
            'Not specified'
        ) AS marital_status,
        
        children_count,
        sign_up_date,
        birthday_date,
        
        -- Email validation
        if(match(e_mail, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'),
           LOWER(TRIM(e_mail)),
           NULL) AS email,
        
        replaceRegexpAll(TRIM(phone_number), '[^0-9+]', '') AS phone_number,
        
        -- Date calculations
        if(birthday_date IS NOT NULL,
           dateDiff('year', birthday_date, today()),
           NULL) AS age,
        
        multiIf(
            dateDiff('year', birthday_date, today()) < 25, 'Gen Z',
            dateDiff('year', birthday_date, today()) < 40, 'Millennial',
            dateDiff('year', birthday_date, today()) < 55, 'Gen X',
            dateDiff('year', birthday_date, today()) >= 55, 'Boomer+',
            'Unknown'
        ) AS age_group,
        
        dateDiff('day', sign_up_date, today()) AS days_since_signup,
        
        multiIf(
            dateDiff('day', sign_up_date, today()) <= 30, 'New',
            dateDiff('day', sign_up_date, today()) <= 90, 'Recent',
            dateDiff('day', sign_up_date, today()) <= 365, 'Established',
            'Veteran'
        ) AS initial_segment,
        
        -- Boolean flags
        (first_name IS NOT NULL AND last_name IS NOT NULL) AS has_complete_name,
        match(e_mail, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') AS has_valid_email,
        (birthday_date IS NOT NULL 
         AND birthday_date <= today() 
         AND birthday_date >= toDate('1900-01-01')) AS has_valid_birthday,
        
        now() AS created_ts,
        now() AS updated_ts
        
    FROM source_data
    WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned_customers