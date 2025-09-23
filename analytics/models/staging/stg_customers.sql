{{ config(materialized='view') }}

select
customer_id, 
first_name,
last_name,  
email,
phone,     
signup_date,   
country, 
state,   
city,    
zip,   
gender      
from {{ source('raw', 'customer_seeds') }}