select
customer_id,
signup_date
from {{ ref('stg_customers') }}
order by signup_date desc
limit 5


