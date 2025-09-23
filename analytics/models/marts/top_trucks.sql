select
truck_id,
model
from {{ ref('stg_truck') }}
order by truck_opening_date desc
limit 5


