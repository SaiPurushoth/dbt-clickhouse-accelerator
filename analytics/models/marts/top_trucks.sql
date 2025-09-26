select
truck_id,
menu_type_id
from {{ ref('stg_truck') }}
order by truck_opening_date desc
limit 5


