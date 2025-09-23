{{ config(materialized='view') }}

select
  event_id,
  event_ts,
  user_id,
  session_id,
  event_type,
  product_id,
  price
from {{ ref('events_seed') }}