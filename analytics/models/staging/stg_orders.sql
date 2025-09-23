{{ config(materialized='view') }}

select
  order_id,
  order_ts,
  user_id,
  order_amount,
  currency
from {{ ref('orders_seed') }}

 