{{ config(materialized='table') }}

with e as (
  select * from {{ ref('stg_events') }}
),
o as (
  select * from {{ ref('stg_orders') }}
)
select
  toDate(coalesce(e.event_ts, o.order_ts)) as dt,
  countIf(e.event_type = 'page_view')      as page_views,
  countIf(e.event_type = 'add_to_cart')    as add_to_cart,
  countIf(e.event_type = 'purchase')       as purchases,
  uniqExactIf(e.user_id, e.event_type='page_view') as unique_visitors,
  sumOrNull(o.order_amount)                as revenue
from e
full outer join o
  on toDate(e.event_ts) = toDate(o.order_ts)
group by dt
order by dt