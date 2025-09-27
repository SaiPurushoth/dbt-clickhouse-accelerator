{{ config(
    materialized='table',
    tags=['intermediate']
) }}

WITH menu_base AS (
    SELECT *
    FROM {{ ref('stg_menu') }}
    WHERE has_valid_pricing = 1
),

menu_sales_performance AS (
    SELECT
        od.menu_item_id,

        countDistinct(od.order_id)                               AS orders_containing_item,
        sum(toFloat64(od.quantity))                               AS total_quantity_sold,
        sum(toFloat64(od.net_line_total))                         AS total_revenue,
        avg(toFloat64(od.unit_price))                             AS avg_selling_price,
        sum(toFloat64(od.line_discount_amount))                   AS total_discounts_given,

        -- Recent performance (last 30 days)
        sumIf(toFloat64(od.quantity),      o.order_timestamp >= subtractDays(now(), 30)) AS quantity_sold_30_days,
        sumIf(toFloat64(od.net_line_total),o.order_timestamp >= subtractDays(now(), 30)) AS revenue_30_days,

        -- Performance by channel
        sumIf(toFloat64(od.quantity), o.order_channel = 'Mobile App') AS mobile_sales,
        sumIf(toFloat64(od.quantity), o.order_channel = 'Walk-up')    AS walkin_sales,

        -- Performance by meal period
        sumIf(toFloat64(od.quantity), o.meal_period = 'Breakfast') AS breakfast_sales,
        sumIf(toFloat64(od.quantity), o.meal_period = 'Lunch')     AS lunch_sales,
        sumIf(toFloat64(od.quantity), o.meal_period = 'Dinner')    AS dinner_sales

    FROM {{ ref('stg_order_details') }} od
    JOIN {{ ref('stg_order_headers') }} o
      ON od.order_id = o.order_id
    WHERE od.is_valid_line_item = 1
    GROUP BY od.menu_item_id
),

menu_profitability AS (
    SELECT
        mb.*,

        -- Normalize numeric aggregates to Float64 to avoid Decimal scale clashes downstream
        ifNull(msp.orders_containing_item, 0)                                             AS orders_containing_item,
        ifNull(toFloat64(msp.total_quantity_sold), 0.0)                                   AS total_quantity_sold,
        ifNull(toFloat64(msp.total_revenue), 0.0)                                         AS total_revenue,
        ifNull(toFloat64(msp.avg_selling_price), toFloat64(mb.sale_price_usd))            AS avg_selling_price,
        ifNull(toFloat64(msp.total_discounts_given), 0.0)                                 AS total_discounts_given,
        ifNull(toFloat64(msp.quantity_sold_30_days), 0.0)                                 AS quantity_sold_30_days,
        ifNull(toFloat64(msp.revenue_30_days), 0.0)                                       AS revenue_30_days,

        -- Derived profitability (Float64 everywhere)
        (
          toFloat64(ifNull(msp.total_revenue, 0))
          - toFloat64(ifNull(msp.total_quantity_sold, 0)) * toFloat64(mb.cost_of_goods_usd)
        )                                                                                 AS total_profit,

        if(
          toFloat64(ifNull(msp.total_revenue, 0)) > 0,
          round(
            (
              ( toFloat64(ifNull(msp.total_revenue, 0))
                - toFloat64(ifNull(msp.total_quantity_sold, 0)) * toFloat64(mb.cost_of_goods_usd)
              )
              / toFloat64(ifNull(msp.total_revenue, 0))
            ) * 100.0, 2
          ),
          toFloat64(mb.profit_margin_pct)
        )                                                                                 AS actual_profit_margin_pct,

        -- Performance classifications
        multiIf(
            toFloat64(msp.total_quantity_sold) >= 1000, 'High Performer',
            toFloat64(msp.total_quantity_sold) >= 100,  'Medium Performer',
            toFloat64(msp.total_quantity_sold) >= 10,   'Low Performer',
            'New/Inactive'
        )                                                                                 AS sales_performance_tier,

        multiIf(
            toFloat64(mb.profit_margin_pct) >= 70, 'High Margin',
            toFloat64(mb.profit_margin_pct) >= 50, 'Medium Margin',
            toFloat64(mb.profit_margin_pct) >= 30, 'Low Margin',
            'Unprofitable'
        )                                                                                 AS profitability_tier,

        -- Popularity metrics
        multiIf(
            toFloat64(msp.quantity_sold_30_days) > toFloat64(msp.total_quantity_sold) * 0.3, 'Trending Up',
            toFloat64(msp.quantity_sold_30_days) > toFloat64(msp.total_quantity_sold) * 0.1, 'Stable',
            toFloat64(msp.quantity_sold_30_days) > 0,                                        'Declining',
            'Inactive'
        )                                                                                 AS trend_status,

        -- Channel performance (already Float64 above via sumIf)
        ifNull(toFloat64(msp.mobile_sales), 0.0)                                          AS mobile_sales,
        ifNull(toFloat64(msp.walkin_sales), 0.0)                                          AS walkin_sales,

        -- Meal period analysis
        ifNull(toFloat64(msp.breakfast_sales), 0.0)                                       AS breakfast_sales,
        ifNull(toFloat64(msp.lunch_sales), 0.0)                                           AS lunch_sales,
        ifNull(toFloat64(msp.dinner_sales), 0.0)                                          AS dinner_sales,

        -- Peak meal period (compare Float64s)
        multiIf(
            greatest(
                ifNull(toFloat64(msp.breakfast_sales), 0.0),
                ifNull(toFloat64(msp.lunch_sales), 0.0),
                ifNull(toFloat64(msp.dinner_sales), 0.0)
            ) = ifNull(toFloat64(msp.breakfast_sales), 0.0), 'Breakfast',
            greatest(
                ifNull(toFloat64(msp.breakfast_sales), 0.0),
                ifNull(toFloat64(msp.lunch_sales), 0.0),
                ifNull(toFloat64(msp.dinner_sales), 0.0)
            ) = ifNull(toFloat64(msp.lunch_sales), 0.0),     'Lunch',
            greatest(
                ifNull(toFloat64(msp.breakfast_sales), 0.0),
                ifNull(toFloat64(msp.lunch_sales), 0.0),
                ifNull(toFloat64(msp.dinner_sales), 0.0)
            ) = ifNull(toFloat64(msp.dinner_sales), 0.0),    'Dinner',
            'No Clear Peak'
        )                                                                                 AS peak_meal_period

    FROM menu_base mb
    LEFT JOIN menu_sales_performance msp
      ON mb.menu_item_id = msp.menu_item_id
)

SELECT *
FROM menu_profitability
