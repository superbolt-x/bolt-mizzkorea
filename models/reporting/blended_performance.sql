{{ config (
    alias = target.database + '_blended_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
  
with initial_sho_data as 
  (SELECT *, {{ get_date_parts('date') }}
  FROM {{ source('reporting', 'shopify_daily_sales_by_order_line_item') }}),
  
paid_data as 
    (select channel, date_granularity, date, 
        coalesce(sum(spend),0) as spend, coalesce(sum(impressions),0) as impressions, coalesce(sum(clicks),0) as clicks, coalesce(sum(add_to_cart),0) as add_to_cart,
        coalesce(sum(purchases),0) as paid_purchases, coalesce(sum(revenue),0) as paid_revenue,
        0 as sho_orders, 0 as upt, 0 as sho_revenue
    from 
        (select 'Meta' as channel, date, date_granularity, 
            spend, impressions, link_clicks as clicks, add_to_cart, purchases, revenue
        from {{ source('reporting', 'facebook_ad_performance') }} 
        union all
        select 'Google Ads' as channel, date, date_granularity, 
            spend, impressions, clicks, 0 as add_to_cart, purchases, revenue
        from {{ source('reporting', 'googleads_campaign_performance') }})
    group by 1,2,3),

sho_data as 
    ({%- for date_granularity in date_granularity_list %}   
    select 'Shopify' as channel, '{{date_granularity}}' as date_granularity, {{date_granularity}} as date,
        0 as spend, 0 as impressions, 0 as clicks, 0 as add_to_cart, 0 as paid_purchases, 0 as paid_revenue,
        COUNT(DISTINCT order_id) as sho_orders, COALESCE(SUM(quantity)::float/NULLIF(COUNT(DISTINCT order_id)::float,0),0) as upt, COALESCE(SUM(total_sales),0) as sho_revenue
    from initial_sho_data
    group by 1,2,3
      {% if not loop.last %}UNION ALL
      {% endif %}
    {% endfor %})

select
    channel, date, date_granularity,
    spend, impressions, clicks, add_to_cart, paid_purchases, paid_revenue, sho_orders, upt, sho_revenue
from 
    (select * from paid_data
    union all
    select * from sho_data)
