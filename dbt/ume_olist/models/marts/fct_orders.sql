with orders as (
  select * from {{ ref('stg_orders') }}
),
items as (
  select * from {{ ref('stg_order_items') }}
)
select
  o.order_id,
  o.customer_id,
  o.order_status,
  o.order_purchase_timestamp,
  sum(i.price) as total_price,
  count(*) as item_count
from orders o
left join items i
  on o.order_id = i.order_id
group by 1,2,3,4
