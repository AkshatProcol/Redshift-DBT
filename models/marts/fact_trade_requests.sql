{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'user_id', 'user_first_name', 'user_last_name', 'product_id', 'product_name',
      'product_type', 'buyer_hub_id', 'buyer_hub_name', 'buyer_hub_location',
      'company_id', 'company_name', 'company_category', 'city_id', 'city_name',
      'event_group_id', 'event_group_title', 'quantity', 'deal_closing_price',
      'deal_closing_quantity', 'remaining_quantity', 'number_of_products',
      'rank_one_gross_total', 'bid_start_time', 'bid_end_time', 'created_at',
      'updated_at', 'closed_at', 'extra_closing_time', 'order_type', 'status',
      'unit_type', 'session_id', 'rfx_mode', 'current_status', 'stage_no',
      'approval_status', 'tender', 'is_price_increase_approval_required',
      'is_demo_event', 'broker_company_id', 'provisional_contracts_id',
      'template_id', 'purchase_request_id'
    ],
    dist='id',
    sort=['id', 'company_id', 'created_at']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH trade_requests_enriched AS (
  SELECT
    tr.id,
    tr.user_id,
    tr.product_id,
    tr.buyer_hub_id,
    tr.company_id,
    tr.city_id,
    tr.event_group_id,
    tr.quantity,
    tr.deal_closing_price,
    tr.deal_closing_quantity,
    tr.remaining_quantity,
    tr.number_of_products,
    tr.rank_one_gross_total,
    tr.bid_start_time,
    tr.bid_end_time,
    tr.created_at,
    tr.updated_at,
    tr.closed_at,
    tr.extra_closing_time,
    tr.order_type,
    tr.status,
    tr.unit_type,
    tr.session_id,
    tr.rfx_mode,
    tr.current_status,
    tr.stage_no,
    tr.approval_status,
    tr.tender,
    tr.is_price_increase_approval_required,
    tr.is_demo_event,
    tr.broker_company_id,
    tr.provisional_contracts_id,
    tr.template_id,
    tr.purchase_request_id,
    
    -- User enrichment
    u.first_name AS user_first_name,
    u.last_name AS user_last_name,
    
    -- Product enrichment
    p.name AS product_name,
    p.product_type,
    
    -- Buyer hub enrichment
    bh.name AS buyer_hub_name,
    bh.address AS buyer_hub_location,
    
    -- Company enrichment
    c.name AS company_name,
    CASE 
      WHEN c.category = 1 THEN 'Broker Created Firm'
      WHEN c.category = 2 THEN 'Broker Firm'
      WHEN c.category = 3 THEN 'Buying Firm'
      WHEN c.category = 4 THEN 'Selling Firm'
      ELSE 'Other'
    END AS company_category,
    
    -- City enrichment
    city.name AS city_name,
    
    -- Event group enrichment
    eg.title AS event_group_title
    
  FROM {{ ref('stg_trade_requests') }} tr
  LEFT JOIN {{ ref('stg_users') }} u
         ON tr.user_id = u.id
  LEFT JOIN {{ ref('stg_products') }} p
         ON tr.product_id = p.id
  LEFT JOIN {{ ref('stg_buyer_hubs') }} bh
         ON tr.buyer_hub_id = bh.id
  LEFT JOIN {{ ref('stg_companies') }} c
         ON tr.company_id = c.id
  LEFT JOIN {{ ref('stg_cities') }} city
         ON tr.city_id = city.id
  LEFT JOIN {{ ref('stg_event_groups') }} eg
         ON tr.event_group_id = eg.id
  
  {% if is_incremental() %}
    -- Smart incremental filter: handles empty tables gracefully
    WHERE tr.updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
       OR tr.created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  user_id,
  user_first_name,
  user_last_name,
  product_id,
  product_name,
  product_type,
  buyer_hub_id,
  buyer_hub_name,
  buyer_hub_location,
  company_id,
  company_name,
  company_category,
  city_id,
  city_name,
  event_group_id,
  event_group_title,
  quantity,
  deal_closing_price,
  deal_closing_quantity,
  remaining_quantity,
  number_of_products,
  rank_one_gross_total,
  bid_start_time,
  bid_end_time,
  created_at,
  updated_at,
  closed_at,
  extra_closing_time,
  order_type,
  status,
  unit_type,
  session_id,
  rfx_mode,
  current_status,
  stage_no,
  approval_status,
  tender,
  is_price_increase_approval_required,
  is_demo_event,
  broker_company_id,
  provisional_contracts_id,
  template_id,
  purchase_request_id
FROM trade_requests_enriched