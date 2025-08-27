{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'trade_request_id', 'product_id', 'product_quality_id', 'quantity', 'status',
      'buyer_hub_id', 'product_origins', 'remaining_quantity', 'auction_config', 'unit_type',
      'current_status', 'created_at', 'updated_at', 'price_ceiling', 'price_floor',
      'price', 'gst', 'floor_price', 'ceil_price', 'floor_quantity', 'sheet_order',
      'created_by_user_id', 'unit_id', 'section', 'order_no', 'rank_one_price',
      'rank_one_total_landed_amount', 'product_name', 'product_type', 'product_quality_name',
      'buyer_hub_name', 'unit_name', 'unit_symbol'
    ],
    dist='trade_request_id',
    sort=['id', 'product_id', 'trade_request_id', 'created_at']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH trade_products_enriched AS (
  SELECT
    tp.id,
    tp.trade_request_id,
    tp.product_id,
    tp.product_quality_id,
    tp.quantity,
    tp.status,
    tp.buyer_hub_id,
    tp.product_origins,
    tp.remaining_quantity,
    tp.auction_config,
    tp.unit_type,
    tp.current_status,
    tp.created_at,
    tp.updated_at,
    tp.price_ceiling,
    tp.price_floor,
    tp.price,
    tp.gst,
    tp.floor_price,
    tp.ceil_price,
    tp.floor_quantity,
    tp.sheet_order,
    tp.created_by_user_id,
    tp.unit_id,
    tp.section,
    tp.order_no,
    tp.rank_one_price,
    tp.rank_one_total_landed_amount,
    
    -- Joined descriptive columns
    p.name AS product_name,
    p.product_type AS product_type,
    pq.name AS product_quality_name,
    bh.name AS buyer_hub_name,
    u.name AS unit_name,
    u.symbol AS unit_symbol
    
  FROM {{ ref('stg_trade_products') }} tp
  LEFT JOIN {{ ref('stg_products') }} p
         ON tp.product_id = p.id
  LEFT JOIN {{ ref('stg_product_qualities') }} pq
         ON tp.product_quality_id = pq.id
  LEFT JOIN {{ ref('stg_buyer_hubs') }} bh
         ON tp.buyer_hub_id = bh.id
  LEFT JOIN {{ ref('stg_units') }} u
         ON tp.unit_id = u.id
  
  {% if is_incremental() %}
    -- Smart incremental filter: handles empty tables gracefully
    WHERE tp.updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
       OR tp.created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  trade_request_id,
  product_id,
  product_quality_id,
  quantity,
  status,
  buyer_hub_id,
  product_origins,
  remaining_quantity,
  auction_config,
  unit_type,
  current_status,
  created_at,
  updated_at,
  price_ceiling,
  price_floor,
  price,
  gst,
  floor_price,
  ceil_price,
  floor_quantity,
  sheet_order,
  created_by_user_id,
  unit_id,
  section,
  order_no,
  rank_one_price,
  rank_one_total_landed_amount,
  product_name,
  product_type,
  product_quality_name,
  buyer_hub_name,
  unit_name,
  unit_symbol
FROM trade_products_enriched