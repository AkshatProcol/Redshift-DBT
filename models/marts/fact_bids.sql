{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'user_id', 'status', 'delivery_type', 'company_id', 'product_sample_id', 'city_id', 'broker_id', 
      'broker_company_id', 'applicable_apmc_product_config_id', 'contract_id', 'session_id', 'approval_status', 
      'lock_version', 'created_at', 'updated_at', 'end_time', 'start_time', 'closed_at', 'placed_at', 
      'negotiable', 'is_multi_product', 'price', 'quantity', 'gst', 'rank_price', 'final_price', 'amount', 
      'score', 'user_first_name', 'user_last_name', 'company_name', 'city_name'
    ]
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH deduplicated_cities AS (
  -- Handle duplicate cities by taking the first occurrence
  SELECT 
    id,
    MAX(name) as name
  FROM {{ ref('stg_cities') }}
  GROUP BY id
),

bids_with_details AS (
  SELECT 
    -- Core bid fields
    b.id,
    b.user_id,
    b.status,
    b.delivery_type,
    b.company_id,
    b.product_sample_id,
    b.city_id,
    b.broker_id,
    b.broker_company_id,
    b.applicable_apmc_product_config_id,
    b.contract_id,
    b.session_id,
    b.approval_status,
    b.lock_version,
    b.created_at,
    b.updated_at,
    b.end_time,
    b.start_time,
    b.closed_at,
    b.placed_at,
    b.negotiable,
    b.is_multi_product,
    b.price,
    b.quantity,
    b.gst,
    b.rank_price,
    b.final_price,
    b.amount,
    b.score,
    
    -- Joined fields from related tables
    u.first_name as user_first_name,
    u.last_name as user_last_name,
    c.name as company_name,
    ci.name as city_name
    
  FROM {{ ref('stg_bids') }} b
  LEFT JOIN {{ ref('stg_users') }} u ON b.user_id = u.id
  LEFT JOIN {{ ref('stg_companies') }} c ON b.company_id = c.id
  LEFT JOIN deduplicated_cities ci ON b.city_id = ci.id
  
  {% if is_incremental() %}
    -- Incremental filter: only process records updated in the last 30 minutes
    WHERE b.updated_at >= (SELECT MAX(updated_at) FROM {{ this }}) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  user_id,
  status,
  delivery_type,
  company_id,
  product_sample_id,
  city_id,
  broker_id,
  broker_company_id,
  applicable_apmc_product_config_id,
  contract_id,
  session_id,
  approval_status,
  lock_version,
  created_at,
  updated_at,
  end_time,
  start_time,
  closed_at,
  placed_at,
  negotiable,
  is_multi_product,
  price,
  quantity,
  gst,
  rank_price,
  final_price,
  amount,
  score,
  user_first_name,
  user_last_name,
  company_name,
  city_name
FROM bids_with_details