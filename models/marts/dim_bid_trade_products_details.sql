{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'quality_params', 'images', 'remarks', 'price_breakup_json',
      'other_details', 'meta_data', 'template_data', 'created_at', 'updated_at'
    ],
    dist='id',
    sort=['id', 'created_at']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH bid_trade_products_details_base AS (
  SELECT
    id,
    quality_params,
    images,
    remarks,
    price_breakup_json,
    other_details,
    meta_data,
    template_data,
    created_at,
    updated_at
    
  FROM {{ ref('stg_bid_trade_products') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: handles empty tables gracefully
    WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
       OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  quality_params,
  images,
  remarks,
  price_breakup_json,
  other_details,
  meta_data,
  template_data,
  created_at,
  updated_at
FROM bid_trade_products_details_base