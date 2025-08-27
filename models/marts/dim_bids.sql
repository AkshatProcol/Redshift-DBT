{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'remarks', 'uuid', 'ip_info', 'quality_params', 'terms_and_conditions',
      'trade_credit_discount', 'images', 'other_details', 'bid_product_ids',
      'price_breakup_json', 'template_data', 'meta_data', 'context'
    ],
    dist='id',
    sort=['id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH bids_details_base AS (
  SELECT
    id,
    remarks,
    uuid,
    ip_info,
    quality_params,
    terms_and_conditions,
    trade_credit_discount,
    images,
    other_details,
    bid_product_ids,
    price_breakup_json,
    template_data,
    meta_data,
    context
    
  FROM {{ ref('stg_bids') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: handles empty tables gracefully
    WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
       OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  remarks,
  uuid,
  ip_info,
  quality_params,
  terms_and_conditions,
  trade_credit_discount,
  images,
  other_details,
  bid_product_ids,
  price_breakup_json,
  template_data,
  meta_data,
  context
FROM bids_details_base