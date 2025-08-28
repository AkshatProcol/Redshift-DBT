-- depends_on: {{ ref('fact_trade_products') }}

{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'product_options', 'other_details', 'config', 'validations',
      'projected_price_range', 'meta_data', 'product_options_identifier',
      'template_data', 'rank_hash', 'variant_details', 'trade_identifiers',
      'event_score_for_participants', 'event_score_for_buyers', 'auction_analytics_json'
    ],
    dist='id',
    sort=['id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH trade_products_base AS (
  SELECT
    id,
    product_options,
    other_details,
    config,
    validations,
    projected_price_range,
    meta_data,
    product_options_identifier,
    template_data,
    rank_hash,
    variant_details,
    trade_identifiers,
    event_score_for_participants,
    event_score_for_buyers,
    auction_analytics_json
    
  FROM {{ ref('stg_trade_products') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: Use parent table's updated_at for synchronization
    WHERE id IN (
      SELECT DISTINCT id FROM {{ ref('stg_trade_products') }}
      WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ ref('fact_trade_products') }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
         OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ ref('fact_trade_products') }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
    )
  {% endif %}
)

SELECT 
  id,
  product_options,
  other_details,
  config,
  validations,
  projected_price_range,
  meta_data,
  product_options_identifier,
  template_data,
  rank_hash,
  variant_details,
  trade_identifiers,
  event_score_for_participants,
  event_score_for_buyers,
  auction_analytics_json
FROM trade_products_base
WHERE id IS NOT NULL