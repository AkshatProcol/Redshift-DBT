-- depends_on: {{ ref('fact_trade_requests') }}

{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'description', 'company_trade_id', 'ref_id', 'ip_info', 'terms_and_conditions',
      'misc', 'product_origins', 'auction_config', 'config', 'customised_audience',
      'auction_rank_hash', 'team_identifier', 'widgets', 'meta_data', 'template_data',
      'validations', 'trade_identifiers', 'acceptance', 'event_score_for_participants',
      'event_score_for_buyers', 'auction_analytics_json'
    ],
    dist='id',
    sort=['id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH trade_requests_disc_base AS (
  SELECT
    id,
    description,
    company_trade_id,
    ref_id,
    ip_info,
    terms_and_conditions,
    misc,
    product_origins,
    auction_config,
    config,
    customised_audience,
    auction_rank_hash,
    team_identifier,
    widgets,
    meta_data,
    template_data,
    validations,
    trade_identifiers,
    acceptance,
    event_score_for_participants,
    event_score_for_buyers,
    auction_analytics_json
    
  FROM {{ ref('stg_trade_requests') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: Use parent table's updated_at for synchronization
    WHERE id IN (
      SELECT DISTINCT id FROM {{ ref('stg_trade_requests') }}
      WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ ref('fact_trade_requests') }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
         OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ ref('fact_trade_requests') }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
    )
  {% endif %}
)

SELECT 
  id,
  description,
  company_trade_id,
  ref_id,
  ip_info,
  terms_and_conditions,
  misc,
  product_origins,
  auction_config,
  config,
  customised_audience,
  auction_rank_hash,
  team_identifier,
  widgets,
  meta_data,
  template_data,
  validations,
  trade_identifiers,
  acceptance,
  event_score_for_participants,
  event_score_for_buyers,
  auction_analytics_json
FROM trade_requests_disc_base
WHERE id IS NOT NULL