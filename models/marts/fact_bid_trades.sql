{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'trade_request_id', 'bid_id', 'forwarded_by', 'status', 
      'created_at', 'updated_at'
    ],
    dist='id',
    sort=['id', 'created_at', 'trade_request_id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH bid_trades_data AS (
  SELECT 
    id,
    trade_request_id,
    bid_id,
    forwarded_by,
    status,
    created_at,
    updated_at
    
  FROM {{ ref('stg_bid_trades') }}
  
  {% if is_incremental() %}
    -- Incremental filter: only process records updated in the last 30 minutes
    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }}) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  trade_request_id,
  bid_id,
  forwarded_by,
  status,
  created_at,
  updated_at
FROM bid_trades_data