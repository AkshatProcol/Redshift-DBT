{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'item_type', 'item_id', 'audience_id', 'status', 'forwarded_by',
      'created_at', 'updated_at', 'visible', 'source', 'audience_type',
      'meta_data'
    ],
    dist='audience_id',
    sort=['audience_id', 'created_at']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH audiences_base AS (
  SELECT
    id,
    item_type,
    item_id,
    audience_id,
    status,
    forwarded_by,
    created_at,
    updated_at,
    visible,
    source,
    audience_type,
    meta_data
    
  FROM {{ ref('stg_audiences') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: handles empty tables gracefully
    WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
       OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  item_type,
  item_id,
  audience_id,
  status,
  forwarded_by,
  created_at,
  updated_at,
  visible,
  source,
  audience_type,
  meta_data
FROM audiences_base