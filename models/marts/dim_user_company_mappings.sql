{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'user_id', 'company_id', 'status', 'designation_id', 'erp_id',
      'created_at', 'updated_at', 'trade_identifier'
    ],
    dist='all',
    sort=['company_id', 'user_id'],
    interleaved=true
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH user_company_mappings_base AS (
  SELECT
    id,
    user_id,
    company_id,
    status,
    designation_id,
    erp_id,
    created_at,
    updated_at,
    trade_identifier
    
  FROM {{ ref('stg_user_company_mappings') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: handles empty tables gracefully
    WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
       OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  user_id,
  company_id,
  status,
  designation_id,
  erp_id,
  created_at,
  updated_at,
  trade_identifier
FROM user_company_mappings_base