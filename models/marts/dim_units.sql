{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'name', 'symbol', 'conversion_factor', 'status', 'base_unit_id',
      'company_id', 'user_id', 'created_at', 'updated_at'
    ],
    dist='all',
    sort=['name']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH units_base AS (
  SELECT
    id,
    name,
    symbol,
    conversion_factor,
    status,
    base_unit_id,
    company_id,
    user_id,
    created_at,
    updated_at
    
  FROM {{ ref('stg_units') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: handles empty tables gracefully
    WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
       OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  name,
  symbol,
  conversion_factor,
  status,
  base_unit_id,
  company_id,
  user_id,
  created_at,
  updated_at
FROM units_base