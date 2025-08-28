{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'buyer_hub_id', 'name', 'city_id', 'company_id', 'company_name',
      'created_at', 'updated_at'
    ],
    dist='all',
    sort=['created_at']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH buyer_hubs_base AS (
  SELECT
    bh.id,
    bh.id as buyer_hub_id,  -- Using id as buyer_hub_id to match your schema
    bh.name,
    bh.city_id,
    bh.company_id,
    COALESCE(comp.name, 'Unknown Company') as company_name,
    bh.created_at,
    bh.updated_at
  
  FROM {{ ref('stg_buyer_hubs') }} bh
  
  -- Join with companies for company_name  
  LEFT JOIN {{ ref('stg_companies') }} comp
    ON bh.company_id = comp.id

  {% if is_incremental() %}
    -- Only process records updated in the last 30 minutes (CDC window)
    WHERE bh.updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01'::timestamp) FROM {{ this }})
  {% endif %}
)

SELECT 
  id,
  buyer_hub_id,
  name,
  city_id,
  company_id,
  company_name,
  created_at,
  updated_at

FROM buyer_hubs_base

ORDER BY created_at DESC