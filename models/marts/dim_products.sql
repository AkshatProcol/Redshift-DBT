{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'status', 'category_id', 'company_id', 'configuration_id', 
      'created_at', 'updated_at', 'created_by', 'user_id'
    ],
    dist='id',
    sort=['id', 'category_id', 'company_id', 'created_at']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH products_base AS (
  SELECT
    id,
    status,
    category_id,
    company_id,
    configuration_id,
    created_at,
    updated_at,
    created_by,
    user_id
    
  FROM {{ ref('stg_products') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: handles empty tables gracefully
    WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
       OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  status,
  category_id,
  company_id,
  configuration_id,
  created_at,
  updated_at,
  created_by,
  user_id
FROM products_base
WHERE id IS NOT NULL