{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'product_id', 'params', 'status', 'created_at', 'updated_at', 
      'name', 'created_by_user_id', 'attachments'
    ],
    dist='product_id',
    sort=['product_id', 'id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH product_qualities_base AS (
  SELECT
    id,
    product_id,
    params,
    status,
    created_at,
    updated_at,
    name,
    created_by_user_id,
    attachments
    
  FROM {{ ref('stg_product_qualities') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: handles empty tables gracefully
    WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
       OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  product_id,
  params,
  status,
  created_at,
  updated_at,
  name,
  created_by_user_id,
  attachments
FROM product_qualities_base
WHERE id IS NOT NULL