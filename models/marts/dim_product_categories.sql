{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'name', 'ancestry', 'status', 'created_at', 'updated_at', 'hierarchy_type',
      'alias', 'origins', 'gst', 'image_url', 'category_type', 'quality_params',
      'company_id', 'is_default_category', 'category_code'
    ],
    dist='all',
    sort=['id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH product_categories_base AS (
  SELECT
    id,
    name,
    ancestry,
    status,
    created_at,
    updated_at,
    hierarchy_type,
    alias,
    origins,
    gst,
    image_url,
    category_type,
    quality_params,
    company_id,
    is_default_category,
    category_code
    
  FROM {{ ref('stg_product_categories') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: handles empty tables gracefully
    WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
       OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  name,
  ancestry,
  status,
  created_at,
  updated_at,
  hierarchy_type,
  alias,
  origins,
  gst,
  image_url,
  category_type,
  quality_params,
  company_id,
  is_default_category,
  category_code
FROM product_categories_base
WHERE id IS NOT NULL