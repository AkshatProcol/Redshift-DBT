{{
  config(
    materialized='view'
  )
}}

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
FROM {{ source('staging', 'product_categories') }}
