-- depends_on: {{ ref('dim_products') }}

{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'name', 'product_type', 'product_code', 'image_url', 'alias',
      'article_code', 'update_remarks', 'audio_url', 'hsn_code',
      'description', 'options', 'misc', 'quality_params', 'origins',
      'terms_and_conditions'
    ],
    dist='id',
    sort=['id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH products_disc_base AS (
  SELECT
    id,
    name,
    product_type,
    product_code,
    image_url,
    alias,
    article_code,
    update_remarks,
    audio_url,
    hsn_code,
    description,
    options,
    misc,
    quality_params,
    origins,
    terms_and_conditions
    
  FROM {{ ref('stg_products') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: Use parent table's updated_at for synchronization
    WHERE id IN (
      SELECT DISTINCT id FROM {{ ref('stg_products') }}
      WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ ref('dim_products') }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
         OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ ref('dim_products') }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
    )
  {% endif %}
)

SELECT 
  id,
  name,
  product_type,
  product_code,
  image_url,
  alias,
  article_code,
  update_remarks,
  audio_url,
  hsn_code,
  description,
  options,
  misc,
  quality_params,
  origins,
  terms_and_conditions
FROM products_disc_base
WHERE id IS NOT NULL