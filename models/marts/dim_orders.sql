-- depends_on: {{ ref('fact_orders') }}

{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'quality_params', 'terms_and_conditions', 'purchase_order', 'price_breakup',
      'po_details', 'template_data', 'order_items_summary', 'other_details',
      'contract_details', 'meta_data'
    ],
    dist='id',
    sort=['id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH orders_base AS (
  SELECT
    id,
    quality_params,
    terms_and_conditions,
    purchase_order,
    price_breakup,
    po_details,
    template_data,
    order_items_summary,
    other_details,
    contract_details,
    meta_data
    
  FROM {{ ref('stg_orders') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: Use parent table's updated_at for synchronization
    WHERE id IN (
      SELECT DISTINCT id FROM {{ ref('stg_orders') }}
      WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ ref('fact_orders') }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
         OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ ref('fact_orders') }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
    )
  {% endif %}
)

SELECT 
  id,
  quality_params,
  terms_and_conditions,
  purchase_order,
  price_breakup,
  po_details,
  template_data,
  order_items_summary,
  other_details,
  contract_details,
  meta_data
FROM orders_base
WHERE id IS NOT NULL