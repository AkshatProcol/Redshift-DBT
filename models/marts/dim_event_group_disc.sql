-- depends_on: {{ ref('dim_event_group') }}

{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'config', 'attachments', 'terms_and_conditions', 'precomputed_data',
      'lot_config', 'closed_by_id', 'precomputed_data_v1', 'meta_data',
      'description_markdown'
    ],
    dist='id',
    sort=['id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH event_group_disc_base AS (
  SELECT
    id,
    config,
    attachments,
    terms_and_conditions,
    precomputed_data,
    lot_config,
    closed_by_id,
    precomputed_data_v1,
    meta_data,
    description_markdown
    
  FROM {{ ref('stg_event_groups') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: Use parent table's updated_at for synchronization
    WHERE id IN (
      SELECT DISTINCT id FROM {{ ref('stg_event_groups') }}
      WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ ref('dim_event_group') }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
         OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ ref('dim_event_group') }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
    )
  {% endif %}
)

SELECT 
  id,
  config,
  attachments,
  terms_and_conditions,
  precomputed_data,
  lot_config,
  closed_by_id,
  precomputed_data_v1,
  meta_data,
  description_markdown
FROM event_group_disc_base
WHERE id IS NOT NULL