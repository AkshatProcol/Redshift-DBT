{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'title', 'status', 'user_id', 'company_id', 'created_at', 'updated_at',
      'started_at', 'closed_at', 'latest_stage_time', 'ref_id'
    ],
    dist='id',
    sort=['id', 'company_id', 'created_at']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH event_group_base AS (
  SELECT
    id,
    title,
    status,
    user_id,
    company_id,
    created_at,
    updated_at,
    started_at,
    closed_at,
    latest_stage_time,
    ref_id
    
  FROM {{ ref('stg_event_groups') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: handles empty tables gracefully
    WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
       OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  title,
  status,
  user_id,
  company_id,
  created_at,
  updated_at,
  started_at,
  closed_at,
  latest_stage_time,
  ref_id
FROM event_group_base
WHERE id IS NOT NULL