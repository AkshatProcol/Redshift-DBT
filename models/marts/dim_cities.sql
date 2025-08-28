{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'name', 'coordinates', 'status', 'apmc_configuration_id',
      'state_id', 'country_id', 'created_at', 'updated_at'
    ],
    dist='all',
    sort=['id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH cities_base AS (
  SELECT
    id,
    name,
    coordinates,
    status,
    apmc_configuration_id,
    state_id,
    country_id,
    created_at,
    updated_at
    
  FROM {{ ref('stg_cities') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: handles empty tables gracefully
    WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
       OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  name,
  coordinates,
  status,
  apmc_configuration_id,
  state_id,
  country_id,
  created_at,
  updated_at
FROM cities_base
WHERE id IS NOT NULL