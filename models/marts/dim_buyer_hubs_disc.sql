{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='buyer_hub_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'status', 'coordinates', 'location_code', 'location_type', 
      'address', 'zone_id', 'misc', 'created_by', 'user_id'
    ],
    dist='all',
    sort=['buyer_hub_id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH buyer_hubs_disc_base AS (
  SELECT
    bh.id as buyer_hub_id,
    CASE 
      WHEN bh.status = 1 THEN 1
      WHEN bh.status = 0 THEN 0
      ELSE 2
    END as status,
    COALESCE(bh.coordinates, '') as coordinates,
    COALESCE(bh.location_code, '') as location_code,
    COALESCE(bh.location_type, 0) as location_type,
    COALESCE(bh.address, '') as address,
    COALESCE(bh.zone_id, 0) as zone_id,
    COALESCE(bh.misc, '') as misc,
    COALESCE(bh.created_by, 0) as created_by,
    COALESCE(bh.user_id, 0) as user_id
  
  FROM {{ ref('stg_buyer_hubs') }} bh

  {% if is_incremental() %}
    -- Only process records updated in the last 30 minutes (CDC window)
    WHERE bh.updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01'::timestamp) 
                          FROM {{ ref('stg_buyer_hubs') }}
                          WHERE id IN (SELECT buyer_hub_id FROM {{ this }}))
  {% endif %}
)

SELECT 
  buyer_hub_id,
  status,
  coordinates,
  location_code,
  location_type,
  address,
  zone_id,
  misc,
  created_by,
  user_id

FROM buyer_hubs_disc_base

ORDER BY buyer_hub_id