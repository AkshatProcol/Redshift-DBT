{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'client_company_id', 'dealing_with_company_id', 'vendor_code', 'status',
      'created_at', 'updated_at', 'invited_by', 'source', 'auto_discount',
      'vrp_code', 'integration_status', 'meta_data', 'dms_upload_status'
    ],
    dist='all',
    sort=['client_company_id', 'vendor_code']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH buyer_seller_mappings_base AS (
  SELECT
    bsc.id,
    bsc.client_company_id,
    bsc.dealing_with_company_id,
    COALESCE(bsc.vendor_code, '') as vendor_code,
    COALESCE(bsc.status, 0) as status,
    bsc.created_at,
    bsc.updated_at,
    COALESCE(bsc.invited_by, 0) as invited_by,
    COALESCE(bsc.source, 0) as source,
    COALESCE(bsc.auto_discount::varchar, '') as auto_discount,
    COALESCE(bsc.vrp_code, '') as vrp_code,
    COALESCE(bsc.integration_status, 0) as integration_status,
    bsc.meta_data,
    COALESCE(bsc.dms_upload_status, 0) as dms_upload_status
  
  FROM {{ ref('stg_buyer_seller_company_mappings') }} bsc

  {% if is_incremental() %}
    -- Only process records updated in the last 30 minutes (CDC window)
    WHERE bsc.updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01'::timestamp) FROM {{ this }})
  {% endif %}
)

SELECT 
  id,
  client_company_id,
  dealing_with_company_id,
  vendor_code,
  status,
  created_at,
  updated_at,
  invited_by,
  source,
  auto_discount,
  vrp_code,
  integration_status,
  meta_data,
  dms_upload_status

FROM buyer_seller_mappings_base

ORDER BY client_company_id, vendor_code