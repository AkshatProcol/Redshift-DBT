{{
  config(
    materialized='view'
  )
}}

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
FROM {{ source('staging', 'buyer_seller_company_mappings') }}
