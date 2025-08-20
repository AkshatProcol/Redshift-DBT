{{
  config(
    materialized='view'
  )
}}

SELECT 
  id,
  item_type,
  item_id,
  company_id,
  status,
  created_at,
  updated_at
FROM {{ source('staging', 'preferred_vendor_item_mappings') }}
