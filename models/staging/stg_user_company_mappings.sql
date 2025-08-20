{{
  config(
    materialized='view'
  )
}}

SELECT 
  id,
  user_id,
  company_id,
  status,
  created_at,
  trade_identifier,
  designation_id,
  erp_id,
  updated_at
FROM {{ source('staging', 'user_company_mappings') }}
