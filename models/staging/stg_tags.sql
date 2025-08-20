{{
  config(
    materialized='view'
  )
}}

SELECT 
  id,
  name,
  creator_company_mapping_id,
  status,
  created_at,
  updated_at,
  visibility_level_type,
  visibility_level_id,
  user_company_mapping_id
FROM {{ source('staging', 'tags') }}
