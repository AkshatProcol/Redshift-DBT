{{
  config(
    materialized='view'
  )
}}

SELECT 
  id,
  company_id,
  name,
  status,
  created_at,
  updated_at,
  team_type,
  zone_id,
  other_details,
  product_category_ids,
  buyer_hub_ids,
  purchase_group_ids
FROM {{ source('staging', 'teams') }}
