{{
  config(
    materialized='view'
  )
}}

SELECT 
  id,
  team_id,
  user_id,
  role,
  status,
  created_at,
  updated_at,
  designation_id
FROM {{ source('staging', 'team_members') }}
