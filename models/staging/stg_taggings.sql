{{
  config(
    materialized='view'
  )
}}

SELECT 
  id,
  tag_id,
  taggable_type,
  taggable_id,
  status,
  tagged_by_id,
  created_at,
  updated_at
FROM {{ source('staging', 'taggings') }}
