{{
  config(
    materialized='ephemeral'
  )
}}

SELECT 
  u.id as user_id,
  u.first_name || ' ' || u.last_name as invited_by_name,
  u.email as invited_by_email
FROM {{ ref('stg_users') }} u
WHERE u.id IN (SELECT DISTINCT invited_by FROM {{ ref('int_vendor_mappings') }} WHERE invited_by IS NOT NULL)
