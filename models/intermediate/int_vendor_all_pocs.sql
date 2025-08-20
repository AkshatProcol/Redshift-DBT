{{
  config(
    materialized='ephemeral'
  )
}}

SELECT 
  ucm.company_id,
  u.id as poc_id,
  u.first_name as poc_name,
  u.email as poc_email,
  u.phone as poc_phone,
  u.created_at as poc_created_at,
  ROW_NUMBER() OVER (PARTITION BY ucm.company_id ORDER BY u.created_at) as poc_sequence
FROM {{ ref('stg_user_company_mappings') }} ucm
INNER JOIN {{ ref('stg_users') }} u ON ucm.user_id = u.id
WHERE ucm.company_id IN (SELECT id FROM {{ ref('int_vendor_companies') }})
  AND ucm.status = 1
