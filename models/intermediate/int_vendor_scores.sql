{{
  config(
    materialized='ephemeral'
  )
}}

SELECT 
  c.id as company_id,
  NULL as score_value
FROM {{ ref('stg_companies') }} c
WHERE c.id IN (SELECT id FROM {{ ref('int_vendor_companies') }})
