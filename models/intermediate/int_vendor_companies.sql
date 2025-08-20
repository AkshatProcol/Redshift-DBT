{{
  config(
    materialized='ephemeral'
  )
}}

SELECT 
  c.id,
  c.name,
  c.email,
  c.phone,
  c.gst_no,
  c.address,
  c.created_at,
  c.updated_at,
  c.category,
  c.created_by,
  c.is_verified,
  c.misc,
  c.status as company_status,
  ci.name as city_name,
  co.name as country_name,
  vm.vendor_code,
  vm.mapping_status,
  vm.network_joined_date,
  vm.source,
  vm.invited_by
FROM {{ ref('stg_companies') }} c
INNER JOIN {{ ref('int_vendor_mappings') }} vm ON c.id = vm.dealing_with_company_id
LEFT JOIN {{ ref('stg_cities') }} ci ON c.city_id = ci.id
LEFT JOIN {{ ref('stg_countries') }} co ON ci.country_id = co.id
WHERE c.status = 1
  AND c.category IN (1, 2, 4)
