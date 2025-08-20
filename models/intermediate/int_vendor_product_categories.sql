{{
  config(
    materialized='ephemeral'
  )
}}

SELECT 
  t.company_id,
  LISTAGG(DISTINCT pc.name, ', ') as category_names
FROM {{ ref('stg_teams') }} t
INNER JOIN {{ ref('stg_product_categories') }} pc ON t.product_category_ids::text LIKE '%' || pc.id::text || '%'
WHERE t.company_id IN (SELECT id FROM {{ ref('int_vendor_companies') }})
  AND t.status = 1
  AND t.product_category_ids IS NOT NULL 
  AND t.product_category_ids::text != '[]'
  AND pc.status = 1
GROUP BY t.company_id
