{{
  config(
    materialized='ephemeral'
  )
}}

-- Add company name lookups to base orders
SELECT 
    ob.*,
    
    -- Company lookups
    sc.name AS seller_company_name,
    bc.name AS buyer_company_name
    
FROM {{ ref('int_orders_base') }} ob
LEFT JOIN {{ ref('stg_companies') }} sc ON ob.seller_company_id = sc.id
LEFT JOIN {{ ref('stg_companies') }} bc ON ob.buyer_company_id = bc.id