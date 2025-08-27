{{
  config(
    materialized='ephemeral'
  )
}}

-- Add remaining lookups to orders with companies
SELECT 
    oc.*,
    
    -- Additional lookups
    bh.name AS buyer_hub_name,
    p.name AS product_name_dim,
    p.product_type,
    pq.name AS product_quality_name
    
FROM {{ ref('int_orders_companies') }} oc
LEFT JOIN {{ ref('stg_buyer_hubs') }} bh ON oc.buyer_hub_id = bh.id
LEFT JOIN {{ ref('stg_products') }} p ON oc.product_id = p.id  
LEFT JOIN {{ ref('stg_product_qualities') }} pq ON oc.product_quality_id = pq.id