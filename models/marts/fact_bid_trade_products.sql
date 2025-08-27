{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'trade_product_id', 'status', 'price', 'quantity', 'product_sample_id',
      'city_id', 'city_name', 'applicable_apmc_product_config_id', 'rank_price',
      'final_price', 'gst', 'ancestry', 'created_at', 'updated_at',
      'company_id', 'company_name', 'score'
    ],
    dist='id',
    sort=['id', 'created_at', 'company_id', 'city_id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH bid_trade_products_enriched AS (
  SELECT 
    btp.id,
    btp.trade_product_id,
    btp.status,
    btp.price,
    btp.quantity,
    btp.product_sample_id,
    btp.city_id,
    c.name AS city_name,
    btp.applicable_apmc_product_config_id,
    btp.rank_price,
    btp.final_price,
    btp.gst,
    btp.ancestry,
    btp.created_at,
    btp.updated_at,
    btp.company_id,
    co.name AS company_name,
    btp.score
    
  FROM {{ ref('stg_bid_trade_products') }} btp
  LEFT JOIN {{ ref('stg_cities') }} c 
         ON btp.city_id = c.id
  LEFT JOIN {{ ref('stg_companies') }} co 
         ON btp.company_id = co.id
  
  {% if is_incremental() %}
    -- Incremental filter: only process records updated in the last 30 minutes
    WHERE btp.updated_at >= (SELECT MAX(updated_at) FROM {{ this }}) - INTERVAL '30 minutes'
       OR btp.created_at >= (SELECT MAX(updated_at) FROM {{ this }}) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  trade_product_id,
  status,
  price,
  quantity,
  product_sample_id,
  city_id,
  city_name,
  applicable_apmc_product_config_id,
  rank_price,
  final_price,
  gst,
  ancestry,
  created_at,
  updated_at,
  company_id,
  company_name,
  score
FROM bid_trade_products_enriched