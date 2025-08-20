{{
  config(
    materialized='view'
  )
}}

SELECT 
  id,
  name,
  coordinates,
  status,
  created_at,
  updated_at,
  apmc_configuration_id,
  state_id,
  country_id
FROM {{ source('staging', 'cities') }}
