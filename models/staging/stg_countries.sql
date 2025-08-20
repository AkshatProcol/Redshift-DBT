{{
  config(
    materialized='view'
  )
}}

SELECT 
  id,
  name,
  country_code,
  phone_prefix,
  phone_number_length,
  created_at,
  flag_url,
  config,
  send_otp_allowed,
  updated_at
FROM {{ source('staging', 'countries') }}
