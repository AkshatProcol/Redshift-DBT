{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'first_name', 'last_name', 'firm_name', 'email', 'phone', 'gender',
      'category', 'city_id', 'company_id', 'status', 'role', 'email_verified',
      'created_by_user', 'session_id', 'access_mode', 'terms_and_policy_accepted',
      'is_phone_verified', 'is_email_verified', 'should_receive_sms',
      'should_receive_email', 'should_receive_sims', 'should_recieve_email',
      'send_whatsapp_msg', 'is_mfa_enabled', 'is_subscribed_to_zones',
      'created_at', 'updated_at', 'deleted_at'
    ],
    dist='all',
    sort=['id', 'created_at', 'company_id'],
    interleaved=true
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH users_base AS (
  SELECT
    id,
    first_name,
    last_name,
    firm_name,
    email,
    phone,
    gender,
    category,
    city_id,
    company_id,
    status,
    role,
    email_verified,
    created_by_user,
    session_id,
    access_mode,
    terms_and_policy_accepted,
    is_phone_verified,
    is_email_verified,
    should_receive_sms,
    should_receive_email,
    should_receive_sims,
    should_recieve_email,
    send_whatsapp_msg,
    is_mfa_enabled,
    is_subscribed_to_zones,
    created_at,
    updated_at,
    deleted_at
    
  FROM {{ ref('stg_users') }}
  WHERE id IS NOT NULL  -- Filter out NULL id values
  
  {% if is_incremental() %}
    -- Smart incremental filter: handles empty tables gracefully
    AND (updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
       OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes')
  {% endif %}
)

SELECT 
  id,
  first_name,
  last_name,
  firm_name,
  email,
  phone,
  gender,
  category,
  city_id,
  company_id,
  status,
  role,
  email_verified,
  created_by_user,
  session_id,
  access_mode,
  terms_and_policy_accepted,
  is_phone_verified,
  is_email_verified,
  should_receive_sms,
  should_receive_email,
  should_receive_sims,
  should_recieve_email,
  send_whatsapp_msg,
  is_mfa_enabled,
  is_subscribed_to_zones,
  created_at,
  updated_at,
  deleted_at
FROM users_base