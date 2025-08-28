{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'address', 'adderss', 'profile_pic_url', 'referral_code', 'referal_code',
      'mfa_secret', 'user_access_right_id', 'user_access_right_ids',
      'other_details', 'team_identifiers', 'tnc_and_policy_log',
      'ip_info', 'update_remarks', 'created_at', 'updated_at', 'deleted_at'
    ],
    dist='all',
    sort=['id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH users_disc_base AS (
  SELECT
    id,
    address,
    adderss,
    profile_pic_url,
    referral_code,
    referal_code,
    mfa_secret,
    user_access_right_id,
    user_access_right_ids,
    other_details,
    team_identifiers,
    tnc_and_policy_log,
    ip_info,
    update_remarks,
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
  address,
  adderss,
  profile_pic_url,
  referral_code,
  referal_code,
  mfa_secret,
  user_access_right_id,
  user_access_right_ids,
  other_details,
  team_identifiers,
  tnc_and_policy_log,
  ip_info,
  update_remarks,
  created_at,
  updated_at,
  deleted_at
FROM users_disc_base