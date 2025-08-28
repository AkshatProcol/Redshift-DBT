{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'name', 'image_url', 'address', 'coordinates', 'phone', 'email',
      'email_extension', 'pan_no', 'fssai_code', 'gst_no', 'owner_name',
      'year_of_establishment', 'tan_number', 'number_of_employees', 'domain',
      'website', 'broker_for', 'document_images', 'update_remarks',
      'allowed_modules', 'misc', 'recommendation_identifier', 'created_at', 'updated_at'
    ],
    dist='all',
    sort=['id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

WITH companies_base AS (
  SELECT
    id,
    name,
    image_url,
    address,
    coordinates,
    phone,
    email,
    email_extension,
    pan_no,
    fssai_code,
    gst_no,
    owner_name,
    year_of_establishment,
    tan_number,
    number_of_employees,
    domain,
    website,
    broker_for,
    document_images,
    update_remarks,
    allowed_modules,
    misc,
    recommendation_identifier,
    created_at,
    updated_at
    
  FROM {{ ref('stg_companies') }}
  
  {% if is_incremental() %}
    -- Smart incremental filter: handles empty tables gracefully
    WHERE updated_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
       OR created_at >= COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1900-01-01'::timestamp) - INTERVAL '30 minutes'
  {% endif %}
)

SELECT 
  id,
  name,
  image_url,
  address,
  coordinates,
  phone,
  email,
  email_extension,
  pan_no,
  fssai_code,
  gst_no,
  owner_name,
  year_of_establishment,
  tan_number,
  number_of_employees,
  domain,
  website,
  broker_for,
  document_images,
  update_remarks,
  allowed_modules,
  misc,
  recommendation_identifier,
  created_at,
  updated_at
FROM companies_base
WHERE id IS NOT NULL