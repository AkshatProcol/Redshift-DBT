SELECT 
    id,
    name,
    status,
    city_id,
    coordinates,
    created_at,
    updated_at,
    company_id,
    location_code,
    location_type,
    address,
    zone_id,
    misc,
    created_by,
    user_id
FROM {{ source('staging', 'buyer_hubs') }}