SELECT 
    id,
    name,
    symbol,
    conversion_factor,
    status,
    base_unit_id,
    company_id,
    user_id,
    created_at,
    updated_at
FROM {{ source('staging', 'units') }}