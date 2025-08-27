SELECT 
    id,
    product_id,
    params,
    status,
    created_at,
    updated_at,
    name,
    created_by_user_id,
    attachments
FROM {{ source('staging', 'product_qualities') }}