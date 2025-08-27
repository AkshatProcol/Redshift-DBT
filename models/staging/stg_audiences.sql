SELECT 
    id,
    item_type,
    item_id,
    audience_id,
    status,
    forwarded_by,
    created_at,
    updated_at,
    visible,
    source,
    audience_type,
    meta_data
FROM {{ source('staging', 'audiences') }}