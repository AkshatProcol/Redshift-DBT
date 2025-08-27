SELECT 
    id,
    trade_request_id,
    bid_id,
    forwarded_by,
    status,
    created_at,
    updated_at
FROM {{ source('staging', 'bid_trades') }}