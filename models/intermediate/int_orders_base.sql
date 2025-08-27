{{
  config(
    materialized='ephemeral'
  )
}}

-- Base order data with calculated fields (no JOINs for performance)
SELECT 
    -- Core order information
    id,
    invoice_no,
    seller_company_id,
    buyer_company_id,
    trade_request_id,
    product_name,
    po_email_sent,
    price,
    quantity,
    quantity_received,
    payment_status,
    quality_feedback_status,
    status,
    delivered_on,
    created_by_id,
    created_at,
    updated_at,
    buying_selling_offer_mapping_id,
    bid_id,
    purchase_order_status,
    buyer_hub_id,
    product_id,
    delivery_schedule_id,
    final_price,
    product_quality_id,
    unit_type,
    source_event_type,
    source_event_id,
    qc_pending,
    source_quote_type,
    source_quote_id,
    quality_approvals_accepted,
    quality_approvals_rejected,
    source_event_creator_id,
    order_type,
    po_generated_at,
    time_cycle,
    contract_status,
    retry_count,
    erp_order_type,
    dms_upload_status,
    
    -- Calculated status labels (simple CASE statements)
    CASE payment_status
      WHEN 1 THEN 'Pending'
      WHEN 2 THEN 'Partial'
      WHEN 3 THEN 'Complete'
      ELSE 'Unknown'
    END AS payment_status_label,
    
    CASE status
      WHEN 1 THEN 'Active'
      WHEN 2 THEN 'Completed'
      WHEN 3 THEN 'Cancelled'
      ELSE 'Unknown'
    END AS order_status_label,
    
    -- Simple fulfillment calculation
    CASE 
      WHEN quantity > 0 AND quantity_received IS NOT NULL 
      THEN ROUND((quantity_received / quantity) * 100, 2)
      ELSE 0
    END AS fulfillment_percentage,
    
    -- Simplified delivery days calculation
    CASE 
      WHEN po_generated_at IS NOT NULL AND delivered_on IS NOT NULL 
      THEN DATEDIFF(days, po_generated_at, delivered_on)
      ELSE NULL
    END AS delivery_days,
    
    -- Epoch timestamps for analytics
    EXTRACT(EPOCH FROM created_at) AS created_at_epoch,
    EXTRACT(EPOCH FROM updated_at) AS updated_at_epoch

FROM {{ ref('stg_orders') }}