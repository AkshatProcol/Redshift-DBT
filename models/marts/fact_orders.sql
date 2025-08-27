{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='id',
    incremental_strategy='merge',
    merge_update_columns=[
      'invoice_no', 'seller_company_id', 'trade_request_id', 'product_name',
      'po_email_sent', 'price', 'quantity', 'quantity_received', 'payment_status',
      'quality_feedback_status', 'status', 'delivered_on', 'created_by_id',
      'updated_at', 'buying_selling_offer_mapping_id', 'bid_id',
      'purchase_order_status', 'buyer_company_id', 'buyer_hub_id', 'product_id',
      'delivery_schedule_id', 'final_price', 'product_quality_id', 'unit_type',
      'source_event_type', 'source_event_id', 'qc_pending', 'source_quote_type',
      'source_quote_id', 'quality_approvals_accepted', 'quality_approvals_rejected',
      'source_event_creator_id', 'order_type', 'po_generated_at', 'time_cycle',
      'contract_status', 'retry_count', 'erp_order_type', 'dms_upload_status',
      'seller_company_name', 'buyer_company_name', 'buyer_hub_name',
      'product_name_dim', 'product_type', 'product_quality_name',
      'payment_status_label', 'order_status_label', 'fulfillment_percentage',
      'delivery_days', 'created_at_epoch', 'updated_at_epoch'
    ],
    dist='id',
    sort=['id', 'created_at', 'seller_company_id', 'buyer_company_id']
  )
}}

{% if is_incremental() %}
  -- For incremental runs, only process records updated in the last 30 minutes
  -- This aligns with the CDC system's staging window
{% endif %}

-- Simplified fact_orders using intermediate models for better performance
SELECT *
FROM {{ ref('int_orders_enriched') }}

{% if is_incremental() %}
WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }}) - INTERVAL '30 minutes'
   OR created_at >= (SELECT MAX(updated_at) FROM {{ this }}) - INTERVAL '30 minutes'
{% endif %}