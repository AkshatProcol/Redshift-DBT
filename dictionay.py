# =====================================================
# COMMON COLUMN GROUPS
# =====================================================

common_audit_columns = {
    "created_at": {"*": "created_at"},
    "updated_at": {"*": "updated_at"}
}

common_status_column = {
    "status": {"*": "status"}
}

common_company_columns = {
    "company_id": {"*": "company_id,company_name"}
}

common_quantity_columns = {
    "quantity": {"*": "quantity"},
    "remaining_quantity": {"*": "remaining_quantity"}
}

common_bid_columns = {
    "price": {"fact_bids": "price"},
    "quantity": {"fact_bids": "quantity"},
    "gst": {"fact_bids": "gst"},
    "rank_price": {"fact_bids": "rank_price"},
    "final_price": {"fact_bids": "final_price"},
    "amount": {"fact_bids": "amount"},
    "score": {"fact_bids": "score"},
    **common_status_column,
    **common_audit_columns
}

common_dim_metadata = {
    "remarks": {"*": "remarks"},
    "uuid": {"*": "uuid"},
    "ip_info": {"*": "ip_info"},
    "quality_params": {"*": "quality_params"},
    "terms_and_conditions": {"*": "terms_and_conditions"},
    "trade_credit_discount": {"*": "trade_credit_discount"},
    "images": {"*": "images"},
    "other_details": {"*": "other_details"},
    "price_breakup_json": {"*": "price_breakup_json"},
    "template_data": {"*": "template_data"},
    "meta_data": {"*": "meta_data"},
    "context": {"*": "context"}
}

# =====================================================
# FACT TABLE MAPPING - OPTIMIZED
# =====================================================

fact_table_mapping = {
    # COMPANIES → FACT_VENDOR
    "companies": {
        "id": {"fact_vendor": "vendor_id"},
        "name": {"fact_vendor": "vendor_name"},
        "email": {"fact_vendor": "vendor_email"},
        "phone": {"fact_vendor": "primary_contact_phone"},
        "gst_no": {"fact_vendor": "gst_no"},
        "address": {"fact_vendor": "address"},
        "category": {"fact_vendor": "vendor_type"},
        "is_verified": {"fact_vendor": "is_verified"},
        "misc": {"fact_vendor": "misc,score_value"}
    },

    # BUYER_SELLER_COMPANY_MAPPINGS → FACT_VENDOR
    "buyer_seller_company_mappings": {
        "vendor_code": {"fact_vendor": "vendor_code"},
        "status": {"fact_vendor": "joining_status_label"},
        "created_at": {"fact_vendor": "network_joined_date"},
        "source": {"fact_vendor": "source"},
        "invited_by": {"fact_vendor": "invited_by,invited_by_name,invited_by_email"},
        "client_company_id": {"fact_vendor": "client_company_id"}
    },

    # USERS (POC) → FACT_VENDOR
    "users_poc": {
        "id": {"fact_vendor": "poc_id"},
        "first_name": {"fact_vendor": "poc_name"},
        "email": {"fact_vendor": "poc_email"},
        "phone": {"fact_vendor": "poc_phone,primary_contact_phone"},
        "created_at": {"fact_vendor": "poc_created_at"}
    },

    "users_invited_by": {
        "id": {"fact_vendor": "invited_by"},
        "first_name + last_name": {"fact_vendor": "invited_by_name"},
        "email": {"fact_vendor": "invited_by_email"}
    },

    # TEAMS → FACT_VENDOR
    "teams": {
        "company_id": {"fact_vendor": "vendor_id"},
        "product_category_ids": {"fact_vendor": "category_names"}
    },

    # CITIES / COUNTRIES
    "cities": {"name": {"fact_vendor": "city_name"}},
    "countries": {"name": {"fact_vendor": "country_name"}},

    # CLASSIFICATION DATA
    "product_categories": {"name": {"fact_vendor": "category_names"}},
    "tags": {"name": {"fact_vendor": "tag_names"}},
    "preferred_vendor_item_mappings": {
        "item_id": {"fact_vendor": "preferred_item_ids"}
    },

    # AUDIENCES → DIM_AUDIENCES
    "audiences": {
        "id": {"dim_audiences": "id"},
        "item_type": {"dim_audiences": "item_type"},
        "item_id": {"dim_audiences": "item_id"},
        "audience_id": {"dim_audiences": "audience_id"},
        "status": {"dim_audiences": "status"},
        "forwarded_by": {"dim_audiences": "forwarded_by"},
        "visible": {"dim_audiences": "visible"},
        "source": {"dim_audiences": "source"},
        "audience_type": {"dim_audiences": "audience_type"},
        **common_audit_columns,
        "meta_data": {"dim_audiences": "meta_data"}
    },

    # EMPTY
    "team_members": {},
    "user_company_mappings": {},
    "taggings": {},

    # BIDS → FACT_BIDS + DIM_BIDS
    "bids": {
        "id": {"fact_bids": "id", "dim_bids": "id"},
        "user_id": {"fact_bids": "user_id"},
        "delivery_type": {"fact_bids": "delivery_type"},
        "company_id": {"fact_bids": "company_id"},
        "product_sample_id": {"fact_bids": "product_sample_id"},
        "city_id": {"fact_bids": "city_id"},
        "broker_id": {"fact_bids": "broker_id"},
        "broker_company_id": {"fact_bids": "broker_company_id"},
        "applicable_apmc_product_config_id": {
            "fact_bids": "applicable_apmc_product_config_id"
        },
        "contract_id": {"fact_bids": "contract_id"},
        "session_id": {"fact_bids": "session_id"},
        "approval_status": {"fact_bids": "approval_status"},
        "lock_version": {"fact_bids": "lock_version"},
        "end_time": {"fact_bids": "end_time"},
        "start_time": {"fact_bids": "start_time"},
        "closed_at": {"fact_bids": "closed_at"},
        "placed_at": {"fact_bids": "placed_at"},
        "negotiable": {"fact_bids": "negotiable"},
        "is_multi_product": {"fact_bids": "is_multi_product"},
        **common_bid_columns,
        **common_dim_metadata
    },

    # BID_TRADES → FACT_BID_TRADES
    "bid_trades": {
        "id": {"fact_bid_trades": "id"},
        "trade_request_id": {"fact_bid_trades": "trade_request_id"},
        "bid_id": {"fact_bid_trades": "bid_id"},
        "forwarded_by": {"fact_bid_trades": "forwarded_by"},
        **common_status_column,
        **common_audit_columns
    },

    # ORDERS → FACT_ORDERS
    "orders": {
        "id": {"fact_orders": "id"},
        "invoice_no": {"fact_orders": "invoice_no"},
        "seller_company_id": {
            "fact_orders": "seller_company_id,seller_company_name"
        },
        "trade_request_id": {"fact_orders": "trade_request_id"},
        "product_name": {"fact_orders": "product_name"},
        "po_email_sent": {"fact_orders": "po_email_sent"},
        "payment_status": {
            "fact_orders": "payment_status,payment_status_label"
        },
        "quality_feedback_status": {"fact_orders": "quality_feedback_status"},
        "delivered_on": {"fact_orders": "delivered_on,delivery_days"},
        "created_by_id": {"fact_orders": "created_by_id"},
        "buying_selling_offer_mapping_id": {
            "fact_orders": "buying_selling_offer_mapping_id"
        },
        "bid_id": {"fact_orders": "bid_id"},
        "purchase_order_status": {"fact_orders": "purchase_order_status"},
        "buyer_company_id": {
            "fact_orders": "buyer_company_id,buyer_company_name"
        },
        "buyer_hub_id": {"fact_orders": "buyer_hub_id,buyer_hub_name"},
        "product_id": {
            "fact_orders": "product_id,product_name_dim,product_type"
        },
        "delivery_schedule_id": {"fact_orders": "delivery_schedule_id"},
        "final_price": {"fact_orders": "final_price"},
        "product_quality_id": {
            "fact_orders": "product_quality_id,product_quality_name"
        },
        "unit_type": {"fact_orders": "unit_type"},
        "source_event_type": {"fact_orders": "source_event_type"},
        "source_event_id": {"fact_orders": "source_event_id"},
        "qc_pending": {"fact_orders": "qc_pending"},
        "source_quote_type": {"fact_orders": "source_quote_type"},
        "source_quote_id": {"fact_orders": "source_quote_id"},
        "quality_approvals_accepted": {
            "fact_orders": "quality_approvals_accepted"
        },
        "quality_approvals_rejected": {
            "fact_orders": "quality_approvals_rejected"
        },
        "source_event_creator_id": {"fact_orders": "source_event_creator_id"},
        "order_type": {"fact_orders": "order_type"},
        "po_generated_at": {"fact_orders": "po_generated_at,delivery_days"},
        "time_cycle": {"fact_orders": "time_cycle"},
        "contract_status": {"fact_orders": "contract_status"},
        "retry_count": {"fact_orders": "retry_count"},
        "erp_order_type": {"fact_orders": "erp_order_type"},
        "dms_upload_status": {"fact_orders": "dms_upload_status"},
        **common_quantity_columns,
        **common_audit_columns,
        **common_status_column
    },

    # BID_TRADE_PRODUCTS
    "bid_trade_products": {
        "id": {
            "fact_bid_trade_products": "id",
            "dim_bid_trade_products_details": "id"
        },
        "trade_product_id": {"fact_bid_trade_products": "trade_product_id"},
        "product_sample_id": {"fact_bid_trade_products": "product_sample_id"},
        "city_id": {"fact_bid_trade_products": "city_id,city_name"},
        "applicable_apmc_product_config_id": {
            "fact_bid_trade_products": "applicable_apmc_product_config_id"
        },
        "ancestry": {"fact_bid_trade_products": "ancestry"},
        "company_id": {"fact_bid_trade_products": "company_id,company_name"},
        **common_bid_columns,
        **common_dim_metadata
    },

    # TRADE_PRODUCTS
    "trade_products": {
        "id": {"fact_trade_products": "id"},
        "trade_request_id": {"fact_trade_products": "trade_request_id"},
        "product_id": {
            "fact_trade_products": "product_id,product_name,product_type"
        },
        "product_quality_id": {
            "fact_trade_products": "product_quality_id,product_quality_name"
        },
        "buyer_hub_id": {
            "fact_trade_products": "buyer_hub_id,buyer_hub_name"
        },
        "product_origins": {"fact_trade_products": "product_origins"},
        "auction_config": {"fact_trade_products": "auction_config"},
        "unit_type": {"fact_trade_products": "unit_type"},
        "current_status": {"fact_trade_products": "current_status"},
        "price_ceiling": {"fact_trade_products": "price_ceiling"},
        "price_floor": {"fact_trade_products": "price_floor"},
        "price": {"fact_trade_products": "price"},
        "gst": {"fact_trade_products": "gst"},
        "floor_price": {"fact_trade_products": "floor_price"},
        "ceil_price": {"fact_trade_products": "ceil_price"},
        "floor_quantity": {"fact_trade_products": "floor_quantity"},
        "sheet_order": {"fact_trade_products": "sheet_order"},
        "created_by_user_id": {"fact_trade_products": "created_by_user_id"},
        "unit_id": {
            "fact_trade_products": "unit_id,unit_name,unit_symbol"
        },
        "section": {"fact_trade_products": "section"},
        "order_no": {"fact_trade_products": "order_no"},
        "rank_one_price": {"fact_trade_products": "rank_one_price"},
        "rank_one_total_landed_amount": {
            "fact_trade_products": "rank_one_total_landed_amount"
        },
        **common_quantity_columns,
        **common_audit_columns,
        **common_status_column
    },

    # TRADE_REQUESTS
    "trade_requests": {
        "id": {"fact_trade_requests": "id"},
        "user_id": {
            "fact_trade_requests": "user_id,user_first_name,user_last_name"
        },
        "product_id": {
            "fact_trade_requests": "product_id,product_name,product_type"
        },
        "buyer_hub_id": {
            "fact_trade_requests": "buyer_hub_id,buyer_hub_name,buyer_hub_location"
        },
        "company_id": {
            "fact_trade_requests": "company_id,company_name,company_category"
        },
        "city_id": {"fact_trade_requests": "city_id,city_name"},
        "event_group_id": {
            "fact_trade_requests": "event_group_id,event_group_title"
        },
        "deal_closing_price": {"fact_trade_requests": "deal_closing_price"},
        "deal_closing_quantity": {
            "fact_trade_requests": "deal_closing_quantity"
        },
        "number_of_products": {"fact_trade_requests": "number_of_products"},
        "rank_one_gross_total": {"fact_trade_requests": "rank_one_gross_total"},
        "bid_start_time": {"fact_trade_requests": "bid_start_time"},
        "bid_end_time": {"fact_trade_requests": "bid_end_time"},
        "closed_at": {"fact_trade_requests": "closed_at"},
        "extra_closing_time": {"fact_trade_requests": "extra_closing_time"},
        "order_type": {"fact_trade_requests": "order_type"},
        "unit_type": {"fact_trade_requests": "unit_type"},
        "session_id": {"fact_trade_requests": "session_id"},
        "rfx_mode": {"fact_trade_requests": "rfx_mode"},
        "current_status": {"fact_trade_requests": "current_status"},
        "stage_no": {"fact_trade_requests": "stage_no"},
        "approval_status": {"fact_trade_requests": "approval_status"},
        "tender": {"fact_trade_requests": "tender"},
        "is_price_increase_approval_required": {
            "fact_trade_requests": "is_price_increase_approval_required"
        },
        "is_demo_event": {"fact_trade_requests": "is_demo_event"},
        "broker_company_id": {"fact_trade_requests": "broker_company_id"},
        "provisional_contracts_id": {
            "fact_trade_requests": "provisional_contracts_id"
        },
        "template_id": {"fact_trade_requests": "template_id"},
        "purchase_request_id": {"fact_trade_requests": "purchase_request_id"},
        **common_quantity_columns,
        **common_audit_columns,
        **common_status_column
    }
}
