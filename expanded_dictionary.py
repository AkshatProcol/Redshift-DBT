fact_table_mapping = {
    # =====================================================
    # COMPANIES TABLE → MULTIPLE FACT TABLES
    # =====================================================
    "companies": {
        "id": {
            "fact_vendor": "vendor_id",
            "fact_company_profile": "company_id", 
            "fact_financial": "company_id"
        },
        "name": {
            "fact_vendor": "vendor_name",
            "fact_company_profile": "company_name"
        },
        "email": {
            "fact_vendor": "vendor_email",
            "fact_company_profile": "contact_email"
        },
        "phone": {
            "fact_vendor": "primary_contact_phone",
            "fact_company_profile": "contact_phone"
        },
        "gst_no": {
            "fact_vendor": "gst_no",
            "fact_financial": "gst_number"
        },
        "address": {
            "fact_vendor": "address",
            "fact_company_profile": "business_address"
        },
        "category": {
            "fact_vendor": "vendor_type",
            "fact_company_profile": "business_category"
        },
        "is_verified": {
            "fact_vendor": "is_verified",
            "fact_company_profile": "verification_status"
        },
        "misc": {
            "fact_vendor": "misc,score_value",
            "fact_financial": "financial_score"
        }
    },

    # =====================================================
    # BUYER_SELLER_COMPANY_MAPPINGS → MULTIPLE FACT TABLES  
    # =====================================================
    "buyer_seller_company_mappings": {
        "vendor_code": {
            "fact_vendor": "vendor_code",
            "fact_relationship": "relationship_code"
        },
        "status": {
            "fact_vendor": "joining_status_label",
            "fact_relationship": "relationship_status",
            "fact_onboarding": "onboarding_stage"
        },
        "created_at": {
            "fact_vendor": "network_joined_date",
            "fact_relationship": "relationship_start_date",
            "fact_onboarding": "onboarding_date"
        },
        "source": {
            "fact_vendor": "source",
            "fact_relationship": "relationship_source"
        },
        "invited_by": {
            "fact_vendor": "invited_by,invited_by_name,invited_by_email",
            "fact_onboarding": "onboarding_user"
        },
        "client_company_id": {
            "fact_vendor": "client_company_id",
            "fact_relationship": "client_id"
        }
    },

    # =====================================================
    # USERS TABLE → MULTIPLE FACT TABLES
    # =====================================================
    "users_poc": {  # Point of Contacts
        "id": {
            "fact_vendor": "poc_id",
            "fact_contact": "contact_id",
            "fact_user_activity": "user_id"
        },
        "first_name": {
            "fact_vendor": "poc_name",
            "fact_contact": "contact_first_name"
        },
        "last_name": {
            "fact_contact": "contact_last_name"
        },
        "email": {
            "fact_vendor": "poc_email",
            "fact_contact": "contact_email",
            "fact_user_activity": "user_email"
        },
        "phone": {
            "fact_vendor": "poc_phone,primary_contact_phone",
            "fact_contact": "contact_phone",
            "fact_user_activity": "user_phone"
        },
        "created_at": {
            "fact_vendor": "poc_created_at",
            "fact_contact": "contact_created_at",
            "fact_user_activity": "user_created_at"
        }
    },

    "users_invited_by": {  # Inviter details
        "id": {
            "fact_vendor": "invited_by",
            "fact_onboarding": "onboarding_user_id"
        },
        "first_name + last_name": {
            "fact_vendor": "invited_by_name",
            "fact_onboarding": "onboarding_user_name"
        },
        "email": {
            "fact_vendor": "invited_by_email",
            "fact_onboarding": "onboarding_user_email"
        }
    },

    # =====================================================
    # TEAMS TABLE → MULTIPLE FACT TABLES
    # =====================================================
    "teams": {
        "name": {
            "fact_team_structure": "team_name"
        },
        "company_id": {
            "fact_vendor": "vendor_id",
            "fact_team_structure": "company_id"
        },
        "product_category_ids": {
            "fact_vendor": "category_names",
            "fact_team_structure": "team_categories"
        }
    },

    # =====================================================
    # GEOGRAPHIC DATA → MULTIPLE FACT TABLES
    # =====================================================
    "cities": {
        "name": {
            "fact_vendor": "city_name",
            "fact_company_profile": "city_name",
            "fact_geography": "city_name"
        }
    },

    "countries": {
        "name": {
            "fact_vendor": "country_name",
            "fact_company_profile": "country_name",
            "fact_geography": "country_name"
        }
    },

    # =====================================================
    # CLASSIFICATION DATA → MULTIPLE FACT TABLES
    # =====================================================
    "product_categories": {
        "name": {
            "fact_vendor": "category_names",
            "fact_team_structure": "team_categories",
            "fact_product_catalog": "category_name"
        }
    },

    "tags": {
        "name": {
            "fact_vendor": "tag_names",
            "fact_tagging": "tag_name"
        }
    },

    "preferred_vendor_item_mappings": {
        "item_id": {
            "fact_vendor": "preferred_item_ids",
            "fact_preferences": "item_id"
        }
    },

    # =====================================================
    # FUTURE FACT TABLES (Examples)
    # =====================================================
    "future_fact_tables": {
        "fact_audit_trail": "Complete change history across all entities",
        "fact_compliance": "Regulatory compliance tracking",
        "fact_performance_metrics": "Business performance indicators", 
        "fact_communication_history": "All vendor communications",
        "fact_document_management": "Document lifecycle tracking",
        "fact_financial_transactions": "Financial transaction history",
        "fact_geographical_analytics": "Location-based analytics",
        "fact_relationship_timeline": "Relationship evolution over time"
    },

    # =====================================================
    # BUSINESS LOGIC TRANSFORMATIONS
    # =====================================================
    "misc_derived": {
        "companies.misc->'score'->>'value'": {
            "fact_vendor": "score_value",
            "fact_financial": "financial_score",
            "fact_performance_metrics": "company_score"
        },
        "companies.phone + users.phone": {
            "fact_vendor": "primary_contact_phone",
            "fact_contact": "primary_phone"
        },
        "companies.category + companies.created_by": {
            "fact_vendor": "vendor_type",
            "fact_company_profile": "business_type"
        },
        "buyer_seller_company_mappings.status": {
            "fact_vendor": "joining_status_label",
            "fact_relationship": "relationship_status",
            "fact_onboarding": "onboarding_stage"
        }
    }
}

# =====================================================
# USAGE EXAMPLE
# =====================================================

"""
When a staging column changes, the system:

1. Identifies the changed column: e.g., "companies.name"
2. Looks up dictionary mapping: 
   {
     "fact_vendor": "vendor_name",
     "fact_company_profile": "company_name"
   }
3. Updates ALL affected fact tables:
   - fact_vendor.vendor_name
   - fact_company_profile.company_name

Benefits:
✅ Single change → Multiple fact table updates
✅ Consistent mapping logic across all pipelines
✅ Easy to add new fact tables
✅ Complete impact traceability
✅ Future-proof architecture
"""
