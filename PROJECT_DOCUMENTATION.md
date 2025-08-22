# DBT Time - Vendor Data Pipeline Documentation

## Project Overview

**DBT Time** is a comprehensive vendor data pipeline built with dbt (Data Build Tool) that creates a unified vendor dataset from multiple staging tables in Amazon Redshift. The pipeline processes vendor information, points of contact (POCs), product categories, tags, and scoring data to generate a production-ready fact table.

### Key Features

- **Incremental Processing**: Processes only changes from the last 30 minutes using `updated_at` and `created_at` timestamps
- **Multi-POC Support**: Each vendor can have multiple Points of Contact, with each POC represented as a separate row
- **Real-time Sync**: Automatically detects and processes changes from staging to production
- **Comprehensive Data Model**: Combines vendor details, contact information, categorization, tagging, and scoring
- **Data Quality Testing**: Built-in tests ensure data integrity and quality

## Architecture & Data Flow

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Staging Schema │    │ Intermediate     │    │   Marts Schema  │
│  (12 tables)    │───▶│ Models           │───▶│   fact_vendor   │
│                 │    │ (9 models)       │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Database Configuration

- **Database**: `dev` (Redshift Serverless)
- **Host**: `default-workgroup.885373794985.ap-south-1.redshift-serverless.amazonaws.com`
- **Port**: 5439
- **Staging Schema**: `staging` (source data)
- **Production Schema**: `prod_poc` (output table)

## Project Structure

```
Redshift-DBT/
├── models/
│   ├── staging/              # Staging models (12 models)
│   │   ├── _sources.yml      # Source table definitions
│   │   ├── stg_companies.sql
│   │   ├── stg_users.sql
│   │   ├── stg_teams.sql
│   │   └── ... (9 more staging models)
│   ├── intermediate/         # Business logic models (9 models)
│   │   ├── int_vendor_companies.sql
│   │   ├── int_vendor_all_pocs.sql
│   │   ├── int_vendor_product_categories.sql
│   │   └── ... (6 more intermediate models)
│   └── marts/               # Final output models
│       ├── _schema.yml      # Output schema definitions
│       └── fact_vendor.sql  # Main fact table
├── macros/
│   └── incremental_merge.sql # Custom merge macro
├── tests/
│   └── test_fact_vendor.sql # Data quality tests
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Database connections
├── packages.yml             # External dependencies
├── requirements.txt         # Python dependencies
├── run_pipeline.sh          # Pipeline execution script
└── verify_setup.sh          # Setup verification script
```

## Data Model

### Source Tables (Staging Schema)

The pipeline processes 12 staging tables:

1. **buyer_seller_company_mappings** - Vendor-client relationships and mappings
2. **companies** - Company information including address, contact, and verification status
3. **users** - User profiles and contact information
4. **teams** - Company teams and their product category associations
5. **team_members** - Team membership and roles
6. **cities** - Geographic city information
7. **countries** - Country data with phone prefixes and configurations
8. **product_categories** - Product category hierarchy and details
9. **user_company_mappings** - User-company relationship mappings
10. **taggings** - Tag associations for companies and users
11. **tags** - Tag definitions and metadata
12. **preferred_vendor_item_mappings** - Preferred vendor item relationships

### Intermediate Models

The pipeline uses 9 intermediate models to process complex business logic:

- `int_vendor_companies` - Core vendor company information
- `int_vendor_all_pocs` - All points of contact for each vendor
- `int_vendor_product_categories` - Product category associations
- `int_vendor_tags` - Tag associations for vendors
- `int_vendor_scores` - Vendor scoring information
- `int_preferred_vendor_mappings` - Preferred vendor relationships
- `int_invited_by_users` - Information about users who invited vendors
- `int_vendor_mappings` - Vendor mapping relationships
- `int_user_product_categories` - User-product category associations

### Output Model (fact_vendor)

The final `fact_vendor` table includes:

#### Vendor Information
- `vendor_id` - Unique vendor company ID
- `vendor_name` - Company name
- `vendor_email` - Company email
- `gst_no` - GST number
- `address` - Company address
- `city_name` & `country_name` - Location information
- `vendor_created_at` - Creation timestamp (epoch)
- `is_verified` - Verification status

#### Business Logic
- `vendor_type` - Type classification (Broker Created Firm, Selling Firm, etc.)
- `joining_status_label` - Human-readable status (Not Initiated, In Progress, etc.)
- `category_names` - Array of product categories
- `vendor_code` - Unique vendor code
- `network_joined_date` - Network joining date (epoch)

#### Point of Contact (POC)
- `poc_id` - POC user ID
- `poc_name` - POC full name
- `poc_email` - POC email
- `poc_phone` - POC phone number
- `primary_contact_phone` - Primary contact (company or POC phone)

#### Metadata
- `tag_names` - Array of associated tags
- `preferred_item_ids` - Array of preferred item IDs
- `score_value` - Vendor score
- `invited_by` - User ID who invited the vendor
- `invited_by_name` & `invited_by_email` - Inviter details
- `misc` - Miscellaneous vendor data
- `updated_at` - Last update timestamp for incremental processing

## Incremental Processing

The pipeline uses an efficient incremental strategy:

- **Strategy**: Redshift MERGE operations for upserts
- **Unique Key**: `vendor_id_poc_id` (composite key combining vendor and POC IDs)
- **Detection Window**: Last 30 minutes based on `updated_at` timestamps
- **Performance**: Only processes changed records, reducing runtime significantly

### Incremental Logic
```sql
{% if is_incremental() %}
WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }}) - INTERVAL '30 minutes'
{% endif %}
```

## Dependencies

### External Packages
- `dbt-labs/dbt_utils` (v1.1.1) - Utility macros
- `calogica/dbt_expectations` (v0.8.0) - Data quality testing

### Python Requirements
- `dbt-core` - Core dbt functionality
- `dbt-redshift` - Redshift adapter

## Setup & Installation

### 1. Install Dependencies
```bash
# Install dbt
pip install dbt-core dbt-redshift

# Install project dependencies
dbt deps
```

### 2. Configuration
The project uses environment-specific profiles in `profiles.yml`:
- **dev**: Development environment (staging schema)
- **prod**: Production environment (prod_poc schema)

### 3. Verify Setup
```bash
# Test connection
dbt debug

# Run verification script
./verify_setup.sh
```

### 4. Execute Pipeline
```bash
# Run all models
dbt run

# Run with automated script
./run_pipeline.sh

# Full refresh (if needed)
dbt run --full-refresh
```

## Data Quality & Testing

### Built-in Tests
- **Uniqueness**: Ensures `vendor_id_poc_id` is unique
- **Not Null**: Validates required fields
- **Referential Integrity**: Checks relationships between models

### Custom Tests
- `test_fact_vendor.sql` - Custom business logic validation

### Running Tests
```bash
# Run all tests
dbt test

# Run specific tests
dbt test --select fact_vendor
```

## Monitoring & Operations

### Pipeline Monitoring
```bash
# Check model status
dbt ls

# Compile without running
dbt compile

# View dependencies
dbt list --select +fact_vendor
```

### Scheduling
For production deployment, schedule the pipeline every 30 minutes:
```bash
# Cron job example
*/30 * * * * cd /path/to/Redshift-DBT && dbt run --target prod
```

### Performance Optimization
- Uses Redshift-optimized SQL patterns
- Leverages incremental processing to minimize data movement
- Efficient JOIN strategies in intermediate models
- Proper indexing through unique keys

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify Redshift credentials in `profiles.yml`
   - Check network connectivity and security groups
   - Validate database/schema permissions

2. **Incremental Processing Issues**
   - Ensure source tables have proper `updated_at` columns
   - Check for data type mismatches in timestamps
   - Verify incremental strategy configuration

3. **Data Quality Failures**
   - Review test failures with `dbt test --store-failures`
   - Check source data quality and completeness
   - Validate business logic in intermediate models

### Debug Commands
```bash
# Show compiled SQL
dbt compile --select fact_vendor

# Run with detailed logging
dbt run --debug

# Check for failures
dbt run --fail-fast
```

## Security Considerations

- Database credentials are stored in `profiles.yml` (ensure proper access controls)
- Consider using environment variables for sensitive information
- Implement proper Redshift user permissions and schema isolation
- Regular monitoring of data access patterns

## Future Enhancements

### Potential Improvements
1. **Data Lineage Tracking**: Implement dbt docs for data lineage visualization
2. **Alerting**: Add automated alerts for pipeline failures
3. **Performance Monitoring**: Implement query performance tracking
4. **Historical Tracking**: Add SCD Type 2 for vendor history
5. **Data Validation**: Enhanced data quality checks with dbt-expectations

### Scalability Considerations
- Consider partitioning strategies for large datasets
- Implement data archival policies for historical data
- Optimize Redshift cluster configuration for workload patterns
- Consider implementing data catalog for metadata management

## Contact & Support

For issues or questions:
1. Review dbt logs and error messages
2. Check Redshift query performance and execution plans
3. Validate source data quality and completeness
4. Consult dbt documentation for advanced configurations

## License

This project is for internal use. Ensure compliance with organizational data handling policies and security requirements.