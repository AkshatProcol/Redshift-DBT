# DBT Time - Vendor Data Pipeline

This dbt project creates a comprehensive vendor data pipeline that connects to a Redshift database and syncs incremental changes from staging tables to a final `fact_vendor` table in the production schema.

## Project Overview

The pipeline processes 12 staging tables and creates a comprehensive vendor dataset where each vendor can have multiple Points of Contact (POCs), with each POC represented as a separate row in the final output.

### Key Features

- **Incremental Processing**: Only processes changes from the last 30 minutes based on `updated_at` and `created_at` columns
- **Multi-POC Support**: Creates separate rows for each POC associated with a vendor
- **Real-time Sync**: Automatically detects and syncs changes from staging to production
- **Comprehensive Vendor Data**: Includes vendor details, POC information, tags, categories, and scores

## Project Structure

```
DBT_TIME/
├── models/
│   ├── staging/           # Staging models for each source table
│   ├── intermediate/      # Intermediate models for complex logic
│   └── marts/            # Final fact table
├── macros/                # Custom macros
├── tests/                 # Data quality tests
├── dbt_project.yml        # Project configuration
├── profiles.yml           # Database connection profiles
└── packages.yml           # External package dependencies
```

## Database Configuration

- **Database**: `dev` (Redshift Serverless)
- **Host**: `default-workgroup.885373794985.ap-south-1.redshift-serverless.amazonaws.com`
- **Port**: 5439
- **User**: admin
- **Staging Schema**: `staging` (12 source tables)
- **Production Schema**: `prod_poc` (fact_vendor table)

## Staging Tables

The pipeline processes the following 12 staging tables from the `staging` schema:

1. `buyer_seller_company_mappings` - Vendor mappings for client companies
2. `teams` - Company teams and product categories
3. `team_members` - Team members and associations
4. `companies` - Company information
5. `cities` - City information
6. `countries` - Country information
7. `product_categories` - Product category information
8. `user_company_mappings` - User to company mappings
9. `users` - User information
10. `taggings` - Tag associations
11. `tags` - Tag information
12. `preferred_vendor_item_mappings` - Preferred vendor item mappings

## Setup Instructions

### 1. Install Dependencies

```bash
# Install dbt
pip install dbt-core dbt-redshift

# Install project dependencies
dbt deps
```

### 2. Test Connection

```bash
# Test the connection to your Redshift database
dbt debug
```

### 3. Run the Pipeline

```bash
# Run all models
dbt run

# Run specific models
dbt run --select staging
dbt run --select intermediate
dbt run --select marts

# Run with full refresh (if needed)
dbt run --full-refresh
```

### 4. Test Data Quality

```bash
# Run all tests
dbt test

# Run specific tests
dbt test --select fact_vendor
```

## Incremental Processing

The pipeline uses incremental processing to efficiently handle updates:

- **Detection**: Monitors `updated_at` and `created_at` columns for changes in the last 30 minutes
- **Strategy**: Uses Redshift MERGE operations for efficient upserts
- **Key**: Composite key combining `vendor_id` and `poc_id` ensures uniqueness
- **Performance**: Only processes changed records, significantly reducing runtime

## Output Schema

The final `fact_vendor` table in the `prod_poc` schema includes:

- **Vendor Information**: Company details, contact info, location, verification status
- **POC Details**: Multiple POCs per vendor with contact information
- **Business Logic**: Vendor type, joining status, product categories
- **Metadata**: Tags, scores, preferred items, network information

## Scheduling

For production use, schedule the pipeline to run every 30 minutes to ensure real-time data sync:

```bash
# Example cron job (every 30 minutes)
*/30 * * * * cd /path/to/DBT_TIME && dbt run --target prod
```

## Monitoring

Monitor the pipeline execution:

```bash
# Check model status
dbt ls

# View run history
dbt run-operation show_models

# Check for failed models
dbt run --fails-fast
```

## Troubleshooting

### Common Issues

1. **Connection Errors**: Verify Redshift credentials and network access
2. **Permission Errors**: Ensure user has access to staging and production schemas
3. **Incremental Issues**: Check if `updated_at` columns are properly maintained in source tables

### Debug Commands

```bash
# Compile SQL without running
dbt compile

# Show compiled SQL for specific model
dbt compile --select fact_vendor

# Check model dependencies
dbt list --select +fact_vendor
```

## Customization

### Adding New Tables

1. Create staging model in `models/staging/`
2. Add source definition in `models/staging/_sources.yml`
3. Update intermediate models if needed
4. Modify `fact_vendor` to include new data

### Modifying Business Logic

- Update intermediate models in `models/intermediate/`
- Modify the final `fact_vendor` model
- Update tests and documentation

## Support

For issues or questions:
1. Check the dbt documentation
2. Review model logs and error messages
3. Verify data quality with tests
4. Check Redshift query performance

## License

This project is for internal use. Please ensure compliance with your organization's data handling policies.
