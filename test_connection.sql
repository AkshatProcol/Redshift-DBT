-- Test connection and schema access
-- This file can be run directly in Redshift to verify access

-- Test staging schema access
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables 
WHERE schemaname = 'staging'
ORDER BY tablename;

-- Test prod_poc schema access
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables 
WHERE schemaname = 'prod_poc'
ORDER BY tablename;

-- Test if we can read from staging tables
SELECT COUNT(*) as total_tables
FROM pg_tables 
WHERE schemaname = 'staging';

-- Test if we can create in prod_poc schema
SELECT current_user, current_database(), current_schema();
