-- Custom test to ensure vendor data quality
-- This test checks that all vendors have at least one POC or are properly marked

SELECT 
  vendor_id,
  vendor_name,
  poc_id,
  poc_name
FROM {{ ref('fact_vendor') }}
WHERE poc_id IS NULL 
  AND vendor_type != 'Broker Created Firm'  -- Allow broker created firms without POCs
  AND joining_status_label NOT IN ('Not Initiated', 'Rejected')  -- Allow incomplete vendors

-- This query should return 0 rows if the test passes
-- If it returns rows, it means there are vendors without POCs that should have them
