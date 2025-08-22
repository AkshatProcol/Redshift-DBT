# Example: Adding New Row - Expected CDC Output

```
🔬 Surgical CDC Processor - Full Column Precision Mode
🚀 Starting Surgical CDC Processing at 2025-08-22 17:00:00

📋 Analyzing companies with surgical precision...
   📊 Found 1 records to analyze
   🔍 Will compare 45 columns per record
   
   🔬 Record 6 (INSERT):
      📝 43 columns changed: id, name, email, phone, gst_no, address, category, is_verified, misc...
   
   🎯 Analyzing 43 changed columns:
      📝 id → affects 3 fact tables
      📝 name → affects 2 fact tables  
      📝 email → affects 2 fact tables
      📝 phone → affects 2 fact tables
      📝 gst_no → affects 2 fact tables
      📝 address → affects 2 fact tables
      📝 category → affects 2 fact tables
      📝 is_verified → affects 2 fact tables
      📝 misc → affects 2 fact tables
      ⚠️  image_url not in dictionary - no fact table mapping
      ⚠️  coordinates not in dictionary - no fact table mapping
      ... (other unmapped columns ignored)

🎯 Executing surgical updates for 6 fact tables...
🔬 Based on 9 column-level changes detected (only mapped columns counted)

🔄 fact_vendor:
   📊 Triggered by 1 record change (INSERT)
   🎯 Columns affected: vendor_id, vendor_name, vendor_email, primary_contact_phone, gst_no, address, vendor_type, is_verified, misc, score_value
   ✅ Successfully updated fact_vendor

🔄 fact_company_profile:
   📊 Triggered by 1 record change (INSERT) 
   🎯 Columns affected: company_id, company_name, contact_email, contact_phone, business_address, business_category, verification_status
   ✅ Successfully updated fact_company_profile

🔄 fact_financial:
   📊 Triggered by 1 record change (INSERT)
   🎯 Columns affected: company_id, gst_number, financial_score  
   ✅ Successfully updated fact_financial

... (all 6 fact tables updated)

🎉 Surgical CDC Processing Complete!
⏱️  Duration: 45.23 seconds
🔬 Records analyzed: 1
📊 Columns compared: 45
🎯 Changes detected: 9 (mapped columns only)
✅ Fact tables updated: 6
```