# CDC Implementation Plan

## 🎯 **Objective**
Implement intelligent Change Data Capture (CDC) system that:
- Detects row-level changes in staging tables
- Uses `expanded_dictionary.py` to propagate changes to multiple fact tables
- Distinguishes between INSERT (created_at = updated_at) and UPDATE operations
- Updates only affected columns rather than full table refreshes

## 🏗️ **Architecture Components**

### 1. **Mirror Tables (staging_public schema)**
```
staging_public.fact_companies      ← Mirror of staging.companies
staging_public.fact_users          ← Mirror of staging.users  
staging_public.fact_teams          ← Mirror of staging.teams
... (for all 12 staging tables)
```

### 2. **Change Detection Logic**
```sql
-- In DBT macro: cdc_processor.sql
IF created_at = updated_at THEN 'INSERT'
ELSE 'UPDATE' + column_comparison()
```

### 3. **Python Orchestrator**
- `cdc_orchestrator.py` - Main CDC controller
- Uses `expanded_dictionary.py` for column mappings
- Executes targeted updates to fact tables

### 4. **Dictionary-Driven Updates**
```python
# Example: companies.name changes
{
  "companies": {
    "name": {
      "fact_vendor": "vendor_name",
      "fact_company_profile": "company_name"
    }
  }
}
# → Updates both fact_vendor.vendor_name AND fact_company_profile.company_name
```

## 🚀 **Implementation Steps**

### Phase 1: Infrastructure Setup
1. **Create Mirror Tables**
   ```bash
   # Create staging_public mirror tables for all 12 staging tables
   dbt run --select staging_public
   ```

2. **Deploy CDC Macro**
   ```sql
   -- macros/cdc_processor.sql already created
   -- Detects changes by comparing staging vs staging_public
   ```

### Phase 2: Change Detection Engine
3. **Test Change Detection**
   ```bash
   # Test the macro
   dbt run-operation detect_changes --args '{"table": "companies"}'
   ```

4. **Python Integration** 
   ```bash
   # Make orchestrator executable
   chmod +x cdc_orchestrator.py
   
   # Test CDC processing
   python cdc_orchestrator.py
   ```

### Phase 3: Production Deployment
5. **Schedule CDC Processing**
   ```bash
   # Cron job every 30 minutes
   */30 * * * * cd /path/to/project && python cdc_orchestrator.py
   ```

6. **Monitor and Optimize**
   - Add logging and alerting
   - Performance monitoring
   - Error handling and retry logic

## 📊 **Data Flow Example**

### Scenario: Company name changes
```
1. staging.companies.name: "ABC Corp" → "ABC Corporation Ltd"
2. CDC detects: UPDATE on companies.id=5, changed_columns=['name']
3. Dictionary lookup: companies.name maps to:
   - fact_vendor.vendor_name
   - fact_company_profile.company_name
4. Execute targeted updates:
   UPDATE fact_vendor SET vendor_name='ABC Corporation Ltd' WHERE vendor_id=5
   UPDATE fact_company_profile SET company_name='ABC Corporation Ltd' WHERE company_id=5
```

## 🔧 **Key Benefits**

### ✅ **Efficiency**
- Only processes changed records (30-min window)
- Updates specific columns, not entire tables
- Parallel processing of multiple fact tables

### ✅ **Consistency** 
- Single source of truth (expanded_dictionary.py)
- Guaranteed consistent mappings across all fact tables
- Atomic updates prevent data inconsistencies

### ✅ **Scalability**
- Easy to add new fact tables to dictionary
- Handles complex many-to-many column relationships
- Future-proof architecture

### ✅ **Observability**
- Complete audit trail of all changes
- Impact analysis (which fact tables affected)
- Performance metrics and monitoring

## 🛠️ **Advanced Features**

### Column-Level Impact Analysis
```python
def analyze_impact(source_table: str, columns: List[str]):
    """Show exactly which fact tables will be affected"""
    return orchestrator.get_affected_fact_tables(source_table, columns)
```

### Batch Processing
```python
def process_batch_changes(changes: List[Dict]):
    """Process multiple changes in optimized batches"""
    # Group changes by fact table for batch updates
```

### Rollback Capability
```python
def rollback_changes(change_id: str):
    """Rollback specific changes using audit trail"""
    # Restore previous values from change history
```

## 📈 **Performance Expectations**

### Current State (Full Refresh)
- **fact_vendor**: ~15 seconds for complete rebuild
- **All 12 tables**: ~3 minutes total pipeline time

### With CDC (Incremental)
- **Average change**: ~2-5 seconds per affected fact table  
- **Bulk changes**: ~30 seconds for 100+ record updates
- **No changes**: ~10 seconds detection time

### Projected Improvement
- **90% reduction** in processing time for typical workloads
- **Real-time updates** with 30-minute maximum latency
- **Linear scalability** as fact tables increase

## 🚨 **Implementation Considerations**

### Data Quality
- Ensure `created_at` and `updated_at` are consistently maintained
- Handle NULL values in timestamp comparisons
- Validate dictionary mappings before deployment

### Error Handling
- Retry logic for failed updates
- Rollback mechanisms for partial failures
- Alert system for CDC pipeline issues

### Testing Strategy
- Unit tests for each dictionary mapping
- Integration tests for end-to-end CDC flow
- Performance tests with realistic data volumes

## 📋 **Next Steps**

1. **Review and approve** this implementation plan
2. **Create mirror tables** for all 12 staging tables  
3. **Test CDC detection** with sample data changes
4. **Validate dictionary mappings** against current fact_vendor
5. **Deploy to staging environment** for full testing
6. **Production rollout** with monitoring

This CDC system will transform your data pipeline from batch-oriented full refreshes to real-time, intelligent change processing! 🚀