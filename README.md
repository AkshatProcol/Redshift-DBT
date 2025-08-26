# Redshift-DBT: Advanced Vendor Data Pipeline with Surgical CDC

![Architecture](https://img.shields.io/badge/Architecture-3--Phase_CDC-blue)
![Database](https://img.shields.io/badge/Database-Amazon_Redshift-orange)
![Processing](https://img.shields.io/badge/Processing-Surgical_Precision-green)
![Status](https://img.shields.io/badge/Status-Production_Ready-success)
![Optimization](https://img.shields.io/badge/Optimized-80%25_Efficiency-brightgreen)
![Updated](https://img.shields.io/badge/Updated-2025--08--26-blue)

A sophisticated vendor data pipeline with intelligent Change Data Capture (CDC) system for Amazon Redshift. Features surgical precision column-level change detection and real-time processing of vendor information from staging to production fact tables.

## 🎯 Project Overview

**Redshift-DBT** combines the power of dbt (Data Build Tool) with a custom Python CDC engine to create an enterprise-grade data pipeline that processes vendor information with unprecedented precision and efficiency.

### ✨ Key Features

- **🔬 Surgical Precision**: Column-level change detection across 359 columns
- **⚡ Optimized Processing**: Dictionary-first approach with 80% efficiency improvement
- **🎯 Intelligent Targeting**: Only processes dictionary-mapped columns (111 vs 578)
- **🔄 3-Phase Architecture**: Detection → Update → Sync
- **📊 Multi-Schema Processing**: Staging → Public → Production flow
- **🏗️ Enterprise Grade**: Proven with live data and production testing
- **🧹 Clean Structure**: Production-ready with unnecessary files removed

## 🏗️ Architecture

```
📊 Public Schema (baseline data)
    ↕️ COMPARE (column-by-column)
📊 Staging Schema (last 30 mins changes)
    ↓ DICTIONARY MAPPING
📊 staging_public Schema (fact tables)
```

### 🔄 Data Flow

1. **Source System** → Updates records in operational database
2. **ETL Process** → Loads changes to staging schema (30-minute windows)
3. **CDC Engine** → Detects changes with surgical precision
4. **Fact Updates** → Updates only affected fact table columns
5. **Public Sync** → Maintains baseline for next cycle comparison

## 🛠️ Technology Stack

- **Database**: Amazon Redshift Serverless
- **Transformation**: dbt (Data Build Tool)
- **CDC Engine**: Python 3 with psycopg2-binary
- **Change Detection**: Advanced column-by-column comparison
- **Orchestration**: Custom surgical precision processors

## 📁 Project Structure

```
Redshift-DBT/
├── 🔧 CDC Engine
│   ├── complete_cdc_processor.py      # Main CDC processor ⭐
│   ├── expanded_dictionary.py         # Column mapping logic ⭐
│   └── cdc_env/                       # Python virtual environment
├── 📊 DBT Project
│   ├── models/
│   │   ├── staging/                   # 12 staging models
│   │   ├── intermediate/              # 9 intermediate models
│   │   └── marts/fact_vendor.sql      # Main fact table ⭐
│   ├── dbt_project.yml
│   └── profiles.yml                   # Database connections
├── 📝 Documentation
│   ├── CLAUDE.md                      # Complete knowledge base ⭐
│   └── README.md                      # This file (updated 2025-08-26)
└── 🚀 Scripts
    └── run_pipeline.sh                # DBT pipeline runner
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Amazon Redshift Serverless access
- dbt-core and dbt-redshift

### 1. Environment Setup

```bash
# Clone the repository
git clone <your-repo-url>
cd Redshift-DBT

# Activate CDC environment
source cdc_env/bin/activate

# Install dbt dependencies
dbt deps
```

### 2. Test Connection

```bash
# Test database connectivity
dbt debug
```

### 3. Run CDC Processing

```bash
# Run complete CDC with surgical precision
python complete_cdc_processor.py

# Alternative: Run basic DBT pipeline
./run_pipeline.sh
```

## 🧠 Intelligent CDC System

### 🔬 Surgical Change Detection

```python
# Change type detection
if created_at == updated_at:
    change_type = 'INSERT'  # New record
else:
    change_type = 'UPDATE'  # Modified record
    # Compare staging.column vs public.column for EVERY column
```

### 📊 3-Phase Processing

1. **🔍 Phase 1**: Surgical change detection (staging vs public)
2. **🎯 Phase 2**: Targeted fact table updates (dictionary-driven)
3. **🔄 Phase 3**: Public schema synchronization (maintain baseline)

### 🎯 Dictionary-Driven Mapping

```python
# Example mapping from expanded_dictionary.py
"companies": {
    "name": {
        "fact_vendor": "vendor_name",
        "fact_company_profile": "company_name"
    },
    "email": {
        "fact_vendor": "vendor_email", 
        "fact_company_profile": "contact_email"
    }
}
```

## 📊 Database Configuration

### Connection Details
- **Host**: `default-workgroup.885373794985.ap-south-1.redshift-serverless.amazonaws.com`
- **Port**: 5439
- **Database**: `dev`
- **User**: `admin`

### Schema Structure
- **`staging`**: 12 source tables (last 30 minutes of changes)
- **`public`**: 12 baseline tables (full historical data for comparison)
- **`staging_public`**: Fact tables (production output)

## 🗂️ Data Model

### Source Tables (12)
1. `companies` - Company information (45 columns)
2. `buyer_seller_company_mappings` - Vendor relationships (14 columns)
3. `users` - User profiles and POCs (42 columns)
4. `teams` - Company teams and categories
5. `team_members` - Team membership
6. `cities` - Geographic city data
7. `countries` - Country information
8. `product_categories` - Product categorization
9. `user_company_mappings` - User-company relationships
10. `taggings` - Tag associations
11. `tags` - Tag definitions
12. `preferred_vendor_item_mappings` - Preferred vendor items

### Fact Tables (staging_public)
- `fact_vendor` - Main vendor fact table (75 columns) ⭐
- `fact_company_profile` - Company profile data
- `fact_financial` - Financial information
- `fact_relationship` - Vendor relationships
- `fact_onboarding` - Onboarding tracking
- `fact_geography` - Location-based data

## 🔧 Business Rules

### Vendor Qualification
For companies to appear in fact_vendor:
- `companies.status = 1` (active)
- `companies.category IN (1,2,4)` (valid vendor types)
- Must have `buyer_seller_company_mappings` record with:
  - `client_company_id = 12855`
  - `status IN (1,2,7)`
  - `source IN (0,1)`
  - `invited_by IS NOT NULL`

## 📊 Performance Metrics

### Current Performance (Optimized 2025-08-26)
- **Records Analyzed**: 10-20 per run
- **Columns Compared**: ~111 per run (down from 578 - 80% improvement) ⚡
- **Changes Detected**: 50-100 column changes
- **Duration**: 60-90 seconds for full cycle
- **Total Efficiency Gain**: ~85% faster than original full refresh approach

### Optimization Evolution
- **Before CDC**: Full refresh all tables (~5 minutes)
- **After CDC**: Surgical updates only (~90 seconds) - 70% improvement
- **After Dictionary-First**: Targeted column comparison (~60 seconds) - 80% fewer comparisons

### Live Testing Results (2025-08-23)
- **✅ INSERT Test**: New company successfully added with surgical precision
- **✅ UPDATE Test**: 3/45 columns detected and updated (93% efficiency)
- **✅ Public Sync**: Redshift compatibility confirmed
- **✅ Fact Updates**: All mapped columns updated correctly

## ⚡ Latest Improvements (2025-08-26)

### 🎯 Dictionary-First Optimization
**Revolutionary Performance Enhancement**: Reduced column comparisons by 80%
- **Before**: 578 column comparisons per cycle
- **After**: 111 column comparisons per cycle
- **Implementation**: Smart filtering based on dictionary mappings
- **Result**: Faster processing with reduced database load

### 🏗️ Architectural Decision: Hybrid Approach
**Question Addressed**: "Can we use dictionary for direct updates instead of DBT?"

**Analysis**: Dictionary handles simple 1:1 mappings, but fact_vendor requires:
- Multi-condition CASE statements (mapping_status → joining_status_label)
- Function transformations (EXTRACT(EPOCH FROM created_at))
- Cross-table aggregations (LISTAGG for category_names)
- Composite key generation (vendor_id || '_' || poc_id)

**Conclusion**: Hybrid architecture is optimal:
- **Dictionary**: Surgical targeting (avoid unnecessary processing)
- **DBT**: Complex business logic (handle transformations)

### 🧹 Project Cleanup
**Removed unnecessary files for production readiness**:
- Deprecated processors (old CDC versions)
- Test files and build artifacts
- Empty folders and redundant documentation

## 🚨 System Status

### ✅ Fully Operational Features
- **Dictionary-First CDC**: 80% performance improvement implemented
- **Public Schema Sync**: Fixed Redshift compatibility (2025-08-23)
- **Column Comparison**: Type-safe comparison with surgical precision
- **Dictionary Mapping**: Comprehensive mappings validated
- **Project Structure**: Clean, production-ready organization

### ⚠️ Known Limitations
- **Fact Table Scope**: Currently focused on `fact_vendor` (100% operational)
- **Future Enhancement**: Expand to additional fact tables as needed

## 🔧 Advanced Usage

### Adding Test Data
```sql
-- Add new company
INSERT INTO staging.companies (name, email, ..., status, category) 
VALUES ('Test Company', 'test@company.com', ..., 1, 1);

-- Add vendor mapping
INSERT INTO staging.buyer_seller_company_mappings 
(client_company_id, dealing_with_company_id, status, source, invited_by)
VALUES (12855, [company_id], 1, 0, [user_id]);
```

### Monitoring
```bash
# Check staging data
SELECT COUNT(*) FROM staging.companies;

# Verify fact table updates
SELECT * FROM staging_public.fact_vendor WHERE vendor_id = [new_id];

# Monitor CDC performance
tail -f cdc.log
```

## 📈 Production Deployment

### Cron Schedule
```bash
# Every 30 minutes
*/30 * * * * cd /path/to/Redshift-DBT && source cdc_env/bin/activate && python complete_cdc_processor.py >> cdc.log 2>&1
```

### Monitoring Setup
- Monitor CDC processing duration
- Track column comparison counts
- Alert on fact table update failures
- Monitor public schema sync success rates

## 🤝 Contributing

### Development Workflow
1. **Test CDC locally**: Add test data → run processor → verify results
2. **Update dictionary**: Add new column mappings as needed
3. **Create new fact tables**: Add DBT models + dictionary entries
4. **Test surgical precision**: Verify only changed columns are processed

### Code Structure
- **CDC Engine**: `complete_cdc_processor.py` (main orchestrator)
- **Mappings**: `expanded_dictionary.py` (column-to-fact mappings)
- **DBT Models**: Standard dbt project structure
- **Documentation**: Keep `CLAUDE.md` updated with changes

## 📝 Documentation

- **📖 Complete Guide**: See `CLAUDE.md` for comprehensive technical documentation
- **🎯 Quick Reference**: This README for overview, setup, and latest updates
- **💡 Architecture Insights**: Both files updated with latest optimizations (2025-08-26)

## 📞 Support

For issues, questions, or feature requests:
1. Check the comprehensive `CLAUDE.md` documentation
2. Review error logs and CDC output
3. Test with staging data insertion
4. Verify database connectivity and permissions

## 📜 License

This project is for internal use. Ensure compliance with your organization's data handling and security policies.

---

**🎯 Ready to experience surgical precision data processing with 80% optimized efficiency? Start with the Quick Start guide above!** 🔬⚡✅

*Last Updated: 2025-08-26 - Dictionary-First Optimization & Architecture Refinement*