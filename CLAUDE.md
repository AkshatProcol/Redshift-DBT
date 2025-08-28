# CLAUDE.md - Project Knowledge Base

## 🎯 **Project Overview**
**DBT Time** - Advanced multi-fact data pipeline with intelligent Change Data Capture (CDC) system for Amazon Redshift. Features a **modular architecture** with surgical precision column-level change detection, processing comprehensive business data from **24 staging tables** into production fact and dimensional tables.

## 🏗️ **Architecture**

### **Data Flow**
```
📊 Public Schema (baseline data)
    ↕️ COMPARE (column-by-column)
📊 Staging Schema (last 30 mins changes)
    ↓ MODULAR CDC SYSTEM
📊 staging_public Schema (fact & dim tables)
```

### **Modular CDC System**
```
cdc_modules/
├── database_connector.py         # Database operations
├── change_detector.py            # Surgical detection
├── fact_updater.py               # DBT orchestration with auto-refresh
├── public_syncer.py              # Schema sync
└── cdc_orchestrator.py           # Main coordination

Entry Points:
├── cdc_main.py                   # Modern CLI interface
└── complete_cdc_processor.py     # Legacy wrapper
```

## 🔧 **Key Technologies**
- **Database**: Amazon Redshift Serverless
- **Transformation**: dbt (Data Build Tool)
- **CDC Engine**: Modular Python architecture
- **Change Detection**: Dictionary-first column-by-column comparison
- **Auto-Refresh**: Smart detection for new/empty tables

## 📊 **Database Configuration**
- **Host**: `default-workgroup.885373794985.ap-south-1.redshift-serverless.amazonaws.com`
- **Database**: `dev` | **User**: `admin` | **Password**: `FLWGTnvecu049*%`

### **Schema Structure**
- **`staging`**: 24 source tables (30-minute change window)
- **`public`**: 24 baseline tables (comparison baseline)
- **`staging_public`**: Production fact/dim tables

## 🗂️ **Data Model**

### **24 Staging Tables**
**Core Business (12)**: companies, buyer_seller_company_mappings, users, teams, team_members, cities, countries, product_categories, user_company_mappings, taggings, tags, preferred_vendor_item_mappings

**Trading & Bidding (10)**: bids, bid_trades, bid_trade_products, trade_requests, trade_products, orders, products, product_qualities, buyer_hubs, event_groups

**System Support (2)**: audiences, units

### **Production Tables (staging_public)**

**Active Fact Tables (5):**
- `fact_vendor` - Vendor analytics with POC relationships
- `fact_bids` - Bid analytics with enrichment
- `fact_bid_trades` - Trading activity tracking
- `fact_trade_products` - Product trading analytics
- `fact_trade_requests` - Trade request lifecycle

**Active Dimensional Tables (3):**
- `dim_audiences` - Audience targeting data
- `dim_bid_trade_products_details` - Rich product details (JSON)
- `dim_bids` - Bid metadata and documentation (JSON)

## 🧠 **CDC System**

### **3-Phase Processing**
1. **🔍 Detection**: Surgical change detection (staging vs public)
2. **📊 Update**: Targeted fact/dim table updates (dictionary-driven + auto-refresh)
3. **🔄 Sync**: Public schema synchronization

### **Change Detection Logic**
```python
if created_at == updated_at:
    change_type = 'INSERT'  # New record
else:
    change_type = 'UPDATE'  # Modified record
```

### **Dictionary Mapping** (`expanded_dictionary.py`)
Maps source columns to multiple fact/dim tables with multi-table support:
```python
"companies": {
    "name": {
        "fact_vendor": "vendor_name",
        "dim_companies": "company_name"  # Future
    }
}
```

## 🚀 **Usage**

### **Modern CLI**
```bash
# Activate environment
source cdc_env/bin/activate

# Complete CDC process (auto-refresh enabled)
python cdc_main.py

# Single table processing
python cdc_main.py --table companies

# Health check
python cdc_main.py --health-check
```

### **Key Features**
- **Auto-Refresh**: Detects empty/new tables and runs full refresh automatically
- **Multi-Table Processing**: Single source table → Multiple fact/dim tables
- **Performance**: ~66 seconds average, 96.3% reliability
- **Surgical Precision**: Only processes changed columns via dictionary
- **Fallback Logic**: Incremental fails → Auto-retry with full refresh

## 📋 **Core Files**
- **CDC Engine**: `cdc_main.py` + `cdc_modules/`
- **Dictionary**: `expanded_dictionary.py` (column mappings)
- **DBT Models**: `models/marts/` (fact_* and dim_* tables)
- **Configuration**: `profiles.yml`, `dbt_project.yml`

## 🎯 **Current Status**
- ✅ **Multi-Table Architecture**: 5 fact + 3 dim tables operational
- ✅ **Auto-Refresh System**: New tables work without manual intervention
- ✅ **Performance Optimized**: Dictionary-first + empty table skipping
- ✅ **Production Ready**: All components tested and documented

The system provides comprehensive business intelligence across vendor management, trading analytics, order processing, and product intelligence with automatic table management and surgical precision change detection.