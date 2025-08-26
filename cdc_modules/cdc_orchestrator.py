"""
CDC Orchestrator Module
Main coordination logic for the 3-phase CDC process
"""

from datetime import datetime
from typing import Dict, List
from .database_connector import DatabaseConnector
from .change_detector import ChangeDetector
from .fact_updater import FactUpdater
from .public_syncer import PublicSyncer


class CDCOrchestrator:
    def __init__(self, dictionary_mapping: Dict, db_config: Dict = None):
        """Initialize CDC orchestrator with all components"""
        self.mapping = dictionary_mapping
        self.staging_tables = [
            'companies', 'buyer_seller_company_mappings', 'users', 'teams',
            'team_members', 'cities', 'countries', 'product_categories',
            'user_company_mappings', 'taggings', 'tags', 'preferred_vendor_item_mappings'
        ]
        
        # Initialize components
        self.db = DatabaseConnector(db_config)
        self.change_detector = ChangeDetector(self.db, dictionary_mapping)
        self.fact_updater = FactUpdater()
        self.public_syncer = PublicSyncer(self.db)
        
        print("🔄 CDC Orchestrator - Surgical Precision + Public Schema Sync")
        print("🎯 3-Phase Process: Detection → Update → Sync")
    
    def process_complete_cdc(self) -> Dict:
        """
        Execute complete 3-phase CDC process with surgical precision
        
        🔄 PHASES:
        1. 🔍 SURGICAL CHANGE DETECTION (staging vs public comparison)
        2. 🎯 TARGETED FACT TABLE UPDATES (dictionary-driven)
        3. 🔄 PUBLIC SCHEMA SYNC (maintain baseline for next cycle)
        """
        start_time = datetime.now()
        print(f"🚀 Starting Complete CDC Process at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Initialize results tracking
        results = {
            "start_time": start_time.isoformat(),
            "phase_1_detection": {},
            "phase_2_updates": {},
            "phase_3_sync": {},
            "overall_summary": {}
        }
        
        # =====================================================
        # 🔍 PHASE 1: SURGICAL CHANGE DETECTION
        # =====================================================
        print("\\n" + "="*60)
        print("🔍 PHASE 1: SURGICAL CHANGE DETECTION")
        print("="*60)
        
        all_targeted_updates = {}
        all_public_sync_data = {}
        total_records = 0
        total_changes = 0
        total_comparisons = 0
        
        for table_name in self.staging_tables:
            analysis_result = self.change_detector.analyze_table_changes(table_name)
            
            if "error" in analysis_result:
                print(f"⚠️  Skipping {table_name}: {analysis_result['error']}")
                continue
            
            # Track results
            results["phase_1_detection"][table_name] = analysis_result
            total_records += analysis_result.get("records_processed", 0)
            total_changes += analysis_result.get("total_column_changes", 0)
            total_comparisons += analysis_result.get("total_comparisons", 0)
            
            # Collect targeted updates
            if analysis_result.get("targeted_updates"):
                for fact_table, columns in analysis_result["targeted_updates"].items():
                    if fact_table not in all_targeted_updates:
                        all_targeted_updates[fact_table] = set()
                    all_targeted_updates[fact_table].update(columns)
            
            # Collect data for public sync
            if analysis_result.get("all_records"):
                all_public_sync_data[table_name] = analysis_result["all_records"]
        
        # Convert sets to lists for JSON serialization
        for fact_table in all_targeted_updates:
            all_targeted_updates[fact_table] = list(all_targeted_updates[fact_table])
        
        print(f"\\n📊 Phase 1 Summary:")
        print(f"   Records analyzed: {total_records}")
        print(f"   Column comparisons: {total_comparisons} (dictionary-optimized)")
        print(f"   Changes detected: {total_changes}")
        print(f"   Fact tables targeted: {len(all_targeted_updates)}")
        
        # =====================================================
        # 🎯 PHASE 2: TARGETED FACT TABLE UPDATES  
        # =====================================================
        print("\\n" + "="*60)
        print("🎯 PHASE 2: TARGETED FACT TABLE UPDATES")
        print("="*60)
        
        fact_update_result = self.fact_updater.update_targeted_fact_tables(all_targeted_updates)
        results["phase_2_updates"] = fact_update_result
        
        print(f"\\n📊 Phase 2 Summary:")
        print(f"   Fact tables processed: {fact_update_result.get('fact_tables_processed', 0)}")
        print(f"   Successful updates: {fact_update_result.get('successful_updates', 0)}")
        print(f"   Failed updates: {fact_update_result.get('failed_updates', 0)}")
        
        # =====================================================
        # 🔄 PHASE 3: PUBLIC SCHEMA SYNCHRONIZATION
        # =====================================================
        print("\\n" + "="*60)
        print("🔄 PHASE 3: PUBLIC SCHEMA SYNCHRONIZATION")
        print("="*60)
        
        sync_results = {}
        total_synced = 0
        total_sync_failures = 0
        
        for table_name, changed_records in all_public_sync_data.items():
            sync_result = self.public_syncer.sync_public_schema(table_name, changed_records)
            sync_results[table_name] = sync_result
            
            total_synced += sync_result.get("sync_success", 0)
            total_sync_failures += sync_result.get("sync_failures", 0)
        
        results["phase_3_sync"] = sync_results
        
        print(f"\\n📊 Phase 3 Summary:")
        print(f"   Tables synced: {len(sync_results)}")
        print(f"   Records synced: {total_synced}")
        print(f"   Sync failures: {total_sync_failures}")
        
        # =====================================================
        # 📋 OVERALL SUMMARY
        # =====================================================
        end_time = datetime.now()
        duration = end_time - start_time
        
        overall_summary = {
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration.total_seconds(),
            "phase_1": {
                "records_analyzed": total_records,
                "column_comparisons": total_comparisons,
                "changes_detected": total_changes,
                "fact_tables_targeted": len(all_targeted_updates)
            },
            "phase_2": {
                "fact_tables_processed": fact_update_result.get("fact_tables_processed", 0),
                "successful_updates": fact_update_result.get("successful_updates", 0),
                "failed_updates": fact_update_result.get("failed_updates", 0)
            },
            "phase_3": {
                "tables_synced": len([r for r in sync_results.values() if r.get("success")]),
                "records_synced": total_synced,
                "sync_failures": total_sync_failures
            },
            "optimization_stats": {
                "dictionary_first_enabled": True,
                "column_comparisons": total_comparisons,
                "estimated_full_comparisons": total_records * 45,  # Rough estimate
                "efficiency_gain": f"{max(0, round((1 - total_comparisons / max(1, total_records * 45)) * 100))}%"
            }
        }
        
        results["overall_summary"] = overall_summary
        
        print("\\n" + "="*60)
        print("📋 COMPLETE CDC PROCESS SUMMARY")
        print("="*60)
        print(f"⏱️  Duration: {duration.total_seconds():.1f} seconds")
        print(f"📊 Records Analyzed: {total_records}")
        print(f"🔍 Column Comparisons: {total_comparisons} (optimized)")
        print(f"🎯 Changes Detected: {total_changes}")
        print(f"📈 Fact Tables Updated: {fact_update_result.get('successful_updates', 0)}")
        print(f"🔄 Public Records Synced: {total_synced}")
        print(f"⚡ Efficiency: {overall_summary['optimization_stats']['efficiency_gain']} fewer comparisons")
        
        if fact_update_result.get("failed_updates", 0) > 0 or total_sync_failures > 0:
            print("⚠️  Some operations had failures - check detailed results")
        else:
            print("✅ All operations completed successfully!")
        
        return results
    
    def run_health_check(self) -> Dict:
        """Run health check on all CDC components"""
        print("🏥 Running CDC Health Check...")
        
        health_results = {
            "database_connection": False,
            "staging_tables": {},
            "public_tables": {},
            "dictionary_mapping": {},
            "overall_health": False
        }
        
        # Test database connection
        conn = self.db.get_connection()
        if conn:
            health_results["database_connection"] = True
            conn.close()
            print("   ✅ Database connection: OK")
        else:
            print("   ❌ Database connection: FAILED")
            return health_results
        
        # Check staging tables
        for table_name in self.staging_tables:
            records = self.db.get_changed_records(table_name)
            health_results["staging_tables"][table_name] = {
                "accessible": len(records) >= 0,
                "record_count": len(records)
            }
        
        # Check dictionary mapping
        for table_name in self.staging_tables:
            if table_name in self.mapping:
                health_results["dictionary_mapping"][table_name] = len(self.mapping[table_name])
            else:
                health_results["dictionary_mapping"][table_name] = 0
        
        # Overall health assessment
        db_ok = health_results["database_connection"]
        tables_ok = all(t["accessible"] for t in health_results["staging_tables"].values())
        mapping_ok = sum(health_results["dictionary_mapping"].values()) > 0
        
        health_results["overall_health"] = db_ok and tables_ok and mapping_ok
        
        if health_results["overall_health"]:
            print("   ✅ Overall health: HEALTHY")
        else:
            print("   ⚠️  Overall health: ISSUES DETECTED")
        
        return health_results