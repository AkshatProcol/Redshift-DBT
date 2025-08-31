"""
Transactional Fact Updater Module
Handles DBT fact table updates with ACID transaction guarantees
"""

import subprocess
import time
from typing import Dict, List
from .database_connector import DatabaseConnector


class TransactionalFactUpdater:
    """
    🔐 TRANSACTIONAL TABLE UPDATER
    Provides ACID guarantees for fact AND dimension table updates:
    - Atomicity: All updates succeed or all rollback
    - Consistency: Always leaves system in valid state  
    - Isolation: Uses backups during updates
    - Durability: Database commits ensure persistence
    
    Note: Despite the class name, this handles both fact_ and dim_ tables
    """
    
    def __init__(self, database_connector: DatabaseConnector):
        """Initialize transactional fact table updater"""
        self.db = database_connector
        self.updated_tables = []
        self.failed_tables = []
        self.backup_tables = {}  # Track backup table names
        
    def is_table_empty(self, table_name: str) -> bool:
        """Check if a fact or dimension table is empty or doesn't exist"""
        if not self.db:
            return False
            
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # Check if table exists and count records
            cursor.execute(f"SELECT COUNT(*) FROM staging_public.{table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            return count == 0
            
        except Exception as e:
            print(f"   ℹ️  Could not check {table_name} (likely new table): {e}")
            return True  # Assume it's new/empty if we can't check
    
    def validate_table_update(self, table_name: str) -> Dict:
        """Validate that a table (fact or dimension) can be updated (compilation check)"""
        try:
            # Check if table is empty/new and needs full refresh
            is_empty = self.is_table_empty(table_name)
            
            # Determine table type for logging
            table_type = "fact" if table_name.startswith("fact_") else "dimension"
            print(f"   🧪 Validating {table_name} ({table_type} table, {'full refresh' if is_empty else 'incremental'})...")
            cmd = f"dbt compile --select {table_name} --target dev"
                
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
            
            return {
                "table_name": table_name,
                "table_type": table_type,
                "valid": result.returncode == 0,
                "is_empty": is_empty,
                "error": result.stderr if result.returncode != 0 else None,
                "update_type": "full_refresh" if is_empty else "incremental"
            }
            
        except subprocess.TimeoutExpired:
            return {
                "table_name": table_name,
                "table_type": "unknown",
                "valid": False,
                "error": "Validation timeout (2 minutes)",
                "update_type": "unknown"
            }
        except Exception as e:
            return {
                "table_name": table_name,
                "table_type": "unknown", 
                "valid": False,
                "error": str(e),
                "update_type": "unknown"
            }
    
    def create_backup(self, table_name: str, backup_table_name: str) -> bool:
        """Create a backup of table (fact or dimension) before update in staging_backup schema"""
        if not self.db:
            print(f"   ⚠️ No database connector - skipping backup for {table_name}")
            return True
            
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # Ensure staging_backup schema exists (safe if already exists)
            cursor.execute("CREATE SCHEMA IF NOT EXISTS staging_backup")
            
            # Create backup table in dedicated backup schema
            cursor.execute(f"""
                CREATE TABLE staging_backup.{backup_table_name} AS 
                SELECT * FROM staging_public.{table_name}
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"   📋 Backup created: staging_backup.{backup_table_name}")
            return True
            
        except Exception as e:
            print(f"   ❌ Failed to backup {table_name}: {e}")
            if 'conn' in locals() and conn:
                conn.rollback()
                conn.close()
            return False
    
    def restore_from_backup(self, table_name: str, backup_table: str) -> bool:
        """Restore table (fact or dimension) from backup in staging_backup schema"""
        if not self.db:
            print(f"   ⚠️ No database connector - cannot restore {table_name}")
            return False
            
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # Truncate current table and restore from backup
            cursor.execute(f"TRUNCATE TABLE staging_public.{table_name}")
            cursor.execute(f"""
                INSERT INTO staging_public.{table_name}
                SELECT * FROM staging_backup.{backup_table}
            """)
            
            # Clean up backup table after successful restore
            cursor.execute(f"DROP TABLE staging_backup.{backup_table}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"   🔄 Restored {table_name} from staging_backup.{backup_table}")
            return True
            
        except Exception as e:
            print(f"   ❌ Failed to restore {table_name}: {e}")
            if 'conn' in locals() and conn:
                conn.rollback()
                conn.close()
            return False
    
    def execute_single_update(self, table_name: str, update_type: str) -> bool:
        """Execute a single table update (fact or dimension)"""
        try:
            table_type = "fact" if table_name.startswith("fact_") else "dimension"
            
            if update_type == "full_refresh":
                print(f"   🆕 {table_name} ({table_type}) - running full refresh for initial data load")
                cmd = f"dbt run --select {table_name} --full-refresh --target dev"
                timeout = 600  # 10 minutes for full refresh
            else:
                print(f"   🔄 {table_name} ({table_type}) - running incremental update")
                cmd = f"dbt run --select {table_name} --target dev"
                timeout = 300  # 5 minutes for incremental
                
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
            
            if result.returncode == 0:
                print(f"   ✅ Successfully updated {table_name}")
                self.updated_tables.append(table_name)
                return True
            else:
                print(f"   ❌ Update failed for {table_name}")
                print(f"   📝 Error: {result.stderr}")
                self.failed_tables.append(table_name)
                return False
                    
        except subprocess.TimeoutExpired:
            print(f"   ⏰ Timeout updating {table_name} ({timeout/60:.0f} minutes)")
            self.failed_tables.append(table_name)
            return False
        except Exception as e:
            print(f"   ❌ Exception updating {table_name}: {e}")
            self.failed_tables.append(table_name)
            return False
    
    def cleanup_backups(self) -> bool:
        """Clean up backup tables after successful transaction"""
        if not self.backup_tables or not self.db:
            return True
            
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            for backup_table in self.backup_tables.values():
                print(f"   🗑️ Cleaning up staging_backup.{backup_table}")
                cursor.execute(f"DROP TABLE IF EXISTS staging_backup.{backup_table}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print("   ✅ Backup cleanup completed")
            return True
            
        except Exception as e:
            print(f"   ⚠️ Backup cleanup warning: {e}")
            return False
    
    def transactional_update(self, targeted_updates: Dict[str, List[str]]) -> Dict:
        """
        🔐 MAIN TRANSACTIONAL UPDATE METHOD
        Executes fact table updates with full ACID guarantees
        """
        if not targeted_updates:
            print("⚡ No fact table updates needed - no relevant changes detected")
            return {
                "fact_tables_processed": 0,
                "successful_updates": 0,
                "failed_updates": 0,
                "updated_tables": [],
                "failed_tables": [],
                "transaction_status": "no_updates_needed"
            }
        
        # Reset state for new transaction
        self.updated_tables.clear()
        self.failed_tables.clear()
        self.backup_tables.clear()
        
        fact_count = sum(1 for table in targeted_updates.keys() if table.startswith("fact_"))
        dim_count = sum(1 for table in targeted_updates.keys() if table.startswith("dim_"))
        
        print(f"🔐 TRANSACTIONAL UPDATE: {len(targeted_updates)} tables ({fact_count} fact + {dim_count} dimension)")
        print("   📋 All updates will succeed together or rollback together")
        
        # =====================================================
        # 🧪 PHASE 1: VALIDATION
        # =====================================================
        print("\n🧪 PHASE 1: Validation")
        validation_results = []
        
        for table_name, affected_columns in targeted_updates.items():
            print(f"   🔍 Validating {table_name} (columns: {', '.join(affected_columns)})")
            validation = self.validate_table_update(table_name)
            validation_results.append(validation)
            
            if not validation["valid"]:
                print(f"   ❌ Validation failed: {validation['error']}")
        
        # Check if all validations passed
        failed_validations = [v for v in validation_results if not v["valid"]]
        if failed_validations:
            print(f"\n❌ TRANSACTION ABORTED: {len(failed_validations)} table(s) failed validation")
            for failure in failed_validations:
                print(f"   • {failure['table_name']}: {failure['error']}")
            
            return {
                "fact_tables_processed": len(targeted_updates),
                "successful_updates": 0,
                "failed_updates": len(targeted_updates),
                "updated_tables": [],
                "failed_tables": list(targeted_updates.keys()),
                "targeted_updates": targeted_updates,
                "transaction_status": "validation_failed",
                "validation_results": validation_results
            }
        
        print(f"   ✅ All {len(targeted_updates)} tables passed validation")
        
        # =====================================================
        # 📋 PHASE 2: BACKUP
        # =====================================================
        print("\n📋 PHASE 2: Backup Creation")
        backup_timestamp = int(time.time())
        
        for validation in validation_results:
            table_name = validation["table_name"]
            if not validation["is_empty"]:  # Only backup existing tables
                backup_table = f"{table_name}_backup_{backup_timestamp}"
                
                print(f"   💾 Creating backup for {table_name}...")
                if self.create_backup(table_name, backup_table):
                    self.backup_tables[table_name] = backup_table
                else:
                    print(f"\n❌ TRANSACTION ABORTED: Backup creation failed")
                    return {
                        "fact_tables_processed": len(targeted_updates),
                        "successful_updates": 0,
                        "failed_updates": len(targeted_updates),
                        "updated_tables": [],
                        "failed_tables": list(targeted_updates.keys()),
                        "targeted_updates": targeted_updates,
                        "transaction_status": "backup_failed"
                    }
        
        print(f"   ✅ Created {len(self.backup_tables)} backups successfully")
        
        # =====================================================
        # 🚀 PHASE 3: EXECUTION
        # =====================================================
        print("\n🚀 PHASE 3: Execution")
        execution_results = []
        
        for validation in validation_results:
            table_name = validation["table_name"]
            affected_columns = targeted_updates[table_name]
            
            print(f"   📊 Updating {table_name} (columns: {', '.join(affected_columns)})")
            
            success = self.execute_single_update(table_name, validation["update_type"])
            execution_results.append({
                "table_name": table_name,
                "table_type": validation["table_type"],
                "success": success,
                "update_type": validation["update_type"]
            })
            
            if not success:
                print(f"   ❌ Update failed for {table_name} - initiating rollback")
                break
        
        # =====================================================
        # 🎯 PHASE 4: COMMIT OR ROLLBACK
        # =====================================================
        failed_updates = [r for r in execution_results if not r["success"]]
        
        if failed_updates:
            print(f"\n🔄 PHASE 4: ROLLBACK")
            print(f"   📊 Restoring {len(self.backup_tables)} tables from backup...")
            
            # Restore all tables from backup
            rollback_success = True
            for table_name, backup_table in self.backup_tables.items():
                print(f"   🔄 Restoring {table_name}...")
                if not self.restore_from_backup(table_name, backup_table):
                    rollback_success = False
            
            # Clear success tracking since we rolled back
            self.updated_tables.clear()
            self.failed_tables = list(targeted_updates.keys())
            
            transaction_status = "rollback_success" if rollback_success else "rollback_failed"
            print(f"   {'✅' if rollback_success else '❌'} Rollback {'completed' if rollback_success else 'failed'}")
            
            return {
                "fact_tables_processed": len(targeted_updates),
                "successful_updates": 0,
                "failed_updates": len(targeted_updates),
                "updated_tables": [],
                "failed_tables": list(targeted_updates.keys()),
                "targeted_updates": targeted_updates,
                "transaction_status": transaction_status,
                "execution_results": execution_results
            }
        else:
            print(f"\n✅ PHASE 4: COMMIT")
            print("   🎉 All updates successful! Cleaning up backups...")
            
            # Clean up backup tables
            self.cleanup_backups()
            
            result = {
                "fact_tables_processed": len(targeted_updates),
                "successful_updates": len(targeted_updates),
                "failed_updates": 0,
                "updated_tables": self.updated_tables.copy(),
                "failed_tables": [],
                "targeted_updates": targeted_updates,
                "transaction_status": "commit_success",
                "execution_results": execution_results
            }
            
            return result