"""
Fact Updater Module
Handles DBT fact table updates with surgical precision targeting
"""

import subprocess
from typing import Dict, List


class FactUpdater:
    def __init__(self):
        """Initialize fact table updater"""
        self.updated_tables = []
        self.failed_tables = []
        
    def execute_fact_table_update(self, fact_table: str) -> bool:
        """Execute fact table update using dbt"""
        try:
            print(f"   🔄 Running dbt for {fact_table}...")
            cmd = f"dbt run --select {fact_table} --target dev"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                print(f"   ✅ Successfully updated {fact_table}")
                self.updated_tables.append(fact_table)
                return True
            else:
                print(f"   ❌ Failed to update {fact_table}")
                print(f"   📝 Error: {result.stderr}")
                self.failed_tables.append(fact_table)
                return False
                
        except subprocess.TimeoutExpired:
            print(f"   ⏰ Timeout updating {fact_table} (5 minutes)")
            self.failed_tables.append(fact_table)
            return False
        except Exception as e:
            print(f"   ❌ Exception updating {fact_table}: {e}")
            self.failed_tables.append(fact_table)
            return False
    
    def update_targeted_fact_tables(self, targeted_updates: Dict[str, List[str]]) -> Dict:
        """Update fact tables based on targeted changes"""
        if not targeted_updates:
            print("⚡ No fact table updates needed - no relevant changes detected")
            return {
                "fact_tables_processed": 0,
                "successful_updates": 0,
                "failed_updates": 0,
                "updated_tables": [],
                "failed_tables": []
            }
        
        print(f"🎯 Phase 2: Updating {len(targeted_updates)} fact tables...")
        
        success_count = 0
        failure_count = 0
        
        for fact_table, affected_columns in targeted_updates.items():
            print(f"📊 Processing {fact_table} (columns: {', '.join(affected_columns)})")
            
            if self.execute_fact_table_update(fact_table):
                success_count += 1
            else:
                failure_count += 1
        
        result = {
            "fact_tables_processed": len(targeted_updates),
            "successful_updates": success_count,
            "failed_updates": failure_count,
            "updated_tables": self.updated_tables.copy(),
            "failed_tables": self.failed_tables.copy(),
            "targeted_updates": targeted_updates
        }
        
        # Clear for next run
        self.updated_tables.clear()
        self.failed_tables.clear()
        
        return result
    
    def run_full_refresh(self, fact_table: str) -> bool:
        """Run full refresh for a specific fact table"""
        try:
            print(f"   🔄 Running full refresh for {fact_table}...")
            cmd = f"dbt run --select {fact_table} --full-refresh --target dev"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=600)
            
            if result.returncode == 0:
                print(f"   ✅ Successfully refreshed {fact_table}")
                return True
            else:
                print(f"   ❌ Failed to refresh {fact_table}")
                print(f"   📝 Error: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print(f"   ⏰ Timeout refreshing {fact_table} (10 minutes)")
            return False
        except Exception as e:
            print(f"   ❌ Exception refreshing {fact_table}: {e}")
            return False
    
    def test_fact_tables(self, fact_tables: List[str] = None) -> Dict:
        """Run dbt tests on fact tables"""
        try:
            if fact_tables:
                # Test specific tables
                tables_selector = " ".join(fact_tables)
                cmd = f"dbt test --select {tables_selector} --target dev"
            else:
                # Test all models
                cmd = "dbt test --target dev"
                
            print(f"   🧪 Running tests: {cmd}")
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
            
            return {
                "success": result.returncode == 0,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "return_code": result.returncode
            }
            
        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "error": "Test timeout (5 minutes)",
                "return_code": -1
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "return_code": -1
            }