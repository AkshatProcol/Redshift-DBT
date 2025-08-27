"""
Change Detector Module  
Surgical precision change detection with dictionary-first optimization
"""

from typing import Dict, List, Set, Tuple, Any
from .database_connector import DatabaseConnector


class ChangeDetector:
    def __init__(self, db_connector: DatabaseConnector, dictionary_mapping: Dict):
        """Initialize change detector with database connector and dictionary"""
        self.db = db_connector
        self.mapping = dictionary_mapping
        
    def get_dictionary_columns(self, table_name: str) -> Set[str]:
        """Get only columns that are mapped in dictionary - OPTIMIZATION"""
        if table_name not in self.mapping:
            return set()
        
        dictionary_columns = set(self.mapping[table_name].keys())
        print(f"      📚 Dictionary columns for {table_name}: {len(dictionary_columns)} (vs all columns)")
        return dictionary_columns
    
    def compare_record_columns(self, table_name: str, staging_record: Dict, table_columns: List[Dict]) -> Tuple[List[str], int]:
        """Compare each column of staging record with public schema - HYBRID APPROACH"""
        # 🚀 OPTIMIZATION: Get dictionary-mapped columns first
        dictionary_columns = self.get_dictionary_columns(table_name)
        
        if staging_record.get('is_new_record', False):
            # New record - only return dictionary-mapped columns as "fact table changes"
            # But public sync will handle ALL columns
            changed_cols = [col['name'] for col in table_columns 
                           if col['name'] in dictionary_columns and 
                           col['name'] not in ['created_at', 'updated_at', 'change_type', 'is_new_record']]
            return changed_cols, len(changed_cols)
        
        # Get corresponding record from public schema
        public_record = self.db.get_public_record(table_name, staging_record['id'])
        if not public_record:
            # No public record - only return dictionary-mapped columns as "fact table changes"
            changed_cols = [col['name'] for col in table_columns 
                           if col['name'] in dictionary_columns and 
                           col['name'] not in ['created_at', 'updated_at', 'change_type', 'is_new_record']]
            return changed_cols, len(changed_cols)
        
        # 🔄 HYBRID APPROACH: 
        # 1. Only compare dictionary columns for FACT TABLE updates (optimization)
        # 2. But public sync will handle ALL columns (data consistency)
        
        fact_table_changes = []  # Only dictionary-mapped columns
        columns_compared = 0
        
        # 🚀 OPTIMIZATION: Only compare dictionary-mapped columns for fact table targeting
        for col in table_columns:
            col_name = col['name']
            if col_name in ['created_at', 'updated_at', 'change_type', 'is_new_record']:
                continue
            
            # SKIP non-dictionary columns for FACT TABLE updates
            if col_name not in dictionary_columns:
                continue
                
            columns_compared += 1
            staging_value = staging_record.get(col_name)
            public_value = public_record.get(col_name)
            
            if self.values_different(staging_value, public_value, col['type']):
                fact_table_changes.append(col_name)
        
        print(f"      ⚡ Optimized: compared {columns_compared} dictionary columns for fact updates (vs {len(table_columns)} total)")
        print(f"      📊 Note: Public sync will handle ALL columns for data consistency")
        return fact_table_changes, columns_compared
    
    def values_different(self, staging_val: Any, public_val: Any, data_type: str) -> bool:
        """Compare two values considering data types and NULL handling"""
        # Handle None/NULL values
        if staging_val is None and public_val is None:
            return False
        if staging_val is None or public_val is None:
            return True
        
        # Convert boolean values for comparison
        if data_type.lower() in ['boolean', 'bool']:
            return bool(staging_val) != bool(public_val)
        
        # Convert to string for safe comparison
        return str(staging_val) != str(public_val)
    
    def get_targeted_fact_updates(self, table_name: str, changed_columns: List[str]) -> Dict[str, List[str]]:
        """Get targeted fact table updates based on changed columns"""
        if table_name not in self.mapping:
            return {}
        
        table_mapping = self.mapping[table_name]
        targeted_updates = {}
        
        # For each changed column, see which fact tables it maps to
        for column in changed_columns:
            if column in table_mapping:
                fact_mappings = table_mapping[column]
                
                for fact_table, fact_columns in fact_mappings.items():
                    if fact_table not in targeted_updates:
                        targeted_updates[fact_table] = []
                    
                    if isinstance(fact_columns, str):
                        if ',' in fact_columns:
                            targeted_updates[fact_table].extend(fact_columns.split(','))
                        else:
                            targeted_updates[fact_table].append(fact_columns)
                    else:
                        targeted_updates[fact_table].append(str(fact_columns))
        
        # Remove duplicates
        for fact_table in targeted_updates:
            targeted_updates[fact_table] = [col.strip() for col in set(targeted_updates[fact_table])]
        
        return targeted_updates
    
    def analyze_table_changes(self, table_name: str) -> Dict:
        """Analyze all changes for a specific table"""
        print(f"🔍 Analyzing changes in {table_name}...")
        
        # Get table structure and records
        columns = self.db.get_table_structure(table_name)
        if not columns:
            return {"error": f"Could not get structure for {table_name}"}
        
        changed_records = self.db.get_changed_records(table_name)
        if not changed_records:
            print(f"   ✅ No records found in staging.{table_name} - SKIPPING (performance optimization)")
            return {
                "records_processed": 0, 
                "changes_detected": 0,
                "total_comparisons": 0,
                "table_skipped": True,
                "skip_reason": "empty_staging_table"
            }
        
        # Analyze each record
        total_changes = 0
        total_comparisons = 0
        targeted_updates = {}
        records_with_changes = []
        
        for record in changed_records:
            record_id = record.get('id', 'unknown')
            change_type = record.get('change_type', 'unknown')
            
            print(f"   📋 Record {record_id} ({change_type}):")
            
            
            # Compare columns for this record
            changed_columns, comparisons = self.compare_record_columns(table_name, record, columns)
            
            if changed_columns:
                print(f"      🎯 Changed columns: {changed_columns}")
                
                # Get targeted fact table updates
                record_targets = self.get_targeted_fact_updates(table_name, changed_columns)
                
                # Merge with overall targets
                for fact_table, fact_columns in record_targets.items():
                    if fact_table not in targeted_updates:
                        targeted_updates[fact_table] = set()
                    targeted_updates[fact_table].update(fact_columns)
                
                records_with_changes.append({
                    'record_id': record_id,
                    'change_type': change_type,
                    'changed_columns': changed_columns,
                    'targeted_updates': record_targets
                })
            else:
                print(f"      ✅ No dictionary-mapped changes detected")
            
            total_changes += len(changed_columns)
            total_comparisons += comparisons
        
        # Convert sets back to lists for JSON serialization
        for fact_table in targeted_updates:
            targeted_updates[fact_table] = list(targeted_updates[fact_table])
        
        return {
            "table_name": table_name,
            "records_processed": len(changed_records),
            "records_with_changes": len(records_with_changes),
            "total_column_changes": total_changes,
            "total_comparisons": total_comparisons,
            "targeted_updates": targeted_updates,
            "changed_records": records_with_changes,
            "all_records": changed_records  # For public sync
        }