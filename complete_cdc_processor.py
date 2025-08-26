#!/usr/bin/env python3
"""
Complete CDC Processor - With Public Schema Synchronization
Includes surgical precision PLUS public schema sync to maintain baseline
"""

import os
import sys
import subprocess
import json
import psycopg2
from datetime import datetime
from typing import Dict, List, Set, Optional, Tuple, Any

# Import the dictionary
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from expanded_dictionary import fact_table_mapping

class CompleteCDCProcessor:
    def __init__(self, db_config: Dict = None):
        """Initialize complete CDC processor with public schema sync"""
        self.mapping = fact_table_mapping
        self.staging_tables = [
            'companies', 'buyer_seller_company_mappings', 'users', 'teams',
            'team_members', 'cities', 'countries', 'product_categories',
            'user_company_mappings', 'taggings', 'tags', 'preferred_vendor_item_mappings'
        ]
        
        # Database connection config
        self.db_config = db_config or {
            'host': 'default-workgroup.885373794985.ap-south-1.redshift-serverless.amazonaws.com',
            'port': 5439,
            'database': 'dev',
            'user': 'admin',
            'password': 'FLWGTnvecu049*%'
        }
        
        print("🔄 Complete CDC Processor - Surgical Precision + Public Schema Sync")
        print("🎯 Will detect changes, update fact tables, AND sync public schema")
    
    def get_db_connection(self):
        """Get database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return None
    
    def get_table_structure(self, table_name: str) -> List[Dict]:
        """Get complete table structure"""
        conn = self.get_db_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT 
                    column_name,
                    data_type,
                    ordinal_position,
                    is_nullable
                FROM information_schema.columns 
                WHERE table_schema = 'staging' 
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'position': row[2],
                    'nullable': row[3] == 'YES'
                })
            
            cursor.close()
            conn.close()
            return columns
            
        except Exception as e:
            print(f"❌ Error getting table structure for {table_name}: {e}")
            if conn:
                conn.close()
            return []
    
    def get_changed_records(self, table_name: str) -> List[Dict]:
        """Get all records from staging table with change type detection"""
        conn = self.get_db_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            
            query = f"""
            SELECT 
                staging.*,
                CASE 
                    WHEN staging.created_at = staging.updated_at THEN 'INSERT'
                    ELSE 'UPDATE'
                END as change_type,
                CASE 
                    WHEN public.id IS NULL THEN true
                    ELSE false
                END as is_new_record
            FROM staging.{table_name} staging
            LEFT JOIN public.{table_name} public ON staging.id = public.id
            ORDER BY staging.updated_at DESC
            """
            
            cursor.execute(query)
            column_names = [desc[0] for desc in cursor.description]
            
            records = []
            for row in cursor.fetchall():
                record = dict(zip(column_names, row))
                records.append(record)
            
            cursor.close()
            conn.close()
            return records
            
        except Exception as e:
            print(f"❌ Error getting changed records for {table_name}: {e}")
            if conn:
                conn.close()
            return []
    
    def get_dictionary_columns(self, table_name: str) -> Set[str]:
        """Get only columns that are mapped in dictionary - OPTIMIZATION"""
        if table_name not in self.mapping:
            return set()
        
        dictionary_columns = set(self.mapping[table_name].keys())
        print(f"      📚 Dictionary columns for {table_name}: {len(dictionary_columns)} (vs all columns)")
        return dictionary_columns
    
    def compare_record_columns(self, table_name: str, staging_record: Dict, columns: List[Dict]) -> Tuple[List[str], int]:
        """Compare each column of staging record with public schema - HYBRID APPROACH"""
        # 🚀 OPTIMIZATION: Get dictionary-mapped columns first
        dictionary_columns = self.get_dictionary_columns(table_name)
        
        if staging_record.get('is_new_record', False):
            # New record - only return dictionary-mapped columns as "fact table changes"
            # But public sync will handle ALL columns
            changed_cols = [col['name'] for col in columns 
                           if col['name'] in dictionary_columns and 
                           col['name'] not in ['created_at', 'updated_at', 'change_type', 'is_new_record']]
            return changed_cols, len(changed_cols)
        
        # Get corresponding record from public schema
        public_record = self.get_public_record(table_name, staging_record['id'])
        if not public_record:
            # No public record - only return dictionary-mapped columns as "fact table changes"
            changed_cols = [col['name'] for col in columns 
                           if col['name'] in dictionary_columns and 
                           col['name'] not in ['created_at', 'updated_at', 'change_type', 'is_new_record']]
            return changed_cols, len(changed_cols)
        
        # 🔄 HYBRID APPROACH: 
        # 1. Only compare dictionary columns for FACT TABLE updates (optimization)
        # 2. But public sync will handle ALL columns (data consistency)
        
        fact_table_changes = []  # Only dictionary-mapped columns
        columns_compared = 0
        
        # 🚀 OPTIMIZATION: Only compare dictionary-mapped columns for fact table targeting
        for col in columns:
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
        
        print(f"      ⚡ Optimized: compared {columns_compared} dictionary columns for fact updates (vs {len(columns)} total)")
        print(f"      📊 Note: Public sync will handle ALL columns for data consistency")
        return fact_table_changes, columns_compared
    
    def get_public_record(self, table_name: str, record_id: Any) -> Optional[Dict]:
        """Get a specific record from public schema"""
        conn = self.get_db_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM public.{table_name} WHERE id = %s", (record_id,))
            
            column_names = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            
            if row:
                record = dict(zip(column_names, row))
                cursor.close()
                conn.close()
                return record
            
            cursor.close()
            conn.close()
            return None
            
        except Exception as e:
            print(f"❌ Error getting public record {record_id} from {table_name}: {e}")
            if conn:
                conn.close()
            return None
    
    def values_different(self, staging_val: Any, public_val: Any, data_type: str) -> bool:
        """Compare two values considering data type and null handling"""
        if staging_val is None and public_val is None:
            return False
        if staging_val is None or public_val is None:
            return True
        
        staging_str = str(staging_val).strip()
        public_str = str(public_val).strip()
        
        if data_type in ['boolean']:
            staging_bool = str(staging_val).lower() in ['true', '1', 't', 'yes']
            public_bool = str(public_val).lower() in ['true', '1', 't', 'yes']
            return staging_bool != public_bool
        elif data_type in ['numeric', 'integer', 'bigint', 'smallint']:
            try:
                return float(staging_str) != float(public_str)
            except (ValueError, TypeError):
                return staging_str != public_str
        else:
            return staging_str != public_str
    
    def get_targeted_fact_updates(self, table_name: str, changed_columns: List[str]) -> Dict[str, List[str]]:
        """Use expanded_dictionary to get targeted fact table updates"""
        if table_name not in self.mapping:
            return {}
        
        table_mapping = self.mapping[table_name]
        targeted_updates = {}
        
        for column in changed_columns:
            if column in table_mapping:
                fact_mappings = table_mapping[column]
                
                for fact_table, fact_columns in fact_mappings.items():
                    if fact_table not in targeted_updates:
                        targeted_updates[fact_table] = []
                    
                    if isinstance(fact_columns, str):
                        targeted_updates[fact_table].extend(fact_columns.split(','))
                    else:
                        targeted_updates[fact_table].append(fact_columns)
        
        # Remove duplicates
        for fact_table in targeted_updates:
            targeted_updates[fact_table] = [col.strip() for col in set(targeted_updates[fact_table])]
        
        return targeted_updates
    
    def execute_fact_table_update(self, fact_table: str) -> bool:
        """Execute fact table update using dbt"""
        try:
            cmd = f"dbt run --select {fact_table} --target dev"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"✅ Successfully updated {fact_table}")
                return True
            else:
                print(f"❌ Failed to update {fact_table}")
                return False
                
        except Exception as e:
            print(f"❌ Exception updating {fact_table}: {e}")
            return False
    
    def find_existing_record(self, table_name: str, record: Dict, cursor) -> Tuple[bool, str]:
        """
        🚀 SMART RECORD MATCHING - Handles NULL IDs with composite keys
        
        For records with NULL IDs, uses alternative unique combinations:
        - buyer_seller_company_mappings: client_company_id + dealing_with_company_id  
        - companies: email (should be unique)
        - users: email (should be unique)
        - others: use available unique combinations
        """
        record_id = record.get('id')
        
        if record_id is not None:
            # Standard ID-based matching
            cursor.execute(f"SELECT COUNT(*) FROM public.{table_name} WHERE id = %s", (record_id,))
            exists = cursor.fetchone()[0] > 0
            condition = f"id = {record_id}"
            return exists, condition
        
        # NULL ID - use table-specific composite keys
        if table_name == 'buyer_seller_company_mappings':
            # Match by client + vendor combination
            client_id = record.get('client_company_id')
            vendor_id = record.get('dealing_with_company_id')
            if client_id and vendor_id:
                cursor.execute(f"""
                    SELECT COUNT(*) FROM public.{table_name} 
                    WHERE client_company_id = %s AND dealing_with_company_id = %s
                """, (client_id, vendor_id))
                exists = cursor.fetchone()[0] > 0
                condition = f"client_company_id = {client_id} AND dealing_with_company_id = {vendor_id}"
                return exists, condition
                
        elif table_name == 'companies':
            # Match by email (should be unique)
            email = record.get('email')
            if email:
                cursor.execute(f"SELECT COUNT(*) FROM public.{table_name} WHERE email = %s", (email,))
                exists = cursor.fetchone()[0] > 0
                condition = f"email = '{email}'"
                return exists, condition
                
        elif table_name == 'users':
            # Match by email (should be unique)  
            email = record.get('email')
            if email:
                cursor.execute(f"SELECT COUNT(*) FROM public.{table_name} WHERE email = %s", (email,))
                exists = cursor.fetchone()[0] > 0
                condition = f"email = '{email}'"
                return exists, condition
        
        # Fallback: assume new record if no matching strategy
        return False, "1=0"  # Always false condition
    
    def sync_public_schema(self, table_name: str, changed_records: List[Dict]) -> bool:
        """
        🔄 CRITICAL: Sync public schema with staging data after processing
        This ensures public schema stays current for future comparisons
        
        ⭐ HYBRID APPROACH:
        - Fact table updates: Only dictionary-mapped columns (optimization)  
        - Public sync: ALL columns (data consistency)
        
        This guarantees that ALL column changes reach public schema,
        even if they don't trigger fact table updates.
        """
        if not changed_records:
            return True
        
        print(f"🔄 Syncing public.{table_name} with {len(changed_records)} changed records...")
        
        conn = self.get_db_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # Get table structure to build proper MERGE/UPSERT
            columns = self.get_table_structure(table_name)
            if not columns:
                cursor.close()
                conn.close()
                return False
            
            column_names = [col['name'] for col in columns]
            
            # Process each changed record
            sync_success = 0
            sync_failures = 0
            
            for record in changed_records:
                record_id = record.get('id')
                change_type = record.get('change_type', 'UPDATE')
                
                try:
                    # Use simple INSERT/UPDATE approach for Redshift compatibility
                    record_id = record.get('id')
                    
                    # 🚀 IMPROVED: Smart record matching for NULL ID handling
                    exists, existing_record_condition = self.find_existing_record(table_name, record, cursor)
                    
                    if record_id is not None:
                        print(f"      🔍 Matching by ID: {record_id} ({'found' if exists else 'not found'})")
                    else:
                        print(f"      🔍 Matching by composite key ({'found' if exists else 'not found'})")
                    
                    if exists:
                        # UPDATE existing record using smart condition matching
                        update_columns = [col for col in column_names if col != 'id']
                        
                        if record_id is not None:
                            # Standard ID-based update
                            update_sql = f"""
                            UPDATE public.{table_name} 
                            SET {', '.join([f'{col} = %s' for col in update_columns])}
                            WHERE id = %s
                            """
                            values = [record.get(col) for col in update_columns] + [record_id]
                        else:
                            # Composite key-based update
                            update_sql = f"""
                            UPDATE public.{table_name} 
                            SET {', '.join([f'{col} = %s' for col in update_columns])}
                            WHERE {existing_record_condition}
                            """
                            values = [record.get(col) for col in update_columns]
                            
                        cursor.execute(update_sql, values)
                    else:
                        # INSERT new record
                        placeholders = ', '.join(['%s'] * len(column_names))
                        columns_str = ', '.join(column_names)
                        insert_sql = f"""
                        INSERT INTO public.{table_name} ({columns_str})
                        VALUES ({placeholders})
                        """
                        values = [record.get(col) for col in column_names]
                        cursor.execute(insert_sql, values)
                    
                    sync_success += 1
                    
                except Exception as e:
                    print(f"   ❌ Failed to sync record {record_id}: {e}")
                    sync_failures += 1
            
            # Commit all changes
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"   ✅ Synced {sync_success} records to public.{table_name}")
            if sync_failures > 0:
                print(f"   ⚠️  {sync_failures} sync failures")
            
            return sync_failures == 0
            
        except Exception as e:
            print(f"❌ Error syncing public schema for {table_name}: {e}")
            if conn:
                try:
                    conn.rollback()
                    conn.close()
                except:
                    pass
            return False
    
    def process_complete_cdc(self) -> Dict:
        """
        Complete CDC processing with public schema synchronization
        """
        start_time = datetime.now()
        print(f"🚀 Starting Complete CDC Processing at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("🔬 Phase 1: Surgical change detection")
        print("🔄 Phase 2: Fact table updates") 
        print("📊 Phase 3: Public schema synchronization")
        
        total_records_analyzed = 0
        total_columns_compared = 0
        total_dictionary_columns_compared = 0  # 🚀 Track optimization impact
        total_changes_detected = 0
        all_targeted_updates = {}
        tables_to_sync = {}
        
        # Phase 1: Surgical Change Detection
        print("\n🔍 Phase 1: Surgical analysis of all staging tables...")
        
        for table in self.staging_tables:
            print(f"\n📋 Analyzing {table}...")
            
            columns = self.get_table_structure(table)
            if not columns:
                print(f"   ⚠️  Could not get table structure - skipping")
                continue
            
            records = self.get_changed_records(table)
            if not records:
                print(f"   ✅ No records in staging")
                continue
            
            print(f"   📊 Found {len(records)} records to analyze")
            tables_to_sync[table] = records  # Store for sync phase
            
            for record in records:
                record_id = record.get('id', 'Unknown')
                change_type = record.get('change_type', 'Unknown')
                
                changed_columns, dict_columns_compared = self.compare_record_columns(table, record, columns)
                
                if changed_columns:
                    total_changes_detected += len(changed_columns)
                    mapped_columns = [col for col in changed_columns if table in self.mapping and col in self.mapping[table]]
                    
                    print(f"      🔬 Record {record_id} ({change_type}): {len(mapped_columns)} mapped columns changed")
                    
                    # Get targeted fact table updates
                    targeted = self.get_targeted_fact_updates(table, changed_columns)
                    
                    for fact_table, fact_columns in targeted.items():
                        if fact_table not in all_targeted_updates:
                            all_targeted_updates[fact_table] = set()
                        all_targeted_updates[fact_table].update(fact_columns)
                
                total_records_analyzed += 1
                total_columns_compared += len(columns)  # Total possible columns
                total_dictionary_columns_compared += dict_columns_compared  # Actual columns compared
        
        # Phase 2: Fact Table Updates  
        if all_targeted_updates:
            print(f"\n🎯 Phase 2: Updating {len(all_targeted_updates)} affected fact tables...")
            
            successful_updates = []
            failed_updates = []
            
            for fact_table in sorted(all_targeted_updates.keys()):
                columns = list(all_targeted_updates[fact_table])
                print(f"   🔄 {fact_table} (affects: {', '.join(columns[:3])}{'...' if len(columns) > 3 else ''})")
                
                if self.execute_fact_table_update(fact_table):
                    successful_updates.append(fact_table)
                else:
                    failed_updates.append(fact_table)
        else:
            print(f"\n✅ Phase 2: No fact table updates needed")
            successful_updates = []
            failed_updates = []
        
        # Phase 3: Public Schema Synchronization
        print(f"\n📊 Phase 3: Synchronizing public schema...")
        
        sync_successful = []
        sync_failed = []
        
        for table_name, records in tables_to_sync.items():
            print(f"   🔄 Syncing public.{table_name}...")
            
            if self.sync_public_schema(table_name, records):
                sync_successful.append(table_name)
            else:
                sync_failed.append(table_name)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n🎉 Complete CDC Processing Finished!")
        print(f"⏱️  Total Duration: {duration.total_seconds():.2f} seconds")
        print(f"🔬 Records Analyzed: {total_records_analyzed}")
        print(f"📊 Total Possible Columns: {total_columns_compared}")
        print(f"⚡ Dictionary Columns Compared: {total_dictionary_columns_compared}")
        efficiency = ((total_columns_compared - total_dictionary_columns_compared) / max(total_columns_compared, 1)) * 100
        print(f"🚀 Optimization Efficiency: {efficiency:.1f}% columns skipped")
        print(f"🎯 Changes Detected: {total_changes_detected}")
        print(f"✅ Fact Tables Updated: {len(successful_updates)}")
        print(f"📊 Public Tables Synced: {len(sync_successful)}")
        
        if failed_updates:
            print(f"❌ Fact Table Failures: {failed_updates}")
        if sync_failed:
            print(f"❌ Sync Failures: {sync_failed}")
        
        return {
            'status': 'success' if not failed_updates and not sync_failed else 'partial_success',
            'duration': duration.total_seconds(),
            'phase_1_analysis': {
                'records_analyzed': total_records_analyzed,
                'total_possible_columns': total_columns_compared,
                'dictionary_columns_compared': total_dictionary_columns_compared,
                'optimization_efficiency_percent': ((total_columns_compared - total_dictionary_columns_compared) / max(total_columns_compared, 1)) * 100,
                'changes_detected': total_changes_detected
            },
            'phase_2_fact_updates': {
                'successful': successful_updates,
                'failed': failed_updates
            },
            'phase_3_public_sync': {
                'successful': sync_successful,
                'failed': sync_failed
            },
            'targeted_updates': {k: list(v) for k, v in all_targeted_updates.items()}
        }

def main():
    """Main execution"""
    processor = CompleteCDCProcessor()
    result = processor.process_complete_cdc()
    
    print("\n" + "="*70)
    print("COMPLETE CDC PROCESSING SUMMARY")
    print("="*70)
    print(json.dumps(result, indent=2, default=str))

if __name__ == "__main__":
    main()