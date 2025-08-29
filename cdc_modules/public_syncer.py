"""
Public Syncer Module
Handles synchronization of staging data with public schema for baseline maintenance
"""

from typing import Dict, List, Tuple, Any
from .database_connector import DatabaseConnector


class PublicSyncer:
    def __init__(self, db_connector: DatabaseConnector):
        """Initialize public schema syncer"""
        self.db = db_connector
        
        # Configuration for composite key handling
        self.composite_key_config = {
            'buyer_seller_company_mappings': {
                'keys': ['client_company_id', 'dealing_with_company_id'],
                'condition_template': 'client_company_id = %s AND dealing_with_company_id = %s'
            },
            'user_company_mappings': {
                'keys': ['user_id', 'company_id'],
                'condition_template': 'user_id = %s AND company_id = %s'
            },
            'taggings': {
                'keys': ['tag_id', 'taggable_id', 'taggable_type'],
                'condition_template': 'tag_id = %s AND taggable_id = %s AND taggable_type = %s'
            },
            'users': {
                'keys': ['email'],
                'condition_template': 'email = %s'
            }
        }
        
        # Configuration for tables that require explicit ID generation
        self.id_required_tables = ['users']
        
    def find_existing_record(self, table_name: str, record: Dict, cursor) -> Tuple[bool, str]:
        """
        🚀 SMART RECORD MATCHING: Handle both ID-based and composite key matching
        Uses configuration-driven approach for scalability
        """
        record_id = record.get('id')
        
        if record_id is not None:
            # Standard ID-based lookup
            cursor.execute(f"SELECT COUNT(*) FROM public.{table_name} WHERE id = %s", (record_id,))
            exists = cursor.fetchone()[0] > 0
            return exists, f"id = {record_id}"
        
        # Handle composite key scenarios for records with NULL IDs using configuration
        if table_name in self.composite_key_config:
            config = self.composite_key_config[table_name]
            keys = config['keys']
            condition_template = config['condition_template']
            
            # Get values for all composite keys
            key_values = [record.get(key) for key in keys]
            
            # Check if all required keys are present and not None
            if all(value is not None for value in key_values):
                try:
                    cursor.execute(f"""
                        SELECT COUNT(*) FROM public.{table_name} 
                        WHERE {condition_template}
                    """, key_values)
                    exists = cursor.fetchone()[0] > 0
                    return exists, condition_template
                except Exception as e:
                    print(f"      ⚠️ Composite key lookup failed for {table_name}: {e}")
                    return False, condition_template
        
        # Default: assume it doesn't exist (will INSERT)
        return False, "1=0"
    
    def sync_public_schema(self, table_name: str, changed_records: List[Dict]) -> Dict:
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
            return {
                "success": True,
                "records_processed": 0,
                "sync_success": 0,
                "sync_failures": 0,
                "message": "No records to sync"
            }
        
        print(f"🔄 Phase 3: Syncing public.{table_name} with {len(changed_records)} changed records...")
        
        conn = self.db.get_connection()
        if not conn:
            return {
                "success": False,
                "error": "Database connection failed",
                "records_processed": 0
            }
        
        try:
            cursor = conn.cursor()
            
            # Get table structure to build proper MERGE/UPSERT
            columns = self.db.get_table_structure(table_name)
            if not columns:
                cursor.close()
                conn.close()
                return {
                    "success": False,
                    "error": f"Could not get table structure for {table_name}",
                    "records_processed": 0
                }
            
            column_names = [col['name'] for col in columns]
            
            # Process each changed record
            sync_success = 0
            sync_failures = 0
            sync_details = []
            
            for record in changed_records:
                record_id = record.get('id')
                change_type = record.get('change_type', 'UPDATE')
                
                try:
                    # 🚀 IMPROVED: Smart record matching for NULL ID handling
                    exists, existing_record_condition = self.find_existing_record(table_name, record, cursor)
                    
                    if record_id is not None:
                        print(f"      🔍 Record ID {record_id}: {'updating' if exists else 'inserting'}")
                    else:
                        print(f"      🔍 Composite key record: {'updating' if exists else 'inserting'}")
                    
                    if exists:
                        # UPDATE existing record using smart condition matching
                        update_columns = [col for col in column_names if col != 'id']
                        
                        if record_id is not None:
                            # Standard ID-based update
                            update_sql = f"""
                            UPDATE public.{table_name} 
                            SET {', '.join([f'"{col}" = %s' for col in update_columns])}
                            WHERE id = %s
                            """
                            values = [record.get(col) for col in update_columns] + [record_id]
                        else:
                            # Composite key-based update using configuration
                            if table_name in self.composite_key_config:
                                config = self.composite_key_config[table_name]
                                condition_template = config['condition_template']
                                key_values = [record.get(key) for key in config['keys']]
                                
                                update_sql = f"""
                                UPDATE public.{table_name} 
                                SET {', '.join([f'"{col}" = %s' for col in update_columns])}
                                WHERE {condition_template}
                                """
                                values = [record.get(col) for col in update_columns] + key_values
                            else:
                                # Fallback for other composite key tables
                                update_sql = f"""
                                UPDATE public.{table_name} 
                                SET {', '.join([f'"{col}" = %s' for col in update_columns])}
                                WHERE {existing_record_condition}
                                """
                                values = [record.get(col) for col in update_columns]
                            
                        cursor.execute(update_sql, values)
                        operation = "updated"
                    else:
                        # INSERT new record (handle NULL ID properly)
                        if record_id is not None:
                            # Standard INSERT with ID
                            placeholders = ', '.join(['%s'] * len(column_names))
                            columns_str = ', '.join([f'"{col}"' for col in column_names])
                            values = [record.get(col) for col in column_names]
                        else:
                            # INSERT with NULL ID - need to generate new ID for tables that require it
                            
                            # Check if this record has enough data to be meaningful first
                            temp_values = [record.get(col) for col in column_names if col != 'id']
                            non_null_values = [v for v in temp_values if v is not None]
                            if len(non_null_values) < 2:  # Skip records with too few meaningful values
                                print(f"      ⚠️ Skipping record with insufficient data (only {len(non_null_values)} non-null values)")
                                sync_failures += 1
                                sync_details.append({
                                    "record_id": record_id,
                                    "operation": "skipped_insufficient_data",
                                    "success": False
                                })
                                continue
                            
                            # For tables that require ID, generate next available ID
                            if table_name in self.id_required_tables:
                                cursor.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM public.{table_name}")
                                next_id = cursor.fetchone()[0]
                                
                                # Standard INSERT with generated ID
                                placeholders = ', '.join(['%s'] * len(column_names))
                                columns_str = ', '.join([f'"{col}"' for col in column_names])
                                values = [next_id if col == 'id' else record.get(col) for col in column_names]
                            else:
                                # INSERT without ID column (for tables that support it)
                                insert_columns = [col for col in column_names if col != 'id']
                                placeholders = ', '.join(['%s'] * len(insert_columns))
                                columns_str = ', '.join([f'"{col}"' for col in insert_columns])
                                values = [record.get(col) for col in insert_columns]
                        
                        insert_sql = f"""
                        INSERT INTO public.{table_name} ({columns_str})
                        VALUES ({placeholders})
                        """
                        cursor.execute(insert_sql, values)
                        operation = "inserted"
                    
                    sync_success += 1
                    sync_details.append({
                        "record_id": record_id,
                        "operation": operation,
                        "success": True
                    })
                    
                except Exception as e:
                    print(f"      ❌ Failed to sync record {record_id}: {e}")
                    
                    # Rollback failed transaction and start fresh for next record
                    try:
                        conn.rollback()
                    except Exception:
                        pass  # Connection might be closed
                        
                    sync_failures += 1
                    sync_details.append({
                        "record_id": record_id,
                        "operation": "failed", 
                        "error": str(e),
                        "success": False
                    })
                    
                    # Skip to next record without breaking the loop
                    continue
            
            # Commit all changes
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"   ✅ Synced {sync_success} records to public.{table_name}")
            if sync_failures > 0:
                print(f"   ⚠️  {sync_failures} sync failures")
            
            return {
                "success": sync_failures == 0,
                "table_name": table_name,
                "records_processed": len(changed_records),
                "sync_success": sync_success,
                "sync_failures": sync_failures,
                "sync_details": sync_details
            }
            
        except Exception as e:
            print(f"   ❌ Public sync failed for {table_name}: {e}")
            if conn:
                conn.rollback()
                conn.close()
            return {
                "success": False,
                "error": str(e),
                "table_name": table_name,
                "records_processed": len(changed_records)
            }