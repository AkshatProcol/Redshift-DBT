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
        
    def find_existing_record(self, table_name: str, record: Dict, cursor) -> Tuple[bool, str]:
        """
        🚀 SMART RECORD MATCHING: Handle both ID-based and composite key matching
        
        For tables with NULL IDs, use business-logical unique combinations:
        - buyer_seller_company_mappings: client_company_id + dealing_with_company_id
        - user_company_mappings: user_id + company_id  
        - taggings: tag_id + taggable_id + taggable_type
        """
        record_id = record.get('id')
        
        if record_id is not None:
            # Standard ID-based lookup
            cursor.execute(f"SELECT COUNT(*) FROM public.{table_name} WHERE id = %s", (record_id,))
            exists = cursor.fetchone()[0] > 0
            return exists, f"id = {record_id}"
        
        # Handle composite key scenarios for records with NULL IDs
        if table_name == 'buyer_seller_company_mappings':
            client_id = record.get('client_company_id')
            vendor_id = record.get('dealing_with_company_id')
            if client_id and vendor_id:
                cursor.execute(f"""
                    SELECT COUNT(*) FROM public.{table_name} 
                    WHERE client_company_id = %s AND dealing_with_company_id = %s
                """, (client_id, vendor_id))
                exists = cursor.fetchone()[0] > 0
                return exists, f"client_company_id = {client_id} AND dealing_with_company_id = {vendor_id}"
        
        elif table_name == 'user_company_mappings':
            user_id = record.get('user_id')
            company_id = record.get('company_id')
            if user_id and company_id:
                cursor.execute(f"""
                    SELECT COUNT(*) FROM public.{table_name} 
                    WHERE user_id = %s AND company_id = %s
                """, (user_id, company_id))
                exists = cursor.fetchone()[0] > 0
                return exists, f"user_id = {user_id} AND company_id = {company_id}"
        
        elif table_name == 'taggings':
            tag_id = record.get('tag_id')
            taggable_id = record.get('taggable_id')
            taggable_type = record.get('taggable_type')
            if tag_id and taggable_id and taggable_type:
                cursor.execute(f"""
                    SELECT COUNT(*) FROM public.{table_name} 
                    WHERE tag_id = %s AND taggable_id = %s AND taggable_type = %s
                """, (tag_id, taggable_id, taggable_type))
                exists = cursor.fetchone()[0] > 0
                return exists, f"tag_id = {tag_id} AND taggable_id = {taggable_id} AND taggable_type = '{taggable_type}'"
        
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
                        operation = "updated"
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
                        operation = "inserted"
                    
                    sync_success += 1
                    sync_details.append({
                        "record_id": record_id,
                        "operation": operation,
                        "success": True
                    })
                    
                except Exception as e:
                    print(f"      ❌ Failed to sync record {record_id}: {e}")
                    sync_failures += 1
                    sync_details.append({
                        "record_id": record_id,
                        "operation": "failed",
                        "error": str(e),
                        "success": False
                    })
            
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
    
    def verify_public_sync(self, table_name: str, record_ids: List[Any]) -> Dict:
        """Verify that records were properly synced to public schema"""
        if not record_ids:
            return {"verified": True, "missing_records": []}
        
        # Filter out None record_ids
        valid_ids = [rid for rid in record_ids if rid is not None]
        if not valid_ids:
            return {"verified": True, "missing_records": [], "note": "No valid IDs to verify"}
        
        missing_records = []
        
        for record_id in valid_ids:
            public_record = self.db.get_public_record(table_name, record_id)
            if not public_record:
                missing_records.append(record_id)
        
        return {
            "verified": len(missing_records) == 0,
            "checked_records": len(valid_ids),
            "missing_records": missing_records
        }