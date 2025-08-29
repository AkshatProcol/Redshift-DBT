"""
Database Connector Module
Handles all database connections, queries, and data retrieval
"""

import psycopg2
from typing import Dict, List, Optional, Any


class DatabaseConnector:
    def __init__(self, db_config: Dict = None):
        """Initialize database connector with configuration"""
        self.db_config = db_config or {
            'host': 'default-workgroup.885373794985.ap-south-1.redshift-serverless.amazonaws.com',
            'port': 5439,
            'database': 'dev',
            'user': 'admin',
            'password': 'FLWGTnvecu049*%'
        }
        
    def get_connection(self):
        """Get database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return None
    
    def get_table_structure(self, table_name: str) -> List[Dict]:
        """Get column structure for a table"""
        conn = self.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            
            query = """
            SELECT 
                column_name as name,
                data_type as type,
                is_nullable
            FROM information_schema.columns 
            WHERE table_schema = 'staging' 
            AND table_name = %s
            ORDER BY ordinal_position
            """
            
            cursor.execute(query, (table_name,))
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    'name': row[0],
                    'type': row[1], 
                    'nullable': row[2] == 'YES'
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
        conn = self.get_connection()
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
            print(f"❌ Error getting changed records from {table_name}: {e}")
            if conn:
                conn.close()
            return []
    
    def get_public_record(self, table_name: str, record_id: Any) -> Optional[Dict]:
        """Get a specific record from public schema"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM public.{table_name} WHERE id = %s", (record_id,))
            
            if cursor.rowcount == 0:
                cursor.close()
                conn.close()
                return None
            
            column_names = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            
            cursor.close()
            conn.close()
            return dict(zip(column_names, row))
            
        except Exception as e:
            print(f"❌ Error getting public record from {table_name}: {e}")
            if conn:
                conn.close()
            return None