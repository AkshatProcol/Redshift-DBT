"""
Table Discovery Module
Handles discovery and validation of staging tables for CDC processing
"""

from typing import List, Dict
from .database_connector import DatabaseConnector


class TableDiscovery:
    def __init__(self, db_connector: DatabaseConnector, dictionary_mapping: Dict):
        """Initialize table discovery with database connection and dictionary mapping"""
        self.db = db_connector
        self.mapping = dictionary_mapping
        
    def discover_staging_tables(self) -> List[str]:
        """Smart staging table discovery with multiple fallback strategies"""
        print("\n🔍 DISCOVERING STAGING TABLES...")
        print("=" * 40)
        
        # Strategy 1: Database discovery (most accurate)
        tables = self.get_tables_from_database()
        if tables:
            return tables
            
        # Fallback to known list
        print("⚠️ Falling back to hardcoded table list")
        return self.get_fallback_tables()
    
    def get_tables_from_database(self) -> List[str]:
        """Get staging tables directly from database schema"""
        try:
            # Use the database connector's get_connection method
            conn = self.db.get_connection()
            if not conn:
                print("❌ Database discovery: Could not establish connection")
                return []
                
            cursor = conn.cursor()
            
            # Redshift-compatible query to get staging schema tables
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'staging' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
            
            cursor.execute(query)
            tables = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            if tables:
                print(f"✅ Database Discovery: Found {len(tables)} staging tables")
                print(f"📋 Tables: {sorted(tables)}")
                return tables
            else:
                print("⚠️ Database discovery: No staging tables found")
                
        except Exception as e:
            print(f"❌ Database discovery failed: {e}")
            
        return []
    
    def get_fallback_tables(self) -> List[str]:
        """Fallback hardcoded list (legacy approach)"""
        tables = [
            'companies', 'buyer_seller_company_mappings', 'users', 'teams',
            'team_members', 'cities', 'countries', 'product_categories',
            'user_company_mappings', 'taggings', 'tags', 'preferred_vendor_item_mappings',
            'bids', 'bid_trades', 'buyer_hubs', 'event_groups', 'bid_trade_products',
            'audiences', 'orders', 'product_qualities', 'products', 'trade_products',
            'trade_requests', 'units'
        ]
        
        print(f"⚠️ Fallback List: Using {len(tables)} hardcoded tables")
        return tables