#!/usr/bin/env python3
"""
Legacy Wrapper for Modular CDC System
Maintains backward compatibility while using new modular architecture

This file provides the same interface as the original complete_cdc_processor.py
but uses the new modular system under the hood.
"""

import os
import sys
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from cdc_modules import CDCOrchestrator
from expanded_dictionary import fact_table_mapping


class CompleteCDCProcessor:
    """
    Legacy wrapper class that maintains the same interface as the original
    monolithic processor but delegates to the new modular system
    """
    
    def __init__(self, db_config=None):
        """Initialize with same interface as original"""
        self.orchestrator = CDCOrchestrator(fact_table_mapping, db_config)
        print("🔄 Complete CDC Processor (Modular Architecture)")
        print("🎯 Using new modular system with backward compatibility")
    
    def process_complete_cdc(self):
        """Main CDC process - delegates to orchestrator"""
        return self.orchestrator.process_complete_cdc()
    
    def run_health_check(self):
        """Health check - delegates to orchestrator"""
        return self.orchestrator.run_health_check()
    


def main():
    """Main function - same as original for compatibility"""
    print("🚀 Starting Complete CDC Process with Modular Architecture...")
    
    start_time = datetime.now()
    
    try:
        # Use legacy interface
        processor = CompleteCDCProcessor()
        results = processor.process_complete_cdc()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\\n🎯 Process completed in {duration.total_seconds():.1f} seconds")
        
        # Show summary if available
        if "overall_summary" in results:
            summary = results["overall_summary"]
            print(f"📊 Records analyzed: {summary.get('phase_1', {}).get('records_analyzed', 0)}")
            print(f"🔍 Changes detected: {summary.get('phase_1', {}).get('changes_detected', 0)}")
            print(f"📈 Fact tables updated: {summary.get('phase_2', {}).get('successful_updates', 0)}")
            print(f"🔄 Records synced: {summary.get('phase_3', {}).get('records_synced', 0)}")
        
        return results
        
    except Exception as e:
        print(f"❌ CDC process failed: {e}")
        raise


if __name__ == "__main__":
    main()