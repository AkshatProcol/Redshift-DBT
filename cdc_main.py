#!/usr/bin/env python3
"""
Modular CDC Main Processor
Clean, organized entry point for the surgical precision CDC system

Usage:
    python cdc_main.py                    # Run complete CDC process
    python cdc_main.py --health-check     # Run health check only
    python cdc_main.py --table companies  # Process single table
"""

import os
import sys
import argparse
import json

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from cdc_modules import CDCOrchestrator
from expanded_dictionary import fact_table_mapping


def main():
    """Main entry point for modular CDC processor"""
    parser = argparse.ArgumentParser(description='Modular CDC Processor with Surgical Precision')
    parser.add_argument('--health-check', action='store_true', 
                       help='Run health check only')
    parser.add_argument('--table', type=str, 
                       help='Process specific table only')
    parser.add_argument('--output', type=str, 
                       help='Save results to JSON file')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose output')
    
    args = parser.parse_args()
    
    print("🎯 Modular CDC Processor - Surgical Precision Architecture")
    print("=" * 60)
    
    # Initialize orchestrator
    orchestrator = CDCOrchestrator(fact_table_mapping)
    
    try:
        if args.health_check:
            # Run health check only
            print("🏥 Health Check Mode")
            results = orchestrator.run_health_check()
            
        elif args.table:
            # Process single table
            print(f"🔍 Single Table Mode: {args.table}")
            if args.table not in orchestrator.staging_tables:
                print(f"❌ Invalid table: {args.table}")
                print(f"Available tables: {', '.join(orchestrator.staging_tables)}")
                sys.exit(1)
            
            # Run detection on single table
            analysis_result = orchestrator.change_detector.analyze_table_changes(args.table)
            
            if analysis_result.get("targeted_updates"):
                # Update fact tables if changes detected
                fact_result = orchestrator.fact_updater.update_targeted_fact_tables(
                    analysis_result["targeted_updates"]
                )
                # Sync public schema
                sync_result = orchestrator.public_syncer.sync_public_schema(
                    args.table, analysis_result.get("all_records", [])
                )
                
                results = {
                    "table": args.table,
                    "detection": analysis_result,
                    "fact_updates": fact_result,
                    "public_sync": sync_result
                }
            else:
                results = {
                    "table": args.table,
                    "detection": analysis_result,
                    "message": "No changes detected"
                }
        else:
            # Run complete CDC process
            print("🚀 Complete CDC Process Mode")
            results = orchestrator.process_complete_cdc()
        
        # Save results if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"📄 Results saved to: {args.output}")
        
        # Print summary
        if isinstance(results, dict) and "overall_summary" in results:
            summary = results["overall_summary"]
            print(f"\\n🎯 Final Summary:")
            print(f"   Duration: {summary.get('duration_seconds', 0):.1f}s")
            print(f"   Records: {summary.get('phase_1', {}).get('records_analyzed', 0)}")
            print(f"   Changes: {summary.get('phase_1', {}).get('changes_detected', 0)}")
            print(f"   Efficiency: {summary.get('optimization_stats', {}).get('efficiency_gain', 'N/A')}")
        
    except KeyboardInterrupt:
        print("\\n⚠️  Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error during CDC process: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    
    print("\\n✅ CDC Process completed successfully!")


if __name__ == "__main__":
    main()