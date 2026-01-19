"""
Storage Management Script for FMCG Data Analytics Platform
Handles BigQuery quota issues and provides optimization tools
"""

import sys
import os
from pathlib import Path
import argparse
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

try:
    from src.utils.storage_manager import BigQueryStorageManager, StorageOptimizedSynchronizer, run_storage_optimization
    from src.utils.bigquery_client import BigQueryManager
except ImportError:
    print("⚠️  Some dependencies not available. Using fallback mode.")
    
    class MockBigQueryManager:
        def __init__(self, project_id=None, dataset=None):
            self.project_id = project_id or 'fmcg-data-generator'
            self.dataset = dataset or 'fmcg_warehouse'
    
    BigQueryManager = MockBigQueryManager


def print_storage_report(report):
    """Print formatted storage report"""
    
    storage = report['storage_analysis']
    
    print("\n" + "="*80)
    print("🗄️  BIGQUERY STORAGE ANALYSIS")
    print("="*80)
    
    # Status indicator
    status_emoji = {
        'OK': '✅',
        'WARNING': '⚠️',
        'CRITICAL': '🚨',
        'ERROR': '❌'
    }
    
    print(f"\n{status_emoji.get(storage['status'], '❓')} Storage Status: {storage['status']}")
    print(f"   Current Usage: {storage['total_storage_gb']:.2f} GB")
    print(f"   Free Limit: {storage['free_storage_limit_gb']:.0f} GB")
    print(f"   Usage Percentage: {storage['usage_percentage']:.1f}%")
    print(f"   Table Count: {storage['table_count']}")
    
    if storage['table_details']:
        print(f"\n📋 Top 10 Tables by Storage Usage:")
        print("-" * 80)
        print(f"{'Table Name':<25} {'Storage (GB)':<12} {'Rows':<12} {'Created':<12}")
        print("-" * 80)
        
        for table in storage['table_details'][:10]:
            created_str = table['created'].strftime('%Y-%m-%d') if table['created'] else 'N/A'
            print(f"{table['table_id']:<25} {table['storage_gb']:<12.2f} {table['row_count']:<12,} {created_str:<12}")
    
    if report['archiving_opportunities']:
        print(f"\n🗄️  Data Archiving Opportunities:")
        print("-" * 60)
        print(f"{'Table':<20} {'Old Records':<12} {'Est. Storage (MB)':<15} {'Date Range':<20}")
        print("-" * 60)
        
        for table, info in report['archiving_opportunities'].items():
            date_range = f"{info['oldest_date']} to {info['newest_date']}"
            print(f"{table:<20} {info['old_record_count']:<12,} {info['estimated_storage_mb']:<15.1f} {date_range:<20}")
    
    if report['recommendations']:
        print(f"\n💡 Recommendations:")
        for i, rec in enumerate(report['recommendations'], 1):
            print(f"   {i}. {rec}")
    
    print(f"\n💰 Estimated Storage Savings: {report['estimated_savings_gb']:.2f} GB")


def archive_data_interactive(storage_manager, archiving_candidates):
    """Interactive data archiving"""
    
    if not archiving_candidates:
        print("\n✅ No archiving candidates found!")
        return
    
    print(f"\n🗄️  Found {len(archiving_candidates)} tables with archivable data:")
    
    for table, info in archiving_candidates.items():
        print(f"\n📋 {table}:")
        print(f"   Records older than 1 year: {info['old_record_count']:,}")
        print(f"   Estimated storage savings: {info['estimated_storage_mb']:.1f} MB")
        print(f"   Date range: {info['oldest_date']} to {info['newest_date']}")
    
    while True:
        response = input(f"\n❓ Archive old data from these tables? (y/n): ").lower().strip()
        
        if response == 'y':
            print("\n🔄 Starting archiving process...")
            
            success_count = 0
            for table, info in archiving_candidates.items():
                print(f"\n📦 Archiving {table}...")
                cutoff_date = info['cutoff_date']
                
                if storage_manager.archive_old_data(table, str(cutoff_date)):
                    print(f"   ✅ Successfully archived {table}")
                    success_count += 1
                else:
                    print(f"   ❌ Failed to archive {table}")
            
            print(f"\n🎉 Archiving completed: {success_count}/{len(archiving_candidates)} tables processed")
            
            # Show updated storage usage
            updated_usage = storage_manager.get_storage_usage()
            print(f"\n📊 Updated Storage Usage: {updated_usage['total_storage_gb']:.2f} GB")
            print(f"   Status: {updated_usage['status']}")
            
            break
        
        elif response == 'n':
            print("⏭️  Archiving skipped.")
            break
        
        else:
            print("Please enter 'y' or 'n'")


def optimize_storage_interactive(storage_manager):
    """Interactive storage optimization"""
    
    print(f"\n🔧 Storage Optimization Options:")
    print(f"   1. Create aggregated views for historical data")
    print(f"   2. Optimize table partitioning")
    print(f"   3. Both optimizations")
    
    while True:
        choice = input(f"\n❓ Select optimization (1-3): ").strip()
        
        if choice == '1':
            print("\n🔄 Creating aggregated views...")
            if storage_manager.create_aggregated_views():
                print("✅ Aggregated views created successfully!")
            else:
                print("❌ Failed to create aggregated views")
            break
        
        elif choice == '2':
            print("\n🔄 Optimizing table partitioning...")
            # Get main fact tables
            storage_usage = storage_manager.get_storage_usage()
            fact_tables = [t for t in storage_usage['table_details'] if t['table_id'].startswith('fact_')]
            
            success_count = 0
            for table in fact_tables[:3]:  # Limit to top 3 tables
                print(f"📦 Optimizing {table['table_id']}...")
                if storage_manager.optimize_table_storage(table['table_id']):
                    success_count += 1
            
            print(f"✅ Optimized {success_count} tables")
            break
        
        elif choice == '3':
            print("\n🔄 Running full optimization...")
            views_success = storage_manager.create_aggregated_views()
            
            storage_usage = storage_manager.get_storage_usage()
            fact_tables = [t for t in storage_usage['table_details'] if t['table_id'].startswith('fact_')]
            
            opt_success_count = 0
            for table in fact_tables[:2]:  # Limit to top 2 tables
                if storage_manager.optimize_table_storage(table['table_id']):
                    opt_success_count += 1
            
            print(f"✅ Aggregated views: {'Created' if views_success else 'Failed'}")
            print(f"✅ Table optimization: {opt_success_count} tables optimized")
            break
        
        else:
            print("Please enter 1, 2, or 3")


def run_storage_aware_sync():
    """Run synchronization with storage constraints"""
    
    try:
        # Initialize clients
        bq_client = BigQueryManager()
        storage_manager = BigQueryStorageManager(bq_client.project_id, bq_client.dataset)
        
        # Create storage-aware synchronizer
        sync = StorageOptimizedSynchronizer(bq_client, storage_manager)
        
        print("\n🔄 Running Storage-Aware Synchronization...")
        
        # Run sync with storage constraints
        results = sync.run_storage_aware_sync(days_back=30)
        
        if results['status'] == 'SUCCESS':
            print("✅ Synchronization completed successfully!")
            
            report = results['report']
            summary = report['summary']
            
            print(f"\n📊 Sync Results:")
            print(f"   Records Analyzed: {summary['total_records_analyzed']:,}")
            print(f"   Critical Issues: {summary['critical_variance_records']}")
            print(f"   Average Variance: {summary['average_variance_percentage']:.2f}%")
            print(f"   Date Range: {results['date_range']}")
            
            storage = results['storage_usage']
            print(f"   Storage Status: {storage['status']} ({storage['total_storage_gb']:.2f} GB)")
            
        else:
            print(f"❌ Synchronization failed: {results['reason']}")
    
    except Exception as e:
        print(f"❌ Storage-aware sync failed: {str(e)}")


def main():
    """Main storage management script"""
    
    parser = argparse.ArgumentParser(description='FMCG BigQuery Storage Management')
    parser.add_argument('--project-id', type=str, help='GCP Project ID')
    parser.add_argument('--dataset', type=str, help='BigQuery Dataset Name')
    parser.add_argument('--analyze', action='store_true', help='Analyze storage usage')
    parser.add_argument('--archive', action='store_true', help='Archive old data interactively')
    parser.add_argument('--optimize', action='store_true', help='Optimize storage interactively')
    parser.add_argument('--sync', action='store_true', help='Run storage-aware synchronization')
    parser.add_argument('--days-to-keep', type=int, default=365, help='Days to keep when archiving')
    
    args = parser.parse_args()
    
    # Default project and dataset
    project_id = args.project_id or 'fmcg-data-generator'
    dataset = args.dataset or 'fmcg_warehouse'
    
    print(f"🗄️  FMCG BigQuery Storage Management")
    print(f"   Project: {project_id}")
    print(f"   Dataset: {dataset}")
    
    try:
        # Initialize storage manager
        storage_manager = BigQueryStorageManager(project_id, dataset)
        
        # Generate storage report
        report = storage_manager.generate_storage_report()
        
        # Always show storage analysis
        print_storage_report(report)
        
        # Handle different actions
        if args.archive:
            archive_data_interactive(storage_manager, report['archiving_opportunities'])
        
        elif args.optimize:
            optimize_storage_interactive(storage_manager)
        
        elif args.sync:
            run_storage_aware_sync()
        
        elif not args.analyze:
            # Default: show recommendations
            if report['storage_analysis']['status'] in ['CRITICAL', 'WARNING']:
                print(f"\n🚨 Storage Issues Detected!")
                print(f"   Run with --archive to clean up old data")
                print(f"   Run with --optimize to improve storage efficiency")
                print(f"   Run with --sync for storage-aware synchronization")
            else:
                print(f"\n✅ Storage usage is healthy!")
        
        print(f"\n🎉 Storage management completed!")
        return 0
        
    except Exception as e:
        print(f"❌ Storage management failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
