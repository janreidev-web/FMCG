"""
Large-Scale Data Management Script for FMCG Platform
Optimized for 471K sales records and 2M inventory records
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
    from src.utils.large_scale_manager import LargeScaleDataManager, run_large_scale_optimization
    from src.utils.storage_manager import BigQueryStorageManager
    from src.utils.bigquery_client import BigQueryManager
except ImportError:
    print("⚠️  Some dependencies not available. Using fallback mode.")


def print_large_scale_analysis(results):
    """Print formatted large-scale analysis"""
    
    tables_info = results['tables_info']
    strategy = results['archiving_strategy']
    recommendations = results['optimization_recommendations']
    
    print("\n" + "="*80)
    print("📊 LARGE-SCALE DATA ANALYSIS RESULTS")
    print("="*80)
    
    print(f"\n📋 Current Dataset Scale:")
    for table, info in tables_info.items():
        print(f"\n🔹 {table.upper()}:")
        print(f"   Total Rows: {info.get('rows', 'N/A'):,}")
        
        if 'earliest_date' in info and info['earliest_date']:
            print(f"   Date Range: {info['earliest_date']} to {info['latest_date']}")
            print(f"   Data Span: {info['date_range_days']} days")
        
        if 'unique_products' in info:
            print(f"   Unique Products: {info['unique_products']:,}")
        
        if table == 'fact_sales' and 'unique_retailers' in info:
            print(f"   Unique Retailers: {info['unique_retailers']:,}")
        elif table == 'fact_inventory' and 'unique_locations' in info:
            print(f"   Unique Locations: {info['unique_locations']:,}")
    
    print(f"\n🎯 ARCHIVING STRATEGY:")
    if strategy['immediate_actions']:
        print(f"   Immediate Actions Required:")
        for i, action in enumerate(strategy['immediate_actions'], 1):
            print(f"   {i}. {action}")
        
        print(f"\n📦 Detailed Archiving Plan:")
        for table, plan in strategy['archiving_plan'].items():
            print(f"   {table}:")
            print(f"     Records to Archive: {plan['old_records']:,}")
            print(f"     Percentage: {plan['percentage']:.1f}%")
            print(f"     Cutoff Date: {plan['cutoff_date']}")
            print(f"     Estimated Savings: {plan['estimated_savings_gb']:.2f} GB")
    else:
        print(f"   ✅ No immediate archiving required")
    
    print(f"\n💰 Storage Impact:")
    savings = strategy['storage_savings']
    print(f"   Potential Savings: {savings['total_gb']:.2f} GB")
    print(f"   Estimated New Storage: {savings['new_estimated_gb']:.1f} GB")
    
    print(f"\n📈 RETENTION POLICY:")
    policy = strategy['retention_policy']
    print(f"   Active Data: {policy['active_data_days']} days (main tables)")
    print(f"   Archive Threshold: {policy['archive_threshold_days']} days")
    print(f"   Permanent Archive: {policy['permanent_archive_days']} days")
    print(f"   Cleanup Frequency: {policy['recommended_cleanup_frequency']}")
    
    print(f"\n💡 OPTIMIZATION RECOMMENDATIONS:")
    for category, tips in recommendations.items():
        print(f"\n   {category.replace('_', ' ').title()}:")
        for i, tip in enumerate(tips[:5], 1):  # Show top 5 tips
            print(f"     {i}. {tip}")


def execute_large_scale_archiving(manager, strategy, interactive=True):
    """Execute large-scale archiving with progress tracking"""
    
    if not strategy['archiving_plan']:
        print("\n✅ No archiving required!")
        return True
    
    print(f"\n🗄️  LARGE-SCALE ARCHIVING EXECUTION")
    print("="*50)
    
    total_records_to_archive = sum(plan['old_records'] for plan in strategy['archiving_plan'].values())
    total_savings = strategy['storage_savings']['total_gb']
    
    print(f"📊 Archiving Summary:")
    print(f"   Total Records to Archive: {total_records_to_archive:,}")
    print(f"   Estimated Storage Savings: {total_savings:.2f} GB")
    print(f"   Tables to Process: {len(strategy['archiving_plan'])}")
    
    if interactive:
        response = input(f"\n❓ Proceed with archiving {total_records_to_archive:,} records? (y/n): ").lower().strip()
        if response != 'y':
            print("⏭️  Archiving cancelled.")
            return False
    
    # Execute archiving
    success = manager.execute_large_scale_archiving(strategy, interactive=False)
    
    if success:
        print(f"\n✅ Large-scale archiving completed successfully!")
        
        # Show post-archiving storage analysis
        storage_manager = BigQueryStorageManager(manager.project_id, manager.dataset)
        updated_storage = storage_manager.get_storage_usage()
        
        print(f"\n📊 Updated Storage Status:")
        print(f"   Current Usage: {updated_storage['total_storage_gb']:.2f} GB")
        print(f"   Status: {updated_storage['status']}")
        
    else:
        print(f"\n❌ Archiving encountered issues. Check logs for details.")
    
    return success


def create_optimized_views(manager):
    """Create optimized aggregated views for large datasets"""
    
    print(f"\n🔧 CREATING OPTIMIZED VIEWS")
    print("="*40)
    
    print("Creating aggregated views for large-scale queries...")
    
    success = manager.create_large_scale_aggregated_views()
    
    if success:
        print(f"\n✅ Optimized views created:")
        print(f"   • daily_sales_aggregated - Last 90 days")
        print(f"   • weekly_sales_aggregated - Last 365 days") 
        print(f"   • monthly_inventory_aggregated - Last 365 days")
        
        print(f"\n💡 Usage Examples:")
        print(f"   -- Recent daily sales:")
        print(f"   SELECT * FROM daily_sales_aggregated WHERE date >= '2024-01-01'")
        print(f"   ")
        print(f"   -- Weekly trends:")
        print(f"   SELECT week, SUM(total_revenue) FROM weekly_sales_aggregated")
        print(f"   WHERE week >= '2024-01-01' GROUP BY week")
        print(f"   ")
        print(f"   -- Monthly inventory:")
        print(f"   SELECT * FROM monthly_inventory_aggregated WHERE month >= '2024-01-01'")
        
    else:
        print(f"\n❌ Failed to create optimized views")
    
    return success


def run_large_scale_sync():
    """Run synchronization optimized for large datasets"""
    
    print(f"\n🔄 LARGE-SCALE SYNCHRONIZATION")
    print("="*40)
    
    try:
        # Initialize managers
        bq_client = BigQueryManager()
        storage_manager = BigQueryStorageManager(bq_client.project_id, bq_client.dataset)
        large_scale_manager = LargeScaleDataManager(bq_client.project_id, bq_client.dataset)
        
        # Check storage status
        storage_status = storage_manager.get_storage_usage()
        
        if storage_status['status'] == 'CRITICAL':
            print(f"🚨 Storage quota exceeded ({storage_status['total_storage_gb']:.2f} GB)")
            print(f"   Archiving required before synchronization...")
            
            # Auto-archive for critical situations
            strategy = large_scale_manager.create_large_scale_archiving_strategy()
            if strategy['archiving_plan']:
                print(f"   Auto-archiving {len(strategy['archiving_plan'])} tables...")
                execute_large_scale_archiving(large_scale_manager, strategy, interactive=False)
        
        # Run storage-aware synchronization
        from src.utils.inventory_sales_sync import StorageOptimizedSynchronizer
        
        sync = StorageOptimizedSynchronizer(bq_client, storage_manager)
        
        # Use shorter date range for large datasets
        results = sync.run_storage_aware_sync(days_back=30)  # Only 30 days for 471K/2M rows
        
        if results['status'] == 'SUCCESS':
            print(f"✅ Large-scale synchronization completed!")
            
            report = results['report']
            summary = report['summary']
            
            print(f"\n📊 Synchronization Results:")
            print(f"   Records Analyzed: {summary['total_records_analyzed']:,}")
            print(f"   Critical Issues: {summary['critical_variance_records']}")
            print(f"   Average Variance: {summary['average_variance_percentage']:.2f}%")
            print(f"   Processing Time: Optimized for large datasets")
            
        else:
            print(f"❌ Synchronization failed: {results['reason']}")
    
    except Exception as e:
        print(f"❌ Large-scale sync failed: {str(e)}")


def generate_performance_report(manager):
    """Generate performance optimization report"""
    
    print(f"\n📈 PERFORMANCE OPTIMIZATION REPORT")
    print("="*50)
    
    recommendations = manager.optimize_large_scale_queries()
    
    print(f"\n🚀 QUERY OPTIMIZATION STRATEGIES:")
    for category, tips in recommendations.items():
        print(f"\n📋 {category.replace('_', ' ').upper()}:")
        for i, tip in enumerate(tips, 1):
            print(f"   {i}. {tip}")
    
    print(f"\n💻 SAMPLE OPTIMIZED QUERIES:")
    print(f"   -- Instead of full table scan:")
    print(f"   SELECT * FROM fact_sales WHERE date >= '2024-01-01' LIMIT 1000")
    print(f"   ")
    print(f"   -- Use aggregated views for trends:")
    print(f"   SELECT week, SUM(total_revenue) as weekly_revenue")
    print(f"   FROM weekly_sales_aggregated")
    print(f"   WHERE week >= '2024-01-01' GROUP BY week ORDER BY week")
    print(f"   ")
    print(f"   -- Product-specific analysis:")
    print(f"   SELECT product_id, SUM(quantity) as total_sold")
    print(f"   FROM daily_sales_aggregated")
    print(f"   WHERE date >= '2024-01-01' AND product_id = 'PROD123'")
    print(f"   GROUP BY product_id")


def main():
    """Main large-scale management script"""
    
    parser = argparse.ArgumentParser(description='FMCG Large-Scale Data Management')
    parser.add_argument('--project-id', type=str, help='GCP Project ID')
    parser.add_argument('--dataset', type=str, help='BigQuery Dataset Name')
    parser.add_argument('--analyze', action='store_true', help='Analyze large-scale data')
    parser.add_argument('--archive', action='store_true', help='Execute large-scale archiving')
    parser.add_argument('--optimize-views', action='store_true', help='Create optimized views')
    parser.add_argument('--sync', action='store_true', help='Run large-scale synchronization')
    parser.add_argument('--performance', action='store_true', help='Show performance optimization guide')
    parser.add_argument('--batch-size', type=int, default=50000, help='Batch processing size')
    parser.add_argument('--archive-days', type=int, default=180, help='Days threshold for archiving')
    
    args = parser.parse_args()
    
    # Default project and dataset
    project_id = args.project_id or 'fmcg-data-generator'
    dataset = args.dataset or 'fmcg_warehouse'
    
    print(f"🗄️  FMCG Large-Scale Data Management")
    print(f"   Project: {project_id}")
    print(f"   Dataset: {dataset}")
    print(f"   Scale: 471K sales, 2M inventory records")
    
    try:
        # Initialize large-scale manager
        manager = LargeScaleDataManager(project_id, dataset)
        
        # Override configurations if provided
        if args.batch_size:
            manager.batch_size = args.batch_size
        if args.archive_days:
            manager.archive_threshold_days = args.archive_days
        
        # Run analysis
        results = run_large_scale_optimization(project_id, dataset)
        
        # Always show analysis
        print_large_scale_analysis(results)
        
        # Handle specific actions
        if args.archive:
            execute_large_scale_archiving(manager, results['archiving_strategy'])
        
        elif args.optimize_views:
            create_optimized_views(manager)
        
        elif args.sync:
            run_large_scale_sync()
        
        elif args.performance:
            generate_performance_report(manager)
        
        elif not args.analyze:
            # Default recommendations
            strategy = results['archiving_strategy']
            if strategy['immediate_actions']:
                print(f"\n🚨 RECOMMENDED ACTIONS:")
                print(f"   1. Archive old data: python {__file__} --archive")
                print(f"   2. Create optimized views: python {__file__} --optimize-views")
                print(f"   3. Run storage-aware sync: python {__file__} --sync")
            else:
                print(f"\n✅ Large-scale data is optimized!")
                print(f"   Consider performance tuning: python {__file__} --performance")
        
        print(f"\n🎉 Large-scale management completed!")
        return 0
        
    except Exception as e:
        print(f"❌ Large-scale management failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
