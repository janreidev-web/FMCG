"""
Historical Data Management Script for FMCG Platform
Handles complete 471K sales + 2M inventory records efficiently
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
    from src.utils.historical_data_manager import FullHistoricalDataManager, run_full_historical_analysis
    from src.utils.inventory_sales_sync import InventorySalesSynchronizer
    from src.utils.bigquery_client import BigQueryManager
except ImportError:
    print("⚠️  Some dependencies not available. Using fallback mode.")


def print_historical_analysis_report(dataset_info):
    """Print formatted historical dataset analysis"""
    
    print("\n" + "="*80)
    print("📊 HISTORICAL DATASET ANALYSIS REPORT")
    print("="*80)
    
    # Scale summary
    scale = dataset_info['scale_summary']
    print(f"\n🗄️  DATASET SCALE:")
    print(f"   Total Records: {scale['total_records']:,}")
    print(f"   Sales Records: {scale['total_sales_rows']:,}")
    print(f"   Inventory Records: {scale['total_inventory_rows']:,}")
    print(f"   Estimated Storage: {scale['estimated_storage_gb']:.1f} GB")
    
    # Date ranges
    if 'date_ranges' in dataset_info and dataset_info['date_ranges']:
        ranges = dataset_info['date_ranges']
        print(f"\n📅 DATA COVERAGE:")
        
        if 'sales_span' in ranges and ranges['sales_span']:
            sales_span = ranges['sales_span']
            print(f"   Sales Data:")
            print(f"     Range: {sales_span.get('earliest_date', 'N/A')} to {sales_span.get('latest_date', 'N/A')}")
            print(f"     Span: {sales_span.get('day_span', 'N/A')} days")
        
        if 'inventory_span' in ranges and ranges['inventory_span']:
            inv_span = ranges['inventory_span']
            print(f"   Inventory Data:")
            print(f"     Range: {inv_span.get('earliest_date', 'N/A')} to {inv_span.get('latest_date', 'N/A')}")
            print(f"     Span: {inv_span.get('day_span', 'N/A')} days")
        
        if 'overall_span' in ranges and ranges['overall_span']:
            overall = ranges['overall_span']
            print(f"   Overall:")
            print(f"     Range: {overall.get('overall_earliest', 'N/A')} to {overall.get('overall_latest', 'N/A')}")
            print(f"     Total Span: {overall.get('overall_span_days', 'N/A')} days")
    
    # Table details
    print(f"\n📋 TABLE ANALYSIS:")
    for table_name, details in dataset_info.get('table_details', {}).items():
        print(f"\n🔹 {table_name.upper()}:")
        
        if 'basic_stats' in details:
            stats = details['basic_stats']
            print(f"   📊 Basic Statistics:")
            print(f"     Total Rows: {stats.get('total_rows', 'N/A'):,}")
            print(f"     Date Range: {stats.get('earliest_date', 'N/A')} to {stats.get('latest_date', 'N/A')}")
            print(f"     Unique Dates: {stats.get('unique_dates', 'N/A')}")
        
        if 'cardinality' in details:
            cardinality = details['cardinality']
            print(f"   🎯 Data Cardinality:")
            if 'unique_products' in cardinality:
                print(f"     Unique Products: {cardinality['unique_products']:,}")
            if 'unique_retailers' in cardinality:
                print(f"     Unique Retailers: {cardinality['unique_retailers']:,}")
            if 'unique_employees' in cardinality:
                print(f"     Unique Employees: {cardinality['unique_employees']:,}")
            if 'unique_locations' in cardinality:
                print(f"     Unique Locations: {cardinality['unique_locations']:,}")
            
            # Financial metrics
            if 'total_revenue' in cardinality:
                print(f"     Total Revenue: ₱{cardinality['total_revenue']:,.0f}")
            if 'total_inventory_value' in cardinality:
                print(f"     Inventory Value: ₱{cardinality['total_inventory_value']:,.0f}")
            
            # Averages
            if 'avg_quantity' in cardinality:
                print(f"     Average Quantity: {cardinality['avg_quantity']:.1f}")
            if 'avg_amount' in cardinality:
                print(f"     Average Amount: ₱{cardinality['avg_amount']:.0f}")
        
        if 'monthly_trends' in details and details['monthly_trends']:
            print(f"   📈 Monthly Trends (Top 5 months):")
            trends = details['monthly_trends'][:5]
            for i, trend in enumerate(trends, 1):
                month = trend.get('month', 'N/A')
                if table_name == 'fact_sales':
                    records = trend.get('monthly_records', 0)
                    revenue = trend.get('monthly_revenue', 0)
                    print(f"     {i}. {month}: {records:,} records, ₱{revenue:,.0f} revenue")
                else:
                    records = trend.get('monthly_records', 0)
                    value = trend.get('monthly_value', 0)
                    print(f"     {i}. {month}: {records:,} records, ₱{value:,.0f} value")
    
    # Data quality
    if 'data_quality' in dataset_info and dataset_info['data_quality']:
        quality = dataset_info['data_quality']
        print(f"\n✅ DATA QUALITY ASSESSMENT:")
        
        if 'completeness' in quality:
            print(f"   📋 Completeness (Null Value Analysis):")
            for field, metrics in quality['completeness'].items():
                null_pct = metrics.get('null_percentage', 0)
                status = "✅" if null_pct < 1 else "⚠️" if null_pct < 5 else "❌"
                print(f"     {status} {field}: {null_pct:.2f}% null values")
    
    # Recommendations
    print(f"\n💡 HISTORICAL DATA RECOMMENDATIONS:")
    print(f"   1. Use aggregated views for trend analysis to improve performance")
    print(f"   2. Implement date-based filtering for specific time periods")
    print(f"   3. Consider batch processing for full dataset operations")
    print(f"   4. Monitor query costs and optimize frequently accessed data")
    print(f"   5. Create materialized views for critical historical aggregations")


def load_historical_data_interactive(manager):
    """Interactive historical data loading"""
    
    print(f"\n📥 HISTORICAL DATA LOADING")
    print("="*40)
    
    print(f"Available tables for loading:")
    print(f"   1. fact_sales ({manager.total_sales_rows:,} rows)")
    print(f"   2. fact_inventory ({manager.total_inventory_rows:,} rows)")
    print(f"   3. Both tables (full historical dataset)")
    
    while True:
        choice = input(f"\n❓ Select data to load (1-3): ").strip()
        
        if choice == '1':
            print(f"\n🔄 Loading fact_sales historical data...")
            sales_data = manager.load_full_historical_data('fact_sales')
            
            if len(sales_data) > 0:
                print(f"✅ Successfully loaded {len(sales_data):,} sales records")
                print(f"   Date range: {sales_data['date'].min()} to {sales_data['date'].max()}")
                print(f"   Revenue: ₱{sales_data['total_amount'].sum():,.0f}")
                print(f"   Products: {sales_data['product_id'].nunique():,}")
            else:
                print(f"❌ No sales data loaded")
            break
        
        elif choice == '2':
            print(f"\n🔄 Loading fact_inventory historical data...")
            inventory_data = manager.load_full_historical_data('fact_inventory')
            
            if len(inventory_data) > 0:
                print(f"✅ Successfully loaded {len(inventory_data):,} inventory records")
                print(f"   Date range: {inventory_data['date'].min()} to {inventory_data['date'].max()}")
                print(f"   Total Value: ₱{inventory_data['total_value'].sum():,.0f}")
                print(f"   Products: {inventory_data['product_id'].nunique():,}")
            else:
                print(f"❌ No inventory data loaded")
            break
        
        elif choice == '3':
            print(f"\n🔄 Loading complete historical dataset...")
            
            # Load sales data
            print(f"   Loading sales data...")
            sales_data = manager.load_full_historical_data('fact_sales')
            
            # Load inventory data
            print(f"   Loading inventory data...")
            inventory_data = manager.load_full_historical_data('fact_inventory')
            
            total_loaded = len(sales_data) + len(inventory_data)
            print(f"✅ Successfully loaded {total_loaded:,} total records")
            print(f"   Sales: {len(sales_data):,}")
            print(f"   Inventory: {len(inventory_data):,}")
            break
        
        else:
            print("Please enter 1, 2, or 3")


def create_historical_views(manager):
    """Create historical aggregated views"""
    
    print(f"\n🔧 CREATING HISTORICAL AGGREGATED VIEWS")
    print("="*45)
    
    print("Creating comprehensive historical views for full dataset analysis...")
    
    success = manager.create_historical_aggregated_views()
    
    if success:
        print(f"\n✅ Historical Views Created:")
        print(f"   📊 historical_daily_sales - Daily sales aggregations")
        print(f"   📊 historical_weekly_sales - Weekly sales trends")
        print(f"   📊 historical_monthly_sales - Monthly sales analysis")
        print(f"   📊 historical_daily_inventory - Daily inventory movements")
        print(f"   📊 historical_monthly_inventory - Monthly inventory trends")
        
        print(f"\n💡 Usage Examples:")
        print(f"   -- Daily sales analysis:")
        print(f"   SELECT date, SUM(daily_revenue) as revenue")
        print(f"   FROM historical_daily_sales")
        print(f"   WHERE date >= '2024-01-01' GROUP BY date")
        print(f"   ")
        print(f"   -- Monthly trends:")
        print(f"   SELECT month, SUM(weekly_revenue) as monthly_revenue")
        print(f"   FROM historical_weekly_sales")
        print(f"   WHERE month >= '2024-01-01' GROUP BY month")
        print(f"   ")
        print(f"   -- Product performance:")
        print(f"   SELECT product_id, SUM(monthly_quantity) as total_sold")
        print(f"   FROM historical_monthly_sales")
        print(f"   WHERE month >= '2024-01-01' GROUP BY product_id")
        print(f"   ORDER BY total_sold DESC LIMIT 10")
        
    else:
        print(f"\n❌ Failed to create historical views")
    
    return success


def run_historical_synchronization(manager):
    """Run synchronization on full historical dataset"""
    
    print(f"\n🔄 HISTORICAL SYNCHRONIZATION")
    print("="*35)
    
    print("Running synchronization analysis on complete historical dataset...")
    print("⚠️  This may take several minutes due to the large dataset size...")
    
    try:
        results = manager.run_historical_synchronization()
        
        if results['status'] == 'SUCCESS':
            print(f"✅ Historical synchronization completed!")
            
            sync_analysis = results['sync_analysis']
            summary = sync_analysis['summary']
            dataset_info = results['dataset_info']
            
            print(f"\n📊 Synchronization Results:")
            print(f"   Sales Records Analyzed: {dataset_info['sales_rows']:,}")
            print(f"   Inventory Records Analyzed: {dataset_info['inventory_rows']:,}")
            print(f"   Total Comparisons: {summary['total_comparisons']:,}")
            print(f"   Critical Variances: {summary['critical_variances']:,}")
            print(f"   Warning Variances: {summary['warning_variances']:,}")
            print(f"   Acceptable Variances: {summary['acceptable_variances']:,}")
            print(f"   Average Variance: {summary['average_variance_percentage']:.2f}%")
            print(f"   Max Variance: {summary['max_variance_percentage']:.2f}%")
            
            # Show top variances
            if 'top_variances' in sync_analysis and sync_analysis['top_variances']:
                print(f"\n🚨 Top 10 Variances:")
                for i, variance in enumerate(sync_analysis['top_variances'][:10], 1):
                    print(f"   {i}. Product: {variance.get('product_id', 'N/A')}")
                    print(f"      Date: {variance.get('date', 'N/A')}")
                    print(f"      Variance: {variance.get('variance_percentage', 0):.2f}%")
                    print(f"      Sales: {variance.get('quantity', 0)} vs Inventory: {variance.get('stock_sold', 0)}")
        else:
            print(f"❌ Historical synchronization failed: {results['reason']}")
    
    except Exception as e:
        print(f"❌ Historical synchronization error: {str(e)}")


def analyze_specific_period(manager):
    """Analyze specific time period from historical data"""
    
    print(f"\n📅 PERIOD-SPECIFIC ANALYSIS")
    print("="*35)
    
    # Get date range from user
    while True:
        start_date = input("Enter start date (YYYY-MM-DD): ").strip()
        try:
            datetime.strptime(start_date, '%Y-%m-%d')
            break
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD")
    
    while True:
        end_date = input("Enter end date (YYYY-MM-DD): ").strip()
        try:
            datetime.strptime(end_date, '%Y-%m-%d')
            break
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD")
    
    date_filter = f"date BETWEEN '{start_date}' AND '{end_date}'"
    
    print(f"\n🔍 Analyzing period {start_date} to {end_date}...")
    
    try:
        # Load filtered data
        sales_data = manager.load_full_historical_data('fact_sales', date_filter=date_filter)
        inventory_data = manager.load_full_historical_data('fact_inventory', date_filter=date_filter)
        
        print(f"\n📊 Period Analysis Results:")
        print(f"   Sales Records: {len(sales_data):,}")
        print(f"   Inventory Records: {len(inventory_data):,}")
        
        if len(sales_data) > 0:
            print(f"   Sales Revenue: ₱{sales_data['total_amount'].sum():,.0f}")
            print(f"   Average Sale: ₱{sales_data['total_amount'].mean():.0f}")
            print(f"   Products Sold: {sales_data['product_id'].nunique():,}")
        
        if len(inventory_data) > 0:
            print(f"   Inventory Value: ₱{inventory_data['total_value'].sum():,.0f}")
            print(f"   Total Stock Movement: {inventory_data['stock_sold'].sum():,}")
            print(f"   Products in Inventory: {inventory_data['product_id'].nunique():,}")
        
        # Quick sync check for the period
        if len(sales_data) > 0 and len(inventory_data) > 0:
            sales_agg = sales_data.groupby(['product_id', 'date'])['quantity'].sum().reset_index()
            inventory_agg = inventory_data.groupby(['product_id', 'date'])['stock_sold'].sum().reset_index()
            
            merged = sales_agg.merge(inventory_agg, on=['product_id', 'date'], how='outer')
            merged = merged.fillna(0)
            
            merged['variance'] = abs(merged['quantity'] - merged['stock_sold'])
            high_variance = merged[merged['variance'] > 0]
            
            print(f"   Synchronization Issues: {len(high_variance):,} product-date combinations")
            print(f"   Total Variance: {merged['variance'].sum():,} units")
        
    except Exception as e:
        print(f"❌ Period analysis failed: {str(e)}")


def main():
    """Main historical data management script"""
    
    parser = argparse.ArgumentParser(description='FMCG Historical Data Management')
    parser.add_argument('--project-id', type=str, help='GCP Project ID')
    parser.add_argument('--dataset', type=str, help='BigQuery Dataset Name')
    parser.add_argument('--analyze', action='store_true', help='Analyze full historical dataset')
    parser.add_argument('--load', action='store_true', help='Load historical data interactively')
    parser.add_argument('--views', action='store_true', help='Create historical aggregated views')
    parser.add_argument('--sync', action='store_true', help='Run historical synchronization')
    parser.add_argument('--period', action='store_true', help='Analyze specific time period')
    parser.add_argument('--batch-size', type=int, default=100000, help='Batch processing size')
    parser.add_argument('--timeout', type=int, default=600, help='Query timeout in seconds')
    
    args = parser.parse_args()
    
    # Default project and dataset
    project_id = args.project_id or 'fmcg-data-generator'
    dataset = args.dataset or 'fmcg_warehouse'
    
    print(f"📊 FMCG Historical Data Management")
    print(f"   Project: {project_id}")
    print(f"   Dataset: {dataset}")
    print(f"   Scale: 471K sales, 2M inventory records")
    print(f"   Storage: ~26.3 GB (full historical dataset)")
    
    try:
        # Initialize historical data manager
        manager = FullHistoricalDataManager(project_id, dataset)
        
        # Override configurations if provided
        if args.batch_size:
            manager.large_batch_size = args.batch_size
        if args.timeout:
            manager.query_timeout_seconds = args.timeout
        
        # Run analysis
        dataset_info = run_full_historical_analysis(project_id, dataset)
        
        # Always show analysis
        print_historical_analysis_report(dataset_info)
        
        # Handle specific actions
        if args.load:
            load_historical_data_interactive(manager)
        
        elif args.views:
            create_historical_views(manager)
        
        elif args.sync:
            run_historical_synchronization(manager)
        
        elif args.period:
            analyze_specific_period(manager)
        
        elif not args.analyze:
            # Default recommendations
            print(f"\n🚀 RECOMMENDED ACTIONS FOR HISTORICAL DATA:")
            print(f"   1. Create aggregated views: python {__file__} --views")
            print(f"   2. Run historical sync: python {__file__} --sync")
            print(f"   3. Analyze specific period: python {__file__} --period")
            print(f"   4. Load data interactively: python {__file__} --load")
        
        print(f"\n🎉 Historical data management completed!")
        return 0
        
    except Exception as e:
        print(f"❌ Historical data management failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
