"""
Historical Data Synchronization Script
Runs full historical dataset synchronization (471K sales + 2M inventory)
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
    from src.utils.inventory_sales_sync import InventorySalesSynchronizer
    from src.utils.bigquery_client import BigQueryManager
    from src.utils.historical_data_manager import FullHistoricalDataManager
except ImportError:
    print("⚠️  Some dependencies not available. Using fallback mode.")


def run_historical_synchronization():
    """Run synchronization on complete historical dataset"""
    
    print("🔄 HISTORICAL DATA SYNCHRONIZATION")
    print("="*50)
    print("📊 Dataset Scale: 471K sales records, 2M inventory records")
    print("⏱️  Expected Duration: 5-10 minutes")
    print("💾 Storage Required: ~26.3 GB")
    
    try:
        # Initialize BigQuery client
        bq_client = BigQueryManager()
        
        # Initialize synchronizer with historical mode
        synchronizer = InventorySalesSynchronizer(bq_client)
        
        print("\n🔄 Loading complete historical dataset...")
        
        # Load full historical data
        synchronizer.load_data(historical_mode=True)
        
        print(f"✅ Data loaded successfully!")
        print(f"   Sales: {len(synchronizer.sales_data):,} records")
        print(f"   Inventory: {len(synchronizer.inventory_data):,} records")
        print(f"   Products: {len(synchronizer.product_data):,} records")
        
        if len(synchronizer.sales_data) > 0:
            print(f"   Sales Date Range: {synchronizer.sales_data['sale_date'].min()} to {synchronizer.sales_data['sale_date'].max()}")
        if len(synchronizer.inventory_data) > 0:
            print(f"   Inventory Date Range: {synchronizer.inventory_data['inventory_date'].min()} to {synchronizer.inventory_data['inventory_date'].max()}")
        
        print("\n🔍 Analyzing synchronization gaps...")
        
        # Analyze gaps
        sync_analysis = synchronizer.analyze_synchronization_gaps()
        
        print(f"✅ Analysis completed!")
        print(f"   Total Comparisons: {len(sync_analysis):,}")
        
        # Generate report
        report = synchronizer.generate_synchronization_report(sync_analysis)
        
        print(f"\n📊 SYNCHRONIZATION RESULTS:")
        summary = report['summary']
        
        print(f"   📈 Records Analyzed: {summary['total_records_analyzed']:,}")
        print(f"   🚨 Critical Issues: {summary['critical_variance_records']:,}")
        print(f"   ⚠️  Warning Issues: {summary['warning_variance_records']:,}")
        print(f"   ✅ Acceptable: {summary['acceptable_variance_records']:,}")
        print(f"   📊 Average Variance: {summary['average_variance_percentage']:.2f}%")
        print(f"   📈 Maximum Variance: {summary['maximum_variance_percentage']:.2f}%")
        
        print(f"\n💰 VOLUME STATISTICS:")
        stats = report['statistics']
        print(f"   Total Sales Quantity: {stats['total_sales_quantity']:,}")
        print(f"   Total Inventory Stock Sold: {stats['total_inventory_stock_sold']:,}")
        print(f"   Overall Variance Amount: {stats['overall_variance_amount']:,}")
        print(f"   Affected SKUs: {stats['affected_skus']:,}")
        print(f"   Affected Dates: {stats['affected_dates']:,}")
        
        # Show critical issues
        if report['critical_issues']:
            print(f"\n🚨 TOP 5 CRITICAL ISSUES:")
            for i, issue in enumerate(report['critical_issues'][:5], 1):
                print(f"   {i}. SKU: {issue['sku']} | Date: {issue['date']}")
                print(f"      Sales: {issue['sales_quantity']} vs Inventory: {issue['inventory_stock_sold']}")
                print(f"      Variance: {issue['variance_percentage']:.2f}% ({issue['variance_amount']} units)")
        
        # Show recommendations
        if report['recommendations']:
            print(f"\n💡 RECOMMENDATIONS:")
            for i, rec in enumerate(report['recommendations'], 1):
                print(f"   {i}. {rec}")
        
        # Create adjustments
        inventory_adjustments, sales_adjustments = synchronizer.create_synchronization_adjustments(sync_analysis)
        
        print(f"\n🔧 ADJUSTMENTS GENERATED:")
        print(f"   Inventory Adjustments: {len(inventory_adjustments):,}")
        print(f"   Sales Adjustments: {len(sales_adjustments):,}")
        print(f"   Total Adjustment Quantity: {inventory_adjustments['adjustment_quantity'].sum() if len(inventory_adjustments) > 0 else 0:,}")
        
        # Ask if user wants to apply adjustments
        if len(inventory_adjustments) > 0:
            print(f"\n❓ Apply adjustments to synchronize data?")
            print(f"   This will update {len(inventory_adjustments):,} inventory records")
            
            response = input("Apply adjustments? (y/n): ").lower().strip()
            
            if response == 'y':
                print("\n🔄 Applying adjustments...")
                success = synchronizer.apply_synchronization_adjustments(inventory_adjustments)
                
                if success:
                    print("✅ Adjustments applied successfully!")
                else:
                    print("❌ Failed to apply adjustments")
            else:
                print("⏭️  Adjustments skipped")
        
        print(f"\n🎉 Historical synchronization completed!")
        return True
        
    except Exception as e:
        print(f"❌ Historical synchronization failed: {str(e)}")
        return False


def run_period_analysis(start_date: str, end_date: str):
    """Run synchronization for specific period"""
    
    print(f"📅 PERIOD-SPECIFIC SYNCHRONIZATION")
    print("="*45)
    print(f"📊 Period: {start_date} to {end_date}")
    
    try:
        # Initialize BigQuery client
        bq_client = BigQueryManager()
        
        # Initialize synchronizer
        synchronizer = InventorySalesSynchronizer(bq_client)
        
        print(f"\n🔄 Loading data for specified period...")
        
        # Load data for specific period
        synchronizer.load_data(start_date=start_date, end_date=end_date, storage_aware=False)
        
        print(f"✅ Data loaded successfully!")
        print(f"   Sales: {len(synchronizer.sales_data):,} records")
        print(f"   Inventory: {len(synchronizer.inventory_data):,} records")
        
        if len(synchronizer.sales_data) == 0 and len(synchronizer.inventory_data) == 0:
            print(f"⚠️  No data found for the specified period")
            return False
        
        # Analyze gaps
        sync_analysis = synchronizer.analyze_synchronization_gaps()
        
        # Generate report
        report = synchronizer.generate_synchronization_report(sync_analysis)
        
        print(f"\n📊 PERIOD SYNCHRONIZATION RESULTS:")
        summary = report['summary']
        
        print(f"   Records Analyzed: {summary['total_records_analyzed']:,}")
        print(f"   Critical Issues: {summary['critical_variance_records']:,}")
        print(f"   Warning Issues: {summary['warning_variance_records']:,}")
        print(f"   Average Variance: {summary['average_variance_percentage']:.2f}%")
        
        # Volume statistics
        stats = report['statistics']
        print(f"\n💰 PERIOD VOLUME:")
        print(f"   Total Sales Quantity: {stats['total_sales_quantity']:,}")
        print(f"   Total Inventory Stock Sold: {stats['total_inventory_stock_sold']:,}")
        print(f"   Overall Variance Amount: {stats['overall_variance_amount']:,}")
        
        return True
        
    except Exception as e:
        print(f"❌ Period synchronization failed: {str(e)}")
        return False


def main():
    """Main historical synchronization script"""
    
    parser = argparse.ArgumentParser(description='FMCG Historical Data Synchronization')
    parser.add_argument('--project-id', type=str, help='GCP Project ID')
    parser.add_argument('--dataset', type=str, help='BigQuery Dataset Name')
    parser.add_argument('--historical', action='store_true', help='Run full historical synchronization')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--batch-size', type=int, default=100000, help='Batch processing size')
    
    args = parser.parse_args()
    
    # Default project and dataset
    project_id = args.project_id or 'fmcg-data-generator'
    dataset = args.dataset or 'fmcg_warehouse'
    
    print(f"🔄 FMCG Historical Data Synchronization")
    print(f"   Project: {project_id}")
    print(f"   Dataset: {dataset}")
    print(f"   Scale: 471K sales, 2M inventory records")
    
    try:
        if args.historical:
            # Run full historical synchronization
            success = run_historical_synchronization()
        elif args.start_date and args.end_date:
            # Run period-specific synchronization
            success = run_period_analysis(args.start_date, args.end_date)
        else:
            # Default: show options
            print(f"\n🚀 SYNCHRONIZATION OPTIONS:")
            print(f"   1. Full historical dataset:")
            print(f"      python {__file__} --historical")
            print(f"   ")
            print(f"   2. Specific period:")
            print(f"      python {__file__} --start-date 2024-01-01 --end-date 2024-12-31")
            print(f"   ")
            print(f"   3. Recent period (last 90 days):")
            print(f"      python {__file__} --start-date 2024-10-01 --end-date 2024-12-31")
            
            # Ask user what they want to do
            print(f"\n❓ What would you like to synchronize?")
            print(f"   1. Full historical dataset (471K + 2M records)")
            print(f"   2. Specific time period")
            print(f"   3. Exit")
            
            choice = input("Enter choice (1-3): ").strip()
            
            if choice == '1':
                success = run_historical_synchronization()
            elif choice == '2':
                start_date = input("Enter start date (YYYY-MM-DD): ").strip()
                end_date = input("Enter end date (YYYY-MM-DD): ").strip()
                success = run_period_analysis(start_date, end_date)
            else:
                print("Exiting...")
                return 0
        
        if success:
            print(f"\n✅ Synchronization completed successfully!")
            return 0
        else:
            print(f"\n❌ Synchronization failed!")
            return 1
        
    except Exception as e:
        print(f"❌ Synchronization error: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
