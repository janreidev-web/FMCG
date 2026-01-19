"""
Synchronization Script for FMCG Sales and Inventory Data
Standalone script to run synchronization validation and adjustments
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import argparse
from typing import Dict

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from src.utils.bigquery_client import BigQueryManager
from src.utils.inventory_sales_sync import InventorySalesSynchronizer, run_synchronization_validation
from src.utils.logger import default_logger


def print_sync_report(results: Dict) -> None:
    """Print formatted synchronization report"""
    
    report = results['report']
    summary = report['summary']
    
    print("\n" + "="*80)
    print("FMCG SALES-INVENTORY SYNCHRONIZATION REPORT")
    print("="*80)
    
    print(f"\n📊 SUMMARY STATISTICS:")
    print(f"   Records Analyzed: {summary['total_records_analyzed']:,}")
    print(f"   Critical Variance: {summary['critical_variance_records']} ({summary['critical_percentage']:.1f}%)")
    print(f"   Warning Variance: {summary['warning_variance_records']}")
    print(f"   Acceptable Variance: {summary['acceptable_variance_records']}")
    print(f"   Average Variance: {summary['average_variance_percentage']:.2f}%")
    print(f"   Maximum Variance: {summary['maximum_variance_percentage']:.2f}%")
    
    print(f"\n📈 VOLUME STATISTICS:")
    stats = report['statistics']
    print(f"   Total Sales Quantity: {stats['total_sales_quantity']:,}")
    print(f"   Total Inventory Stock Sold: {stats['total_inventory_stock_sold']:,}")
    print(f"   Overall Variance Amount: {stats['overall_variance_amount']:,}")
    print(f"   Affected SKUs: {stats['affected_skus']}")
    print(f"   Affected Dates: {stats['affected_dates']}")
    
    if report['critical_issues']:
        print(f"\n🚨 CRITICAL ISSUES (Top 10):")
        for i, issue in enumerate(report['critical_issues'][:10], 1):
            print(f"   {i}. SKU: {issue['sku']} | Date: {issue['date']}")
            print(f"      Sales: {issue['sales_quantity']} | Inventory: {issue['inventory_stock_sold']}")
            print(f"      Variance: {issue['variance_percentage']:.2f}% ({issue['variance_amount']} units)")
    
    if report['recommendations']:
        print(f"\n💡 RECOMMENDATIONS:")
        for i, rec in enumerate(report['recommendations'], 1):
            print(f"   {i}. {rec}")
    
    adjustments = report['adjustments']
    print(f"\n🔧 ADJUSTMENTS REQUIRED:")
    print(f"   Inventory Adjustments: {adjustments['inventory_adjustments_count']}")
    print(f"   Sales Adjustments: {adjustments['sales_adjustments_count']}")
    print(f"   Total Adjustment Quantity: {adjustments['total_adjustment_quantity']:,}")


def print_sku_summary(synchronizer: InventorySalesSynchronizer) -> None:
    """Print SKU-level synchronization summary"""
    
    try:
        sku_summary = synchronizer.get_sku_level_summary()
        
        print(f"\n📦 TOP 10 SKU VARIANCES:")
        print("-" * 100)
        print(f"{'SKU':<15} {'Product Name':<30} {'Sales Qty':<10} {'Inv Qty':<10} {'Variance':<10} {'Var %':<8}")
        print("-" * 100)
        
        for _, sku in sku_summary.head(10).iterrows():
            print(f"{sku['sku']:<15} {sku['product_name'][:28]:<30} "
                  f"{sku['sales_quantity']:<10} {sku['stock_sold']:<10} "
                  f"{sku['variance']:<10} {sku['variance_percentage']:<8.1f}")
        
    except Exception as e:
        print(f"Error generating SKU summary: {str(e)}")


def apply_adjustments_interactive(synchronizer: InventorySalesSynchronizer, 
                                inventory_adjustments) -> bool:
    """Interactive adjustment application"""
    
    if len(inventory_adjustments) == 0:
        print("\n✅ No adjustments needed!")
        return True
    
    print(f"\n🔧 ADJUSTMENT PREVIEW:")
    print(f"   Total adjustments to apply: {len(inventory_adjustments)}")
    print(f"   Total quantity adjustment: {inventory_adjustments['adjustment_quantity'].sum():,}")
    
    # Show sample adjustments
    print(f"\n   Sample adjustments (first 5):")
    for _, adj in inventory_adjustments.head(5).iterrows():
        print(f"   - {adj['product_id']} ({adj['date']}): "
              f"{adj['adjustment_type']} = {adj['adjustment_quantity']} units")
    
    while True:
        response = input(f"\n❓ Apply these adjustments? (y/n): ").lower().strip()
        
        if response == 'y':
            success = synchronizer.apply_synchronization_adjustments(inventory_adjustments)
            if success:
                print("✅ Adjustments applied successfully!")
                return True
            else:
                print("❌ Failed to apply adjustments!")
                return False
        
        elif response == 'n':
            print("⏭️  Adjustments skipped.")
            return False
        
        else:
            print("Please enter 'y' or 'n'")


def main():
    """Main synchronization script"""
    
    parser = argparse.ArgumentParser(description='FMCG Sales-Inventory Synchronization')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--apply-adjustments', action='store_true', 
                       help='Apply synchronization adjustments')
    parser.add_argument('--interactive', action='store_true', 
                       help='Interactive mode for applying adjustments')
    parser.add_argument('--sku-summary', action='store_true', 
                       help='Show detailed SKU-level summary')
    
    args = parser.parse_args()
    
    # Set default date range (last 30 days)
    if not args.end_date:
        end_date = datetime.now().date()
    else:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    
    if not args.start_date:
        start_date = end_date - timedelta(days=30)
    else:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    
    print(f"🔄 Running synchronization validation for {start_date} to {end_date}")
    
    try:
        # Initialize BigQuery client
        bq_client = BigQueryManager()
        
        # Run synchronization validation
        results = run_synchronization_validation(bq_client, str(start_date), str(end_date))
        
        # Print report
        print_sync_report(results)
        
        # Print SKU summary if requested
        if args.sku_summary:
            print_sku_summary(results['sync_analysis'])
        
        # Handle adjustments
        inventory_adjustments = results['inventory_adjustments']
        
        if args.apply_adjustments:
            if args.interactive:
                apply_adjustments_interactive(
                    InventorySalesSynchronizer(bq_client), 
                    inventory_adjustments
                )
            else:
                print(f"\n🔧 Applying {len(inventory_adjustments)} adjustments automatically...")
                success = InventorySalesSynchronizer(bq_client).apply_synchronization_adjustments(
                    inventory_adjustments
                )
                if success:
                    print("✅ Adjustments applied successfully!")
                else:
                    print("❌ Failed to apply adjustments!")
        
        print(f"\n✅ Synchronization validation completed!")
        print(f"   Log files saved to: logs/synchronization_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        
        return 0
        
    except Exception as e:
        print(f"❌ Synchronization failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
