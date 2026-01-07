#!/usr/bin/env python3
"""Test that daily sales prioritize latest campaigns"""

import pandas as pd
from datetime import datetime, timedelta
from src.etl.pipeline import ETLPipeline

# Create a mock BigQueryManager for testing
class MockBigQueryManager:
    def __init__(self, project_id, dataset, credentials_path=None):
        self.project_id = project_id
        self.dataset = dataset
        self.credentials_path = credentials_path
        self.mock_data = {}
    
    def ensure_dataset(self):
        pass
    
    def create_table(self, table_name, schema):
        pass
    
    def load_dataframe(self, df, table_name, mode="WRITE_TRUNCATE"):
        pass
    
    def execute_query(self, query):
        # Mock responses for incremental update queries
        if "dim_products" in query:
            return pd.DataFrame([{
                'product_id': 'PRO000000000000001',
                'product_name': 'Test Product',
                'unit_price': 100.0,
                'cost': 60.0,
                'status': 'Active'
            }])
        elif "dim_retailers" in query:
            return pd.DataFrame([{
                'retailer_id': 'RET000000000000001',
                'retailer_name': 'Test Retailer',
                'retailer_type': 'Supermarket',
                'status': 'Active'
            }])
        elif "dim_employees" in query:
            return pd.DataFrame([{
                'employee_id': 'EMP000000000000001',
                'first_name': 'Test',
                'last_name': 'Employee',
                'termination_date': None
            }])
        elif "dim_campaigns" in query:
            # Return campaigns with different start dates
            return pd.DataFrame([
                {
                    'campaign_id': 'CAM000000000000001',
                    'campaign_name': 'Old Campaign 2020',
                    'start_date': datetime(2020, 1, 1).date(),
                    'status': 'Active'
                },
                {
                    'campaign_id': 'CAM000000000000002', 
                    'campaign_name': 'Medium Campaign 2022',
                    'start_date': datetime(2022, 6, 15).date(),
                    'status': 'Active'
                },
                {
                    'campaign_id': 'CAM000000000000003',
                    'campaign_name': 'Latest Campaign 2025',
                    'start_date': datetime(2025, 12, 1).date(),
                    'status': 'Active'
                }
            ])
        elif "MAX(CAST(REGEXP_EXTRACT" in query:
            # Return existing max sale_id
            return pd.DataFrame([{'max_id': 100}])
        elif "COUNT(*) as count" in query:
            # Return 0 for existing sales check (no sales for target date)
            return pd.DataFrame([{'count': 0}])
        else:
            return pd.DataFrame()

def main():
    print("=== LATEST CAMPAIGN SELECTION TEST ===\n")
    
    # Test the pipeline
    mock_bq = MockBigQueryManager('test-project', 'test-dataset')
    pipeline = ETLPipeline(bq_manager=mock_bq)
    
    config = {'daily_sales_amount': 2000000}
    
    print("📋 TESTING LATEST CAMPAIGN PRIORITIZATION")
    print("=" * 50)
    
    try:
        # Generate daily sales
        sales_df = pipeline._generate_daily_sales(config)
        
        if len(sales_df) > 0:
            print(f"Generated {len(sales_df)} sales records")
            
            # Count campaigns used
            campaign_counts = sales_df['campaign_id'].value_counts()
            print(f"\n📊 CAMPAIGN USAGE:")
            print(f"  Total sales with campaigns: {len(sales_df[sales_df['campaign_id'].notna()])}")
            
            for campaign_id, count in campaign_counts.items():
                if pd.notna(campaign_id):
                    print(f"  {campaign_id}: {count} sales")
            
            # Check if latest campaign is prioritized
            latest_campaign_id = 'CAM000000000000003'  # Latest campaign from mock data
            if latest_campaign_id in campaign_counts:
                latest_count = campaign_counts[latest_campaign_id]
                total_with_campaigns = len(sales_df[sales_df['campaign_id'].notna()])
                percentage = (latest_count / total_with_campaigns * 100) if total_with_campaigns > 0 else 0
                print(f"\n🎯 LATEST CAMPAIGN ANALYSIS:")
                print(f"  Latest campaign ({latest_campaign_id}): {latest_count} sales ({percentage:.1f}%)")
                print(f"  Expected: Should be majority of campaign-assigned sales")
                print(f"  Result: {'✅ PRIORITIZED' if percentage > 50 else '❌ NOT PRIORITIZED'}")
            else:
                print(f"\n❌ Latest campaign not found in sales")
            
            # Show sample records with campaigns
            print(f"\n📋 SAMPLE SALES WITH CAMPAIGNS:")
            campaign_sales = sales_df[sales_df['campaign_id'].notna()].head(5)
            for _, sale in campaign_sales.iterrows():
                print(f"  Sale {sale['sale_id'][:15]}... → Campaign {sale['campaign_id']}")
                
        else:
            print("❌ No sales records generated")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
