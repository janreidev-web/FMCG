#!/usr/bin/env python3
"""Test daily sales transaction range is 99-148"""

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
            return pd.DataFrame([{
                'campaign_id': 'CAM000000000000001',
                'campaign_name': 'Test Campaign',
                'status': 'Active'
            }])
        elif "MAX(CAST(REGEXP_EXTRACT" in query:
            # Return existing max sale_id
            return pd.DataFrame([{'max_id': 100}])
        elif "COUNT(*) as count" in query:
            # Return 0 for existing sales check (no sales for target date)
            return pd.DataFrame([{'count': 0}])
        else:
            return pd.DataFrame()

def main():
    print("=== DAILY TRANSACTION RANGE TEST ===\n")
    
    # Test the pipeline
    mock_bq = MockBigQueryManager('test-project', 'test-dataset')
    pipeline = ETLPipeline(bq_manager=mock_bq)
    
    config = {'daily_sales_amount': 2000000}
    
    print("📋 TESTING DAILY TRANSACTION RANGE")
    print("=" * 50)
    
    # Test multiple runs to verify range
    transaction_counts = []
    
    for run in range(10):
        try:
            # Generate daily sales
            sales_df = pipeline._generate_daily_sales(config)
            transaction_count = len(sales_df)
            transaction_counts.append(transaction_count)
            print(f"Run {run + 1}: {transaction_count} transactions")
        except Exception as e:
            print(f"Run {run + 1}: ERROR - {e}")
    
    if transaction_counts:
        print(f"\n📊 TRANSACTION RANGE ANALYSIS:")
        print(f"  Min: {min(transaction_counts)}")
        print(f"  Max: {max(transaction_counts)}")
        print(f"  Average: {sum(transaction_counts) / len(transaction_counts):.1f}")
        print(f"  Expected Range: 99-148")
        
        # Check if all counts are within expected range
        in_range = all(99 <= count <= 148 for count in transaction_counts)
        print(f"  All in range: {'✅ YES' if in_range else '❌ NO'}")
        
        if not in_range:
            out_of_range = [count for count in transaction_counts if not (99 <= count <= 148)]
            print(f"  Out of range: {out_of_range}")
        
        print(f"\n🎯 RESULT: {'✅ PASSED' if in_range else '❌ FAILED'}")
    else:
        print("❌ No successful runs")

if __name__ == '__main__':
    main()
