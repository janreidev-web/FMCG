#!/usr/bin/env python3
"""Test chronological ID ordering for all fact tables"""

import pandas as pd
from datetime import datetime
from src.etl.pipeline import ETLPipeline

# Create a mock BigQueryManager for testing
class MockBigQueryManager:
    def __init__(self, project_id, dataset, credentials_path=None):
        self.project_id = project_id
        self.dataset = dataset
        self.credentials_path = credentials_path
    
    def ensure_dataset(self):
        pass
    
    def create_table(self, table_name, schema):
        pass
    
    def load_dataframe(self, df, table_name, mode="WRITE_TRUNCATE"):
        pass
    
    def execute_query(self, query):
        return pd.DataFrame()

def check_fact_chronological_order(df, date_column, id_column, entity_name):
    """Check if fact table IDs are in chronological order based on dates"""
    if len(df) == 0:
        print(f"\n📊 {entity_name}:")
        print(f"  No records found")
        return True
    
    # Sort by date to get expected order
    df_sorted = df.sort_values(date_column).reset_index(drop=True)
    
    # Sort by ID to get actual order
    df_by_id = df.sort_values(id_column).reset_index(drop=True)
    
    # Check if dates are in chronological order when sorted by ID
    dates_by_id = df_by_id[date_column].tolist()
    is_chronological = all(dates_by_id[i] >= dates_by_id[i-1] for i in range(1, len(dates_by_id)))
    
    print(f"\n📊 {entity_name}:")
    print(f"  Total records: {len(df)}")
    print(f"  Date range: {df[date_column].min()} to {df[date_column].max()}")
    print(f"  Chronological order: {'✅ YES' if is_chronological else '❌ NO'}")
    
    if not is_chronological:
        print(f"  ⚠️  First 5 by ID vs dates:")
        for i in range(min(5, len(df_by_id))):
            print(f"    ID {df_by_id.iloc[i][id_column]}: {df_by_id.iloc[i][date_column]}")
    
    return is_chronological

def main():
    print("=== FACT TABLES CHRONOLOGICAL ID ORDERING TEST ===\n")
    
    # Test the pipeline
    mock_bq = MockBigQueryManager('test-project', 'test-dataset')
    pipeline = ETLPipeline(bq_manager=mock_bq)
    
    config = {'locations_count': 3, 'initial_employees': 8, 'initial_products': 5, 'initial_retailers': 3, 'initial_campaigns': 4}
    pipeline.generate_dimension_data(config)
    pipeline.generate_fact_data(config)
    
    print("📋 FACT TABLES ID ORDERING ANALYSIS")
    print("=" * 60)
    
    results = {}
    
    # Test Sales (date)
    if 'fact_sales' in pipeline.data_cache:
        sales_df = pipeline.data_cache['fact_sales']
        # Sales uses IDGenerator, check if chronological
        sales_by_date = sales_df.sort_values('date').reset_index(drop=True)
        sales_by_id = sales_df.sort_values('sale_id').reset_index(drop=True)
        
        dates_by_id = sales_by_id['date'].tolist()
        is_chronological = all(dates_by_id[i] >= dates_by_id[i-1] for i in range(1, len(dates_by_id)))
        
        print(f"\n📊 Sales:")
        print(f"  Total records: {len(sales_df)}")
        print(f"  Date range: {sales_df['date'].min()} to {sales_df['date'].max()}")
        print(f"  Chronological order: {'✅ YES' if is_chronological else '❌ NO'}")
        results['sales'] = is_chronological
    
    # Test Inventory (date)
    if 'fact_inventory' in pipeline.data_cache:
        inventory_df = pipeline.data_cache['fact_inventory']
        results['inventory'] = check_fact_chronological_order(
            inventory_df, 'date', 'inventory_id', 'Inventory'
        )
    
    # Test Operating Costs (date)
    if 'fact_operating_costs' in pipeline.data_cache:
        costs_df = pipeline.data_cache['fact_operating_costs']
        results['operating_costs'] = check_fact_chronological_order(
            costs_df, 'date', 'cost_id', 'Operating Costs'
        )
    
    # Test Marketing Costs (date)
    if 'fact_marketing_costs' in pipeline.data_cache:
        marketing_df = pipeline.data_cache['fact_marketing_costs']
        results['marketing_costs'] = check_fact_chronological_order(
            marketing_df, 'date', 'marketing_cost_id', 'Marketing Costs'
        )
    
    # Test Employee Facts (date)
    if 'fact_employees' in pipeline.data_cache:
        employee_facts_df = pipeline.data_cache['fact_employees']
        # Employee facts use IDGenerator, check if chronological
        emp_facts_by_date = employee_facts_df.sort_values('date').reset_index(drop=True)
        emp_facts_by_id = employee_facts_df.sort_values('employee_fact_id').reset_index(drop=True)
        
        dates_by_id = emp_facts_by_id['date'].tolist()
        is_chronological = all(dates_by_id[i] >= dates_by_id[i-1] for i in range(1, len(dates_by_id)))
        
        print(f"\n📊 Employee Facts:")
        print(f"  Total records: {len(employee_facts_df)}")
        print(f"  Date range: {employee_facts_df['date'].min()} to {employee_facts_df['date'].max()}")
        print(f"  Chronological order: {'✅ YES' if is_chronological else '❌ NO'}")
        results['employee_facts'] = is_chronological
    
    # Summary
    print(f"\n📈 SUMMARY:")
    print("=" * 40)
    all_correct = True
    for entity, is_correct in results.items():
        status = "✅ PASS" if is_correct else "❌ FAIL"
        print(f"  {entity.replace('_', ' ').title()}: {status}")
        if not is_correct:
            all_correct = False
    
    print(f"\n🎯 OVERALL RESULT: {'✅ ALL FACT TABLES HAVE CORRECT CHRONOLOGICAL ID ORDERING!' if all_correct else '❌ SOME FACT TABLES HAVE INCORRECT ID ORDERING'}")
    
    # Show sample chronology for each fact table
    print(f"\n🔍 SAMPLE CHRONOLOGY:")
    print("=" * 40)
    
    # Sales
    if 'fact_sales' in pipeline.data_cache:
        sales_df = pipeline.data_cache['fact_sales'].sort_values('sale_id').head(3)
        print(f"\n💰 Sales (by ID):")
        for _, sale in sales_df.iterrows():
            sale_num = sale['sale_id'].replace('SAL000000000000', '')[:2]
            print(f"  Sale {sale_num}: {sale['date']} - ₱{sale['total_amount']:,.0f}")
    
    # Inventory
    if 'fact_inventory' in pipeline.data_cache:
        inv_df = pipeline.data_cache['fact_inventory'].sort_values('inventory_id').head(3)
        print(f"\n📦 Inventory (by ID):")
        for _, inv in inv_df.iterrows():
            print(f"  Inventory {inv['inventory_id']}: {inv['date']} - {inv['closing_stock']} units")
    
    # Operating Costs
    if 'fact_operating_costs' in pipeline.data_cache:
        cost_df = pipeline.data_cache['fact_operating_costs'].sort_values('cost_id').head(3)
        print(f"\n💸 Operating Costs (by ID):")
        for _, cost in cost_df.iterrows():
            print(f"  Cost {cost['cost_id']}: {cost['date']} - ₱{cost['amount']:,.0f} ({cost['cost_category']})")
    
    # Marketing Costs
    if 'fact_marketing_costs' in pipeline.data_cache:
        market_df = pipeline.data_cache['fact_marketing_costs'].sort_values('marketing_cost_id').head(3)
        print(f"\n📢 Marketing Costs (by ID):")
        for _, market in market_df.iterrows():
            market_num = market['marketing_cost_id'].replace('MAR000000000000', '')[:2]
            print(f"  Marketing {market_num}: {market['date']} - ₱{market['amount']:,.0f} ({market['cost_category']})")
    
    print("\n" + "=" * 60)
    print("✅ Fact tables chronological ID ordering test completed!")

if __name__ == '__main__':
    main()
