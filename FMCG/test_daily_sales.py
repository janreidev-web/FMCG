#!/usr/bin/env python3
"""
Test script to verify daily sales date logic works correctly
"""

import os
import sys
import pandas as pd
from datetime import date, timedelta

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_date_conversion():
    """Test the date conversion logic"""
    print("=== Testing Date Conversion Logic ===")
    
    # Test case 1: datetime.date object (what BigQuery returns)
    test_date1 = date(2026, 1, 4)
    print(f"Test 1 - Input: {test_date1} (type: {type(test_date1)})")
    
    if isinstance(test_date1, date):
        result1 = test_date1
        print(f"  Result: {result1} (type: {type(result1)})")
    elif hasattr(test_date1, 'date'):
        result1 = test_date1.date()
        print(f"  Result: {result1} (type: {type(result1)})")
    else:
        result1 = pd.to_datetime(test_date1).date()
        print(f"  Result: {result1} (type: {type(result1)})")
    
    # Test case 2: string
    test_date2 = "2026-01-04"
    print(f"\nTest 2 - Input: {test_date2} (type: {type(test_date2)})")
    
    if isinstance(test_date2, date):
        result2 = test_date2
        print(f"  Result: {result2} (type: {type(result2)})")
    elif hasattr(test_date2, 'date'):
        result2 = test_date2.date()
        print(f"  Result: {result2} (type: {type(result2)})")
    else:
        result2 = pd.to_datetime(test_date2).date()
        print(f"  Result: {result2} (type: {type(result2)})")
    
    # Test case 3: pandas Timestamp
    test_date3 = pd.Timestamp('2026-01-04')
    print(f"\nTest 3 - Input: {test_date3} (type: {type(test_date3)})")
    
    if isinstance(test_date3, date):
        result3 = test_date3
        print(f"  Result: {result3} (type: {type(result3)})")
    elif hasattr(test_date3, 'date'):
        result3 = test_date3.date()
        print(f"  Result: {result3} (type: {type(result3)})")
    else:
        result3 = pd.to_datetime(test_date3).date()
        print(f"  Result: {result3} (type: {type(result3)})")
    
    print("\n=== Date Range Calculation Test ===")
    
    # Test date range logic
    yesterday = date.today() - timedelta(days=1)
    latest_date = date(2026, 1, 4)  # Simulate latest date in database (same as yesterday)
    
    print(f"Today: {date.today()}")
    print(f"Yesterday: {yesterday}")
    print(f"Latest in DB: {latest_date}")
    
    start_date = latest_date + timedelta(days=1)
    print(f"Start date: {start_date}")
    
    if start_date > yesterday:
        print("✅ Up to date - no new data needed")
    else:
        print(f"📊 Need to generate from {start_date} to {yesterday}")
    
    # Test case where latest is day before yesterday
    print("\n--- Test with latest = day before yesterday ---")
    latest_date = date(2026, 1, 3)
    print(f"Latest in DB: {latest_date}")
    
    start_date = latest_date + timedelta(days=1)
    print(f"Start date: {start_date}")
    
    if start_date > yesterday:
        print("✅ Up to date - no new data needed")
    else:
        print(f"📊 Need to generate from {start_date} to {yesterday}")
    
    print("\n=== Test Complete ===")

def test_environment_variables():
    """Test environment variable handling"""
    print("=== Testing Environment Variables ===")
    
    project_id = os.environ.get("GCP_PROJECT_ID", "fmcg-data-simulator")
    dataset = os.environ.get("BQ_DATASET", "fmcg_analytics")
    
    print(f"GCP_PROJECT_ID: {project_id}")
    print(f"BQ_DATASET: {dataset}")
    
    fact_sales_table = f"{project_id}.{dataset}.fact_sales"
    print(f"FACT_SALES_TABLE: {fact_sales_table}")
    
    print("=== Environment Variables Test Complete ===")

if __name__ == "__main__":
    test_date_conversion()
    test_environment_variables()
