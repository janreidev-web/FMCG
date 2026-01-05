#!/usr/bin/env python3
"""
Debug script to investigate the PyArrow conversion issue
"""

import pandas as pd
import sys
import os
import numpy as np

# Add FMCG directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'FMCG'))

from generators.dimensional import generate_fact_employees, generate_dim_employees_normalized, generate_dim_jobs, generate_dim_departments
from helpers import ensure_bigquery_dtypes

def debug_employee_facts():
    """Debug the employee facts generation and conversion"""
    print("🔍 Debugging employee facts generation...")
    
    # Generate sample data
    print("1. Generating sample employees...")
    departments = generate_dim_departments()
    jobs = generate_dim_jobs(departments)
    employees = generate_dim_employees_normalized(
        locations=[],  # Empty for testing
        departments=departments,
        jobs=jobs,
        banks=[],
        insurance=[],
        num_employees=5
    )
    
    print("2. Generating employee facts...")
    employee_facts = generate_fact_employees(employees, jobs)
    
    print(f"3. Generated {len(employee_facts)} employee fact records")
    
    # Convert to DataFrame
    df = pd.DataFrame(employee_facts)
    print(f"4. DataFrame shape: {df.shape}")
    print(f"5. DataFrame columns: {list(df.columns)}")
    
    # Check employee_fact_id specifically
    print(f"6. employee_fact_id column info:")
    print(f"   - Dtype: {df['employee_fact_id'].dtype}")
    print(f"   - Values: {df['employee_fact_id'].tolist()}")
    print(f"   - Type of first value: {type(df['employee_fact_id'].iloc[0])}")
    print(f"   - All values are integers: {all(isinstance(x, (int, np.integer)) for x in df['employee_fact_id'])}")
    
    # Check for any mixed types or NaN values
    print(f"7. Checking for issues:")
    print(f"   - Any NaN values: {df['employee_fact_id'].isna().any()}")
    print(f"   - Any None values: {df['employee_fact_id'].isnull().any()}")
    print(f"   - Unique types in column: {set(type(x).__name__ for x in df['employee_fact_id'])}")
    
    # Apply our conversion
    print("8. Applying BigQuery data type conversion...")
    try:
        converted_df = ensure_bigquery_dtypes(df, "fact_employees")
        print(f"   ✓ Conversion successful")
        print(f"   - Converted dtype: {converted_df['employee_fact_id'].dtype}")
        print(f"   - Converted values: {converted_df['employee_fact_id'].tolist()}")
        
        # Try to create a simple PyArrow table to test
        print("9. Testing PyArrow conversion...")
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            # Try to convert to PyArrow Table
            table = pa.Table.from_pandas(converted_df)
            print(f"   ✓ PyArrow conversion successful")
            print(f"   - Table schema: {table.schema}")
            
            return True
            
        except Exception as arrow_error:
            print(f"   ✗ PyArrow conversion failed: {arrow_error}")
            return False
            
    except Exception as e:
        print(f"   ✗ Conversion failed: {e}")
        return False

def debug_simple_case():
    """Debug with a simple case"""
    print("\n🔍 Debugging simple case...")
    
    # Create a simple DataFrame with just the problematic column
    simple_data = pd.DataFrame({
        'employee_fact_id': [1, 2, 3, 4, 5],
        'employee_id': ['EMP000001', 'EMP000002', 'EMP000003', 'EMP000004', 'EMP000005'],
        'effective_date': ['2026-01-05', '2026-01-05', '2026-01-05', '2026-01-05', '2026-01-05'],
        'performance_rating': [3, 4, 5, 4, 3]
    })
    
    print(f"Simple DataFrame:")
    print(f"  - employee_fact_id dtype: {simple_data['employee_fact_id'].dtype}")
    print(f"  - employee_fact_id values: {simple_data['employee_fact_id'].tolist()}")
    
    try:
        converted_df = ensure_bigquery_dtypes(simple_data, "fact_employees")
        print(f"  ✓ Simple conversion successful")
        print(f"  - Converted dtype: {converted_df['employee_fact_id'].dtype}")
        
        # Test PyArrow
        try:
            import pyarrow as pa
            table = pa.Table.from_pandas(converted_df)
            print(f"  ✓ Simple PyArrow conversion successful")
            return True
        except Exception as arrow_error:
            print(f"  ✗ Simple PyArrow conversion failed: {arrow_error}")
            return False
            
    except Exception as e:
        print(f"  ✗ Simple conversion failed: {e}")
        return False

if __name__ == "__main__":
    print("🐛 Starting PyArrow conversion debug...")
    
    simple_success = debug_simple_case()
    full_success = debug_employee_facts()
    
    print(f"\n{'='*60}")
    if simple_success and full_success:
        print("🎉 All debug tests passed!")
    else:
        print("❌ Some debug tests failed!")
        if not simple_success:
            print("  - Simple case failed (basic issue)")
        if not full_success:
            print("  - Full case failed (data generation issue)")
    print(f"{'='*60}")
