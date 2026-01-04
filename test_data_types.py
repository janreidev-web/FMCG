#!/usr/bin/env python3
"""
Test script to check for data type mismatches before running the full workflow
"""

import os
import sys
import pandas as pd
from datetime import date

# Add FMCG to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'FMCG'))

try:
    from FMCG.generators.dimensional import (
        generate_dim_locations, generate_dim_departments, generate_dim_jobs,
        generate_dim_banks, generate_dim_insurance, generate_dim_categories,
        generate_dim_brands, generate_dim_subcategories, generate_dim_products,
        generate_dim_employees_normalized, generate_dim_retailers_normalized,
        generate_dim_campaigns, generate_dim_dates
    )
    print("✅ Successfully imported all generators")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

def test_data_types():
    """Test all generators for data type consistency"""
    print("\n🔍 Testing Data Types for All Generators...")
    print("=" * 60)
    
    # Test locations
    print("\n📍 Testing DIM_LOCATIONS...")
    locations = generate_dim_locations(num_locations=5)
    locations_df = pd.DataFrame(locations)
    print(f"   Columns: {list(locations_df.columns)}")
    print(f"   Data types: {dict(locations_df.dtypes)}")
    print(f"   Sample data: {locations_df.iloc[0].to_dict()}")
    
    # Test departments
    print("\n🏢 Testing DIM_DEPARTMENTS...")
    departments = generate_dim_departments()
    departments_df = pd.DataFrame(departments)
    print(f"   Columns: {list(departments_df.columns)}")
    print(f"   Data types: {dict(departments_df.dtypes)}")
    
    # Test jobs
    print("\n💼 Testing DIM_JOBS...")
    jobs = generate_dim_jobs(departments)
    jobs_df = pd.DataFrame(jobs)
    print(f"   Columns: {list(jobs_df.columns)}")
    print(f"   Data types: {dict(jobs_df.dtypes)}")
    
    # Test banks
    print("\n🏦 Testing DIM_BANKS...")
    banks = generate_dim_banks()
    banks_df = pd.DataFrame(banks)
    print(f"   Columns: {list(banks_df.columns)}")
    print(f"   Data types: {dict(banks_df.dtypes)}")
    
    # Test insurance
    print("\n🏥 Testing DIM_INSURANCE...")
    insurance = generate_dim_insurance()
    insurance_df = pd.DataFrame(insurance)
    print(f"   Columns: {list(insurance_df.columns)}")
    print(f"   Data types: {dict(insurance_df.dtypes)}")
    
    # Test categories
    print("\n📂 Testing DIM_CATEGORIES...")
    categories = generate_dim_categories()
    categories_df = pd.DataFrame(categories)
    print(f"   Columns: {list(categories_df.columns)}")
    print(f"   Data types: {dict(categories_df.dtypes)}")
    
    # Test brands
    print("\n🏷️ Testing DIM_BRANDS...")
    brands = generate_dim_brands()
    brands_df = pd.DataFrame(brands)
    print(f"   Columns: {list(brands_df.columns)}")
    print(f"   Data types: {dict(brands_df.dtypes)}")
    
    # Test subcategories
    print("\n📋 Testing DIM_SUBCATEGORIES...")
    subcategories = generate_dim_subcategories()
    subcategories_df = pd.DataFrame(subcategories)
    print(f"   Columns: {list(subcategories_df.columns)}")
    print(f"   Data types: {dict(subcategories_df.dtypes)}")
    
    # Test products
    print("\n📦 Testing DIM_PRODUCTS...")
    products = generate_dim_products(
        categories=categories, brands=brands, subcategories=subcategories, num_products=5
    )
    products_df = pd.DataFrame(products)
    print(f"   Columns: {list(products_df.columns)}")
    print(f"   Data types: {dict(products_df.dtypes)}")
    
    # Test employees (most complex)
    print("\n👥 Testing DIM_EMPLOYEES...")
    employees = generate_dim_employees_normalized(
        locations=locations[:5], departments=departments, jobs=jobs[:5],
        banks=banks, insurance=insurance, num_employees=5
    )
    employees_df = pd.DataFrame(employees)
    print(f"   Columns: {list(employees_df.columns)}")
    print(f"   Data types: {dict(employees_df.dtypes)}")
    
    # Apply the same data type fixes as main.py
    print("\n🔧 Applying data type fixes...")
    
    # Handle date fields
    date_columns = ['hire_date', 'birth_date', 'termination_date']
    for col in date_columns:
        if col in employees_df.columns:
            employees_df[col] = pd.to_datetime(employees_df[col], errors='coerce')
            print(f"   Converted {col}: {employees_df[col].dtype}")
    
    # Handle termination_date separately
    if 'termination_date' in employees_df.columns:
        employees_df['termination_date'] = pd.to_datetime(employees_df['termination_date'], errors='coerce')
        employees_df['termination_date'] = employees_df['termination_date'].dt.strftime('%Y-%m-%d')
        employees_df['termination_date'] = employees_df['termination_date'].replace('NaT', None)
        print(f"   termination_date converted to string")
    
    # Add tenure
    if 'hire_date' in employees_df.columns:
        today = pd.Timestamp.now().normalize()
        employees_df['tenure_years'] = ((today - employees_df['hire_date']).dt.days / 365.25).round(2)
        print(f"   Added tenure_years: {employees_df['tenure_years'].dtype}")
    
    print(f"   Final data types: {dict(employees_df.dtypes)}")
    
    # Test retailers
    print("\n🏪 Testing DIM_RETAILERS...")
    retailers = generate_dim_retailers_normalized(num_retailers=3, locations=locations[:3])
    retailers_df = pd.DataFrame(retailers)
    print(f"   Columns: {list(retailers_df.columns)}")
    print(f"   Data types: {dict(retailers_df.dtypes)}")
    
    # Test campaigns
    print("\n📢 Testing DIM_CAMPAIGNS...")
    campaigns = generate_dim_campaigns()
    campaigns_df = pd.DataFrame(campaigns)
    print(f"   Columns: {list(campaigns_df.columns)}")
    print(f"   Data types: {dict(campaigns_df.dtypes)}")
    
    # Test dates (problematic one)
    print("\n📅 Testing DIM_DATES...")
    dates = generate_dim_dates()
    dates_df = pd.DataFrame(dates)
    print(f"   Columns: {list(dates_df.columns)}")
    print(f"   Original data types: {dict(dates_df.dtypes)}")
    
    # Apply date_id fix
    if 'date_id' in dates_df.columns:
        dates_df['date_id'] = dates_df['date_id'].astype(str)
        print(f"   Fixed date_id: {dates_df['date_id'].dtype}")
        print(f"   Sample date_id: {dates_df['date_id'].iloc[0]}")
    
    print(f"   Final data types: {dict(dates_df.dtypes)}")
    
    print("\n✅ Data type testing completed!")
    print("=" * 60)
    
    # Check for potential PyArrow issues
    print("\n⚠️  Checking for potential PyArrow issues...")
    
    issues = []
    
    # Check employees for mixed types
    for col in employees_df.columns:
        if employees_df[col].dtype == 'object':
            unique_types = set(type(x).__name__ for x in employees_df[col].dropna().head(10))
            if len(unique_types) > 1:
                issues.append(f"Employees {col}: mixed types {unique_types}")
    
    # Check dates for int date_id
    if 'date_id' in dates_df.columns and dates_df['date_id'].dtype in ['int64', 'int32']:
        issues.append("Dates date_id: still integer type")
    
    if issues:
        print("❌ Found potential issues:")
        for issue in issues:
            print(f"   - {issue}")
    else:
        print("✅ No obvious PyArrow issues detected!")
    
    return len(issues) == 0

if __name__ == "__main__":
    success = test_data_types()
    sys.exit(0 if success else 1)
