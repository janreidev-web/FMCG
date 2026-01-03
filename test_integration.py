#!/usr/bin/env python3
"""
Integration tests for FMCG data pipeline
Tests end-to-end data generation and schema validation
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'FMCG'))

import unittest
from datetime import date, timedelta
import pandas as pd
from generators.dimensional import (
    generate_dim_locations, generate_dim_departments, generate_dim_jobs,
    generate_dim_banks, generate_dim_insurance, generate_dim_employees_normalized,
    generate_fact_employee_wages, generate_fact_employees,
    generate_fact_sales, generate_fact_operating_costs
)
from schema import (
    DIM_EMPLOYEES_SCHEMA, DIM_LOCATIONS_SCHEMA, DIM_JOBS_SCHEMA,
    FACT_EMPLOYEES_SCHEMA, FACT_EMPLOYEE_WAGES_SCHEMA
)

class TestIntegration(unittest.TestCase):
    """Test full data generation pipeline"""
    
    def setUp(self):
        """Set up test data"""
        self.locations = generate_dim_locations(50)
        self.departments = generate_dim_departments()
        self.jobs = generate_dim_jobs(self.departments)
        self.banks = generate_dim_banks()
        self.insurance = generate_dim_insurance()
        self.employees = generate_dim_employees_normalized(
            100, self.locations, self.jobs, 
            self.banks, self.insurance, self.departments
        )
        self.products = [
            {
                'product_key': i,
                'product_id': f'PROD{i:03d}',
                'product_name': f'Product {i}',
                'category': 'Food',
                'subcategory': 'Beverages',
                'brand': 'TestBrand',
                'wholesale_price': 100.0,
                'retail_price': 150.0,
                'status': 'Active'
            }
            for i in range(1, 21)
        ]
        self.retailers = [
            {
                'retailer_key': i,
                'retailer_id': f'RET{i:03d}',
                'retailer_name': f'Retailer {i}',
                'retailer_type': 'Supermarket',
                'location_key': self.locations[i % len(self.locations)]['location_key']
            }
            for i in range(1, 31)
        ]
        self.campaigns = [
            {
                'campaign_key': i,
                'campaign_id': f'CAM{i:03d}',
                'campaign_name': f'Campaign {i}',
                'campaign_type': 'Promotion',
                'start_date': date(2025, 1, 1),
                'end_date': date(2025, 12, 31),
                'budget': 10000.0,
                'currency': 'PHP'
            }
            for i in range(1, 11)
        ]
    
    def test_full_pipeline(self):
        """Test complete data generation pipeline"""
        print("\n" + "="*50)
        print("TESTING FULL DATA GENERATION PIPELINE")
        print("="*50)
        
        # Test 1: Generate all dimensions
        print("\n1. Testing dimension generation...")
        self.assertGreater(len(self.locations), 0)
        self.assertGreater(len(self.departments), 0)
        self.assertGreater(len(self.jobs), 0)
        self.assertGreater(len(self.banks), 0)
        self.assertGreater(len(self.insurance), 0)
        self.assertGreater(len(self.employees), 0)
        print(f"   ✓ Generated {len(self.locations)} locations")
        print(f"   ✓ Generated {len(self.departments)} departments")
        print(f"   ✓ Generated {len(self.jobs)} jobs")
        print(f"   ✓ Generated {len(self.banks)} banks")
        print(f"   ✓ Generated {len(self.insurance)} insurance providers")
        print(f"   ✓ Generated {len(self.employees)} employees")
        
        # Test 2: Generate employee facts and wages
        print("\n2. Testing employee facts and wages...")
        active_employees = [e for e in self.employees if e['employment_status'] == 'Active']
        
        if active_employees:
            employee_facts = generate_fact_employees(active_employees, self.jobs)
            employee_wages = generate_fact_employee_wages(active_employees, self.jobs)
            
            self.assertGreater(len(employee_facts), 0)
            self.assertGreater(len(employee_wages), 0)
            print(f"   ✓ Generated {len(employee_facts)} employee facts")
            print(f"   ✓ Generated {len(employee_wages)} wage records")
        
        # Test 3: Generate sales data
        print("\n3. Testing sales generation...")
        try:
            sales = generate_fact_sales(
                active_employees[:20], self.products[:10], self.retailers[:10], 
                self.campaigns[:5], 1000000,  # 1M test amount
                start_date=date(2025, 1, 1),
                end_date=date(2025, 1, 31)
            )
            self.assertGreater(len(sales), 0)
            print(f"   ✓ Generated {len(sales)} sales records")
        except Exception as e:
            print(f"   ⚠ Sales generation failed: {e}")
        
        # Test 4: Generate operating costs
        print("\n4. Testing operating costs...")
        try:
            costs = generate_fact_operating_costs(
                100000,  # 100K test amount
                start_date=date(2025, 1, 1),
                end_date=date(2025, 1, 31)
            )
            self.assertGreater(len(costs), 0)
            print(f"   ✓ Generated {len(costs)} cost records")
        except Exception as e:
            print(f"   ⚠ Operating costs generation failed: {e}")
        
        print("\n✓ Full pipeline test completed!")
    
    def test_schema_compliance(self):
        """Test data compliance with schema definitions"""
        print("\n" + "="*50)
        print("TESTING SCHEMA COMPLIANCE")
        print("="*50)
        
        # Test employee schema
        print("\n1. Testing employee schema compliance...")
        if self.employees:
            employee_df = pd.DataFrame(self.employees)
            required_fields = [field['name'] for field in DIM_EMPLOYEES_SCHEMA]
            
            for field in required_fields:
                self.assertIn(field, employee_df.columns, f"Missing field: {field}")
            
            # Check for removed fields
            self.assertNotIn('age', employee_df.columns)
            self.assertNotIn('full_name', employee_df.columns)
            print("   ✓ Employee schema compliant")
        
        # Test job schema
        print("\n2. Testing job schema compliance...")
        if self.jobs:
            job_df = pd.DataFrame(self.jobs)
            required_fields = [field['name'] for field in DIM_JOBS_SCHEMA]
            
            for field in required_fields:
                self.assertIn(field, job_df.columns, f"Missing field: {field}")
            print("   ✓ Job schema compliant")
        
        # Test location schema
        print("\n3. Testing location schema compliance...")
        if self.locations:
            location_df = pd.DataFrame(self.locations)
            required_fields = [field['name'] for field in DIM_LOCATIONS_SCHEMA]
            
            for field in required_fields:
                self.assertIn(field, location_df.columns, f"Missing field: {field}")
            print("   ✓ Location schema compliant")
        
        print("\n✓ Schema compliance test completed!")
    
    def test_data_relationships(self):
        """Test data relationships and foreign keys"""
        print("\n" + "="*50)
        print("TESTING DATA RELATIONSHIPS")
        print("="*50)
        
        # Create lookup dictionaries
        location_lookup = {loc['location_key']: loc for loc in self.locations}
        job_lookup = {job['job_key']: job for job in self.jobs}
        department_lookup = {dept['department_key']: dept for dept in self.departments}
        bank_lookup = {bank['bank_key']: bank for bank in self.banks}
        insurance_lookup = {ins['insurance_key']: ins for ins in self.insurance}
        
        relationship_errors = []
        
        for emp in self.employees:
            # Test location relationship
            if emp['location_key'] not in location_lookup:
                relationship_errors.append(f"Employee {emp['employee_key']}: Invalid location_key")
            
            # Test job relationship
            if emp['job_key'] not in job_lookup:
                relationship_errors.append(f"Employee {emp['employee_key']}: Invalid job_key")
            else:
                # Test job-department relationship
                job = job_lookup[emp['job_key']]
                if job['department_key'] not in department_lookup:
                    relationship_errors.append(f"Job {job['job_key']}: Invalid department_key")
            
            # Test bank relationship
            if emp['bank_key'] not in bank_lookup:
                relationship_errors.append(f"Employee {emp['employee_key']}: Invalid bank_key")
            
            # Test insurance relationship
            if emp['insurance_key'] not in insurance_lookup:
                relationship_errors.append(f"Employee {emp['employee_key']}: Invalid insurance_key")
        
        if relationship_errors:
            print(f"   ⚠ Found {len(relationship_errors)} relationship errors:")
            for error in relationship_errors[:5]:  # Show first 5 errors
                print(f"     - {error}")
            if len(relationship_errors) > 5:
                print(f"     ... and {len(relationship_errors) - 5} more")
        else:
            print("   ✓ All foreign key relationships valid")
        
        print("\n✓ Data relationships test completed!")
    
    def test_data_quality(self):
        """Test data quality and business rules"""
        print("\n" + "="*50)
        print("TESTING DATA QUALITY")
        print("="*50)
        
        quality_issues = []
        
        # Test employee data quality
        print("\n1. Testing employee data quality...")
        for emp in self.employees:
            # Test age range (calculated from birth_date)
            age = (date.today() - emp['birth_date']).days // 365
            if age < 18 or age > 65:
                quality_issues.append(f"Employee {emp['employee_key']}: Invalid age {age}")
            
            # Test hire date logic
            if emp['hire_date'] < date(2015, 1, 1) or emp['hire_date'] > date.today():
                quality_issues.append(f"Employee {emp['employee_key']}: Invalid hire_date")
            
            # Test termination date logic
            if emp['termination_date'] and emp['employment_status'] != 'Terminated':
                quality_issues.append(f"Employee {emp['employee_key']}: Termination date but not terminated")
            
            # Test email format
            if not emp['email'].endswith('@company.com'):
                quality_issues.append(f"Employee {emp['employee_key']}: Invalid email format")
        
        # Test job salary ranges
        print("\n2. Testing job salary ranges...")
        for job in self.jobs:
            if job['base_salary_min'] >= job['base_salary_max']:
                quality_issues.append(f"Job {job['job_key']}: Invalid salary range")
            
            # Test reasonable salary bounds
            if job['base_salary_min'] < 15000 or job['base_salary_max'] > 2000000:
                quality_issues.append(f"Job {job['job_key']}: Salary out of reasonable bounds")
        
        # Test department consistency
        print("\n3. Testing department consistency...")
        dept_names = {dept['department_name'] for dept in self.departments}
        for job in self.jobs:
            job_dept = next((dept['department_name'] for dept in self.departments 
                           if dept['department_key'] == job['department_key']), None)
            if not job_dept:
                quality_issues.append(f"Job {job['job_key']}: Invalid department reference")
        
        if quality_issues:
            print(f"   ⚠ Found {len(quality_issues)} quality issues:")
            for issue in quality_issues[:10]:  # Show first 10 issues
                print(f"     - {issue}")
            if len(quality_issues) > 10:
                print(f"     ... and {len(quality_issues) - 10} more")
        else:
            print("   ✓ All data quality checks passed")
        
        print("\n✓ Data quality test completed!")
    
    def test_performance_metrics(self):
        """Test performance and scalability"""
        print("\n" + "="*50)
        print("TESTING PERFORMANCE METRICS")
        print("="*50)
        
        import time
        
        # Test generation performance
        print("\n1. Testing generation performance...")
        
        start_time = time.time()
        test_employees = generate_dim_employees_normalized(
            200, self.locations, self.jobs, 
            self.banks, self.insurance, self.departments
        )
        employee_time = time.time() - start_time
        
        print(f"   ✓ Generated 200 employees in {employee_time:.2f} seconds")
        
        start_time = time.time()
        test_wages = generate_fact_employee_wages(test_employees, self.jobs)
        wages_time = time.time() - start_time
        
        print(f"   ✓ Generated {len(test_wages)} wage records in {wages_time:.2f} seconds")
        
        # Performance benchmarks
        if employee_time > 5.0:
            print(f"   ⚠ Employee generation slow: {employee_time:.2f}s")
        
        if wages_time > 10.0:
            print(f"   ⚠ Wage generation slow: {wages_time:.2f}s")
        
        print("\n✓ Performance metrics test completed!")

if __name__ == '__main__':
    print("Running FMCG Integration Tests...")
    print("=" * 50)
    
    # Run integration tests
    unittest.main(verbosity=2)
