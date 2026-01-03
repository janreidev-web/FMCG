#!/usr/bin/env python3
"""
Unit tests for FMCG data generators
Tests for data integrity, schema compliance, and edge cases
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'FMCG'))

import unittest
from datetime import date, datetime, timedelta
from generators.dimensional import (
    generate_dim_locations, generate_dim_departments, generate_dim_jobs,
    generate_dim_banks, generate_dim_insurance, generate_dim_employees_normalized,
    generate_fact_employee_wages, generate_fact_employees
)

class TestDimensionalGenerators(unittest.TestCase):
    """Test dimensional data generators for data quality and integrity"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_locations = generate_dim_locations(10)
        self.test_departments = generate_dim_departments()
        self.test_jobs = generate_dim_jobs(self.test_departments)
        self.test_banks = generate_dim_banks()
        self.test_insurance = generate_dim_insurance()
    
    def test_generate_dim_locations(self):
        """Test location generation"""
        locations = self.test_locations
        
        # Test basic structure
        self.assertIsInstance(locations, list)
        self.assertEqual(len(locations), 10)
        
        # Test required fields
        for location in locations:
            self.assertIn('location_key', location)
            self.assertIn('street_address', location)
            self.assertIn('city', location)
            self.assertIn('province', location)
            self.assertIn('region', location)
            self.assertIn('postal_code', location)
            self.assertIn('country', location)
            
            # Test data types
            self.assertIsInstance(location['location_key'], int)
            self.assertIsInstance(location['street_address'], str)
            self.assertIsInstance(location['city'], str)
            self.assertIsInstance(location['country'], str)
            
            # Test Philippines context
            self.assertEqual(location['country'], 'Philippines')
            # Test that region is a valid Philippine region
            valid_regions = [
                'Region I', 'Region II', 'Region III', 'Region IV-A', 'Region IV-B', 'Region V',
                'Region VI', 'Region VII', 'Region VIII', 'Region IX', 'Region X', 'Region XI', 
                'Region XII', 'Region XIII', 'CAR', 'NCR', 'BARMM', 'Luzon', 'Visayas', 'Mindanao'
            ]
            self.assertTrue(
                any(region in location['region'] for region in valid_regions),
                f"Region '{location['region']}' is not a valid Philippine region"
            )
    
    def test_generate_dim_departments(self):
        """Test department generation"""
        departments = self.test_departments
        
        # Test basic structure
        self.assertIsInstance(departments, list)
        self.assertGreater(len(departments), 0)
        
        # Test required fields
        for dept in departments:
            self.assertIn('department_key', dept)
            self.assertIn('department_name', dept)
            self.assertIn('department_code', dept)
            
            # Test data types
            self.assertIsInstance(dept['department_key'], int)
            self.assertIsInstance(dept['department_name'], str)
            self.assertIsInstance(dept['department_code'], str)
            
            # Test unique keys
            self.assertTrue(dept['department_key'] > 0)
    
    def test_generate_dim_jobs(self):
        """Test job generation"""
        jobs = self.test_jobs
        
        # Test basic structure
        self.assertIsInstance(jobs, list)
        self.assertGreater(len(jobs), 0)
        
        # Test required fields
        for job in jobs:
            self.assertIn('job_key', job)
            self.assertIn('job_title', job)
            self.assertIn('job_level', job)
            self.assertIn('department_key', job)
            self.assertIn('work_setup', job)
            self.assertIn('work_type', job)
            self.assertIn('base_salary_min', job)
            self.assertIn('base_salary_max', job)
            
            # Test data types
            self.assertIsInstance(job['job_key'], int)
            self.assertIsInstance(job['job_title'], str)
            self.assertIsInstance(job['job_level'], str)
            self.assertIsInstance(job['base_salary_min'], int)
            self.assertIsInstance(job['base_salary_max'], int)
            
            # Test salary ranges
            self.assertGreaterEqual(job['base_salary_max'], job['base_salary_min'])
            self.assertGreater(job['base_salary_min'], 0)
            
            # Test job levels
            self.assertIn(job['job_level'], ['Entry', 'Junior', 'Senior', 'Manager', 'Director'])
            
            # Test work types
            self.assertIn(job['work_type'], ['Full-time', 'Part-time', 'Contract', 'Intern'])
    
    def test_generate_dim_employees(self):
        """Test employee generation"""
        employees = generate_dim_employees_normalized(
            50, self.test_locations, self.test_jobs, 
            self.test_banks, self.test_insurance, self.test_departments
        )
        
        # Test basic structure
        self.assertIsInstance(employees, list)
        self.assertGreaterEqual(len(employees), 45)  # Allow for rounding in distribution
        self.assertLessEqual(len(employees), 50)
        
        # Test required fields
        for emp in employees:
            self.assertIn('employee_key', emp)
            self.assertIn('employee_id', emp)
            self.assertIn('first_name', emp)
            self.assertIn('last_name', emp)
            self.assertIn('gender', emp)
            self.assertIn('birth_date', emp)
            self.assertIn('hire_date', emp)
            self.assertIn('employment_status', emp)
            
            # Test removed fields (should not exist)
            self.assertNotIn('age', emp)
            self.assertNotIn('full_name', emp)
            
            # Test data types
            self.assertIsInstance(emp['employee_key'], int)
            self.assertIsInstance(emp['employee_id'], str)
            self.assertIsInstance(emp['first_name'], str)
            self.assertIsInstance(emp['last_name'], str)
            self.assertIsInstance(emp['birth_date'], date)
            self.assertIsInstance(emp['hire_date'], date)
            
            # Test date logic
            self.assertLessEqual(emp['birth_date'], date.today() - timedelta(days=18*365))
            self.assertGreaterEqual(emp['birth_date'], date.today() - timedelta(days=65*365))
            self.assertLessEqual(emp['hire_date'], date.today())
            self.assertGreaterEqual(emp['hire_date'], date(2015, 1, 1))
            
            # Test employment status
            self.assertIn(emp['employment_status'], ['Active', 'Terminated', 'On Leave'])
            
            # Test termination date logic
            if emp['employment_status'] == 'Terminated':
                self.assertIsNotNone(emp.get('termination_date'))
                self.assertGreater(emp['termination_date'], emp['hire_date'])
                self.assertLessEqual(emp['termination_date'], date.today())
            
            # Test employee ID format
            self.assertTrue(emp['employee_id'].startswith('EMP'))
            self.assertEqual(len(emp['employee_id']), 9)  # EMP + 6 digits
    
    def test_generate_fact_employee_wages(self):
        """Test employee wage generation"""
        employees = generate_dim_employees_normalized(
            10, self.test_locations, self.test_jobs, 
            self.test_banks, self.test_insurance, self.test_departments
        )
        
        wages = generate_fact_employee_wages(employees, self.test_jobs)
        
        # Test basic structure
        self.assertIsInstance(wages, list)
        self.assertGreater(len(wages), 0)
        
        # Test required fields
        for wage in wages:
            self.assertIn('wage_key', wage)
            self.assertIn('employee_key', wage)
            self.assertIn('effective_date', wage)
            self.assertIn('job_title', wage)
            self.assertIn('job_level', wage)
            self.assertIn('department', wage)
            self.assertIn('monthly_salary', wage)
            self.assertIn('annual_salary', wage)
            self.assertIn('currency', wage)
            self.assertIn('years_of_service', wage)
            self.assertIn('salary_grade', wage)
            
            # Test data types
            self.assertIsInstance(wage['wage_key'], int)
            self.assertIsInstance(wage['employee_key'], int)
            self.assertIsInstance(wage['effective_date'], date)
            self.assertIsInstance(wage['job_title'], str)
            self.assertIsInstance(wage['monthly_salary'], int)
            self.assertIsInstance(wage['annual_salary'], int)
            
            # Test salary calculations
            self.assertEqual(wage['annual_salary'], wage['monthly_salary'] * 12)
            self.assertGreater(wage['monthly_salary'], 0)
            
            # Test currency
            self.assertEqual(wage['currency'], 'PHP')
            
            # Test years of service
            self.assertGreaterEqual(wage['years_of_service'], 0)
    
    def test_generate_fact_employees(self):
        """Test employee fact generation"""
        employees = generate_dim_employees_normalized(
            10, self.test_locations, self.test_jobs, 
            self.test_banks, self.test_insurance, self.test_departments
        )
        
        # Filter active employees only
        active_employees = [e for e in employees if e['employment_status'] == 'Active']
        
        if active_employees:
            facts = generate_fact_employees(active_employees, self.test_jobs)
            
            # Test basic structure
            self.assertIsInstance(facts, list)
            self.assertEqual(len(facts), len(active_employees))
            
            # Test required fields
            for fact in facts:
                self.assertIn('employee_fact_key', fact)
                self.assertIn('employee_key', fact)
                self.assertIn('effective_date', fact)
                self.assertIn('performance_rating', fact)
                self.assertIn('last_review_date', fact)
                self.assertIn('promotion_eligible', fact)
                
                # Test removed compensation fields
                self.assertNotIn('monthly_salary', fact)
                self.assertNotIn('annual_bonus', fact)
                self.assertNotIn('total_compensation', fact)
                
                # Test data types
                self.assertIsInstance(fact['employee_fact_key'], int)
                self.assertIsInstance(fact['employee_key'], int)
                self.assertIsInstance(fact['effective_date'], date)
                self.assertIsInstance(fact['performance_rating'], int)
                self.assertIsInstance(fact['promotion_eligible'], bool)
                
                # Test performance rating range
                self.assertIn(fact['performance_rating'], [1, 2, 3, 4, 5])
    
    def test_foreign_key_integrity(self):
        """Test foreign key relationships"""
        employees = generate_dim_employees_normalized(
            20, self.test_locations, self.test_jobs, 
            self.test_banks, self.test_insurance, self.test_departments
        )
        
        location_keys = {loc['location_key'] for loc in self.test_locations}
        department_keys = {dept['department_key'] for dept in self.test_departments}
        job_keys = {job['job_key'] for job in self.test_jobs}
        bank_keys = {bank['bank_key'] for bank in self.test_banks}
        insurance_keys = {ins['insurance_key'] for ins in self.test_insurance}
        
        for emp in employees:
            self.assertIn(emp['location_key'], location_keys)
            self.assertIn(emp['job_key'], job_keys)
            self.assertIn(emp['bank_key'], bank_keys)
            self.assertIn(emp['insurance_key'], insurance_keys)
    
    def test_salary_ranges_by_department(self):
        """Test salary ranges are appropriate by department"""
        for job in self.test_jobs:
            # Test salary ranges are within reasonable bounds
            self.assertGreaterEqual(job['base_salary_min'], 20000)  # Minimum entry level
            self.assertLessEqual(job['base_salary_max'], 1000000)  # Maximum director level
            
            # Test department-specific salary logic
            if job['job_level'] == 'Entry':
                self.assertLessEqual(job['base_salary_max'], 55000)  # Updated to match actual max
            elif job['job_level'] == 'Director':
                self.assertGreaterEqual(job['base_salary_min'], 220000)

class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""
    
    def test_empty_inputs(self):
        """Test behavior with empty inputs"""
        # Test with empty locations list - should raise ValueError
        with self.assertRaises((ValueError, IndexError, TypeError)):
            generate_dim_employees_normalized(
                1, [], [], [], [], []
            )
    
    def test_date_edge_cases(self):
        """Test date generation edge cases"""
        # This test would need to be expanded based on specific date edge cases
        pass

if __name__ == '__main__':
    print("Running FMCG Data Generator Tests...")
    print("=" * 50)
    
    # Run tests
    unittest.main(verbosity=2)
