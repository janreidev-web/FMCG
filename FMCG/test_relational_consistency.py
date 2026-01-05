#!/usr/bin/env py
"""
Relational Consistency Test Suite for FMCG Data Simulator

This test file verifies:
1. Schema vs Data Generation Consistency
2. ID Pattern Standardization
3. Foreign Key Relationships
4. Data Type Compatibility
5. Enhanced Inventory Features

Usage:
    py test_relational_consistency.py
"""

import sys
import os
import pandas as pd
from datetime import datetime, date, timedelta
import random

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from generators.dimensional import (
        generate_dim_products, generate_dim_employees_normalized, generate_dim_locations,
        generate_dim_departments, generate_dim_jobs, generate_dim_banks, generate_dim_insurance,
        generate_fact_inventory, generate_dim_retailers_normalized, generate_dim_campaigns,
        generate_fact_sales, generate_daily_sales_with_delivery_updates,
        generate_fact_operating_costs, generate_fact_marketing_costs,
        generate_dim_categories, generate_dim_brands, generate_dim_subcategories,
        validate_relationships
    )
    from config import (
        INITIAL_EMPLOYEES, INITIAL_PRODUCTS, INITIAL_RETAILERS,
        NEW_EMPLOYEES_PER_MONTH, NEW_PRODUCTS_PER_MONTH, NEW_CAMPAIGNS_PER_QUARTER
    )
    from schema import (
        FACT_SALES_SCHEMA, FACT_INVENTORY_SCHEMA, FACT_MARKETING_COSTS_SCHEMA,
        FACT_OPERATING_COSTS_SCHEMA, DIM_PRODUCTS_SCHEMA, DIM_EMPLOYEES_SCHEMA,
        DIM_RETAILERS_SCHEMA, DIM_CAMPAIGNS_SCHEMA
    )
except ImportError as e:
    print(f"Import error: {e}")
    print("Please ensure you're running this from the FMCG directory")
    sys.exit(1)

class RelationalConsistencyTester:
    def __init__(self):
        self.test_results = []
        self.errors = []
        self.warnings = []
        
    def log_test(self, test_name, passed, message=""):
        """Log test result"""
        status = "✅ PASS" if passed else "❌ FAIL"
        self.test_results.append({
            'test': test_name,
            'status': status,
            'message': message,
            'timestamp': datetime.now()
        })
        print(f"{status}: {test_name}")
        if message:
            print(f"   {message}")
        if not passed:
            self.errors.append(f"{test_name}: {message}")
    
    def log_warning(self, warning):
        """Log warning"""
        self.warnings.append(warning)
        print(f"⚠️  WARNING: {warning}")
    
    def test_id_pattern_consistency(self):
        """Test that all IDs follow consistent patterns"""
        print("\n=== Testing ID Pattern Consistency ===")
        
        # Generate sample data
        locations = generate_dim_locations(5)
        departments = generate_dim_departments()
        jobs = generate_dim_jobs(departments)
        banks = generate_dim_banks()
        insurance = generate_dim_insurance()
        categories = generate_dim_categories()
        brands = generate_dim_brands()
        subcategories = generate_dim_subcategories()
        products = generate_dim_products(10, categories, brands, subcategories)
        retailers = generate_dim_retailers_normalized(10, locations)
        campaigns = generate_dim_campaigns()
        employees = generate_dim_employees_normalized(10, locations, jobs, banks, insurance)
        inventory = generate_fact_inventory(products[:5], locations[:3])
        sales = generate_fact_sales(employees[:3], products[:3], retailers[:3], campaigns[:3], target_amount=1000000, start_date=date.today(), end_date=date.today())
        marketing_costs = generate_fact_marketing_costs(campaigns[:3], target_amount=500000)
        
        # Expected ID patterns
        id_patterns = {
            'location_id': r'^LOC\d{6}$',
            'department_id': r'^DEPT\d{6}$',
            'job_id': r'^JOB\d{6}$',
            'bank_id': r'^BANK\d{6}$',
            'insurance_id': r'^INS\d{6}$',
            'category_id': r'^CAT\d{6}$',
            'brand_id': r'^BRD\d{6}$',
            'subcategory_id': r'^SUB\d{6}$',
            'product_id': r'^PROD\d{6}$',
            'retailer_id': r'^RET\d{6}$',
            'campaign_id': r'^CAM\d{6}$',
            'employee_id': r'^EMP\d{6}$',
            'sale_id': r'^SAL\d{6}$',
            'inventory_id': r'^INV\d{15}$',
            'marketing_cost_key': r'^\d+$',  # Integer
            'wage_key': r'^WAGE_[A-Z0-9]+_\d{8}_\d{3}$',  # WAGE_EMP000001_20260105_001
            'cost_key': r'^COST_\d{8}_[A-Z]{3}_\d{3}$'  # COST_20260105_REN_001
        }
        
        import re
        
        # Test each entity type
        test_data = [
            (locations, 'location_id'),
            (departments, 'department_id'),
            (jobs, 'job_id'),
            (banks, 'bank_id'),
            (insurance, 'insurance_id'),
            (categories, 'category_id'),
            (brands, 'brand_id'),
            (subcategories, 'subcategory_id'),
            (products, 'product_id'),
            (retailers, 'retailer_id'),
            (campaigns, 'campaign_id'),
            (employees, 'employee_id'),
            (sales, 'sale_id'),
            (inventory, 'inventory_id'),
            (marketing_costs, 'marketing_cost_key')
        ]
        
        for data_list, id_field in test_data:
            if not data_list:
                continue
                
            pattern = id_patterns.get(id_field)
            if not pattern:
                self.log_warning(f"No pattern defined for {id_field}")
                continue
                
            sample_id = data_list[0][id_field]
            matches = re.match(pattern, str(sample_id))
            
            self.log_test(
                f"ID Pattern - {id_field}",
                matches is not None,
                f"Expected: {pattern}, Got: {sample_id}"
            )
    
    def test_schema_vs_data_compatibility(self):
        """Test that generated data matches schema definitions"""
        print("\n=== Testing Schema vs Data Compatibility ===")
        
        # Generate sample data
        locations = generate_dim_locations(3)
        departments = generate_dim_departments()
        jobs = generate_dim_jobs(departments)
        banks = generate_dim_banks()
        insurance = generate_dim_insurance()
        categories = generate_dim_categories()
        brands = generate_dim_brands()
        subcategories = generate_dim_subcategories()
        products = generate_dim_products(5, categories, brands, subcategories)
        retailers = generate_dim_retailers_normalized(5, locations)
        campaigns = generate_dim_campaigns()
        employees = generate_dim_employees_normalized(5, locations, jobs, banks, insurance)
        inventory = generate_fact_inventory(products[:3], locations[:2])
        sales = generate_fact_sales(employees[:2], products[:2], retailers[:2], campaigns[:2], target_amount=500000, start_date=date.today(), end_date=date.today())
        marketing_costs = generate_fact_marketing_costs(campaigns[:2], target_amount=250000)
        
        # Test schemas
        schema_tests = [
            (products, DIM_PRODUCTS_SCHEMA, 'DIM_PRODUCTS'),
            (retailers, DIM_RETAILERS_SCHEMA, 'DIM_RETAILERS'),
            (campaigns, DIM_CAMPAIGNS_SCHEMA, 'DIM_CAMPAIGNS'),
            (sales, FACT_SALES_SCHEMA, 'FACT_SALES'),
            (inventory, FACT_INVENTORY_SCHEMA, 'FACT_INVENTORY'),
            (marketing_costs, FACT_MARKETING_COSTS_SCHEMA, 'FACT_MARKETING_COSTS')
        ]
        
        for data_list, schema, table_name in schema_tests:
            if not data_list:
                continue
                
            sample_record = data_list[0]
            schema_fields = {field['name'] for field in schema}
            data_fields = set(sample_record.keys())
            
            # Check if all schema fields are present in data
            missing_fields = schema_fields - data_fields
            extra_fields = data_fields - schema_fields
            
            passed = len(missing_fields) == 0
            message = f"Missing: {missing_fields}" if missing_fields else ""
            if extra_fields:
                message += f", Extra: {extra_fields}"
            
            self.log_test(
                f"Schema Compatibility - {table_name}",
                passed,
                message
            )
    
    def test_foreign_key_relationships(self):
        """Test foreign key relationships between tables"""
        print("\n=== Testing Foreign Key Relationships ===")
        
        # Generate related data
        locations = generate_dim_locations(5)
        departments = generate_dim_departments()
        jobs = generate_dim_jobs(departments)
        banks = generate_dim_banks()
        insurance = generate_dim_insurance()
        categories = generate_dim_categories()
        brands = generate_dim_brands()
        subcategories = generate_dim_subcategories()
        products = generate_dim_products(10, categories, brands, subcategories)
        retailers = generate_dim_retailers_normalized(10, locations)
        campaigns = generate_dim_campaigns()
        employees = generate_dim_employees_normalized(10, locations, jobs, banks, insurance)
        inventory = generate_fact_inventory(products[:5], locations[:3])
        sales = generate_fact_sales(employees[:3], products[:3], retailers[:3], campaigns[:3], target_amount=1000000, start_date=date.today(), end_date=date.today())
        
        # Test relationships
        relationship_tests = [
            (retailers, locations, 'location_id', 'location_id', 'Retailers → Locations'),
            (employees, locations, 'location_id', 'location_id', 'Employees → Locations'),
            (employees, jobs, 'job_id', 'job_id', 'Employees → Jobs'),
            (employees, banks, 'bank_id', 'bank_id', 'Employees → Banks'),
            (employees, insurance, 'insurance_id', 'insurance_id', 'Employees → Insurance'),
            (products, categories, 'category_id', 'category_id', 'Products → Categories'),
            (products, brands, 'brand_id', 'brand_id', 'Products → Brands'),
            (products, subcategories, 'subcategory_id', 'subcategory_id', 'Products → Subcategories'),
            (inventory, products, 'product_id', 'product_id', 'Inventory → Products'),
            (inventory, locations, 'location_id', 'location_id', 'Inventory → Locations'),
            (sales, products, 'product_id', 'product_id', 'Sales → Products'),
            (sales, retailers, 'retailer_id', 'retailer_id', 'Sales → Retailers'),
        ]
        
        for child_data, parent_data, child_key, parent_key, relationship in relationship_tests:
            if not child_data or not parent_data:
                continue
                
            child_keys = {record[child_key] for record in child_data}
            parent_keys = {record[parent_key] for record in parent_data}
            
            # Check if all child keys exist in parent
            invalid_keys = child_keys - parent_keys
            passed = len(invalid_keys) == 0
            
            self.log_test(
                f"Foreign Key - {relationship}",
                passed,
                f"Invalid keys: {invalid_keys}" if invalid_keys else "All keys valid"
            )
    
    def test_enhanced_inventory_features(self):
        """Test enhanced inventory FMCG-specific features"""
        print("\n=== Testing Enhanced Inventory Features ===")
        
        # Generate sample data
        locations = generate_dim_locations(3)
        categories = generate_dim_categories()
        brands = generate_dim_brands()
        subcategories = generate_dim_subcategories()
        products = generate_dim_products(5, categories, brands, subcategories)
        inventory = generate_fact_inventory(products[:3], locations[:2])
        
        if not inventory:
            self.log_test("Enhanced Inventory Features", False, "No inventory data generated")
            return
        
        sample_inventory = inventory[0]
        
        # Test required FMCG fields
        required_fields = [
            'reorder_point', 'safety_stock', 'economic_order_quantity',
            'shelf_life_days', 'batch_number', 'manufacture_date',
            'expiration_date', 'days_to_expire', 'inventory_status',
            'storage_conditions', 'inventory_turnover_days',
            'total_inventory_value', 'gross_margin_percent'
        ]
        
        missing_fields = [field for field in required_fields if field not in sample_inventory]
        
        self.log_test(
            "Enhanced Inventory - Required Fields",
            len(missing_fields) == 0,
            f"Missing: {missing_fields}" if missing_fields else "All FMCG fields present"
        )
        
        # Test inventory status values
        valid_statuses = ['Normal', 'Low Stock', 'Overstocked', 'Near Expiry', 'Expired']
        status = sample_inventory.get('inventory_status')
        status_valid = status in valid_statuses
        
        self.log_test(
            "Enhanced Inventory - Status Values",
            status_valid,
            f"Status: {status}, Valid: {valid_statuses}"
        )
        
        # Test storage conditions
        valid_conditions = ['Ambient', 'Refrigerated', 'Frozen', 'Climate Controlled']
        conditions = sample_inventory.get('storage_conditions')
        conditions_valid = conditions in valid_conditions
        
        self.log_test(
            "Enhanced Inventory - Storage Conditions",
            conditions_valid,
            f"Conditions: {conditions}, Valid: {valid_conditions}"
        )
        
        # Test logical relationships
        reorder_point = sample_inventory.get('reorder_point', 0)
        safety_stock = sample_inventory.get('safety_stock', 0)
        cases_on_hand = sample_inventory.get('cases_on_hand', 0)
        
        # Reorder point should be less than safety stock (trigger reorder before safety stock is reached)
        reorder_logic_valid = reorder_point < safety_stock
        
        self.log_test(
            "Enhanced Inventory - Reorder Logic",
            reorder_logic_valid,
            f"Reorder Point: {reorder_point}, Safety Stock: {safety_stock}"
        )
        
        # Test expiration logic
        days_to_expire = sample_inventory.get('days_to_expire', 0)
        expiration_date = sample_inventory.get('expiration_date')
        manufacture_date = sample_inventory.get('manufacture_date')
        
        if expiration_date and manufacture_date:
            calculated_days = (expiration_date - date.today()).days
            expiration_logic_valid = abs(calculated_days - days_to_expire) <= 1  # Allow 1 day difference
            
            self.log_test(
                "Enhanced Inventory - Expiration Logic",
                expiration_logic_valid,
                f"Calculated: {calculated_days}, Stored: {days_to_expire}"
            )
    
    def test_monthly_quarterly_quantities(self):
        """Test monthly and quarterly update quantities"""
        print("\n=== Testing Monthly/Quarterly Quantities ===")
        
        # Test ranges
        employee_range = (1, 5)
        product_range = (2, 6)
        campaign_quantity = 1
        
        # Test if quantities are within expected ranges
        employees_valid = employee_range[0] <= NEW_EMPLOYEES_PER_MONTH <= employee_range[1]
        products_valid = product_range[0] <= NEW_PRODUCTS_PER_MONTH <= product_range[1]
        campaigns_valid = NEW_CAMPAIGNS_PER_QUARTER == campaign_quantity
        
        self.log_test(
            "Monthly Employees Range",
            employees_valid,
            f"NEW_EMPLOYEES_PER_MONTH: {NEW_EMPLOYEES_PER_MONTH}, Expected: {employee_range}"
        )
        
        self.log_test(
            "Monthly Products Range",
            products_valid,
            f"NEW_PRODUCTS_PER_MONTH: {NEW_PRODUCTS_PER_MONTH}, Expected: {product_range}"
        )
        
        self.log_test(
            "Quarterly Campaigns Quantity",
            campaigns_valid,
            f"NEW_CAMPAIGNS_PER_QUARTER: {NEW_CAMPAIGNS_PER_QUARTER}, Expected: {campaign_quantity}"
        )
    
    def test_data_validation_function(self):
        """Test the validate_relationships function"""
        print("\n=== Testing Data Validation Function ===")
        
        # Generate test data
        locations = generate_dim_locations(3)
        departments = generate_dim_departments()
        jobs = generate_dim_jobs(departments)
        banks = generate_dim_banks()
        insurance = generate_dim_insurance()
        categories = generate_dim_categories()
        brands = generate_dim_brands()
        subcategories = generate_dim_subcategories()
        products = generate_dim_products(5, categories, brands, subcategories)
        retailers = generate_dim_retailers_normalized(5, locations)
        campaigns = generate_dim_campaigns()
        employees = generate_dim_employees_normalized(5, locations, jobs, banks, insurance)
        
        # Run validation
        validation_result = validate_relationships(
            employees, products, retailers, campaigns,
            locations, departments, jobs, banks, insurance,
            categories, brands, subcategories
        )
        
        self.log_test(
            "Data Validation Function",
            validation_result,
            "All relationships validated successfully" if validation_result else "Validation failed"
        )
    
    def run_all_tests(self):
        """Run all consistency tests"""
        print("🧪 Starting FMCG Relational Consistency Tests")
        print("=" * 50)
        
        self.test_id_pattern_consistency()
        self.test_schema_vs_data_compatibility()
        self.test_foreign_key_relationships()
        self.test_enhanced_inventory_features()
        self.test_monthly_quarterly_quantities()
        self.test_data_validation_function()
        
        # Summary
        print("\n" + "=" * 50)
        print("🏁 TEST SUMMARY")
        print("=" * 50)
        
        total_tests = len(self.test_results)
        passed_tests = len([r for r in self.test_results if 'PASS' in r['status']])
        failed_tests = total_tests - passed_tests
        
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {failed_tests}")
        print(f"Warnings: {len(self.warnings)}")
        
        if failed_tests > 0:
            print("\n❌ FAILED TESTS:")
            for error in self.errors:
                print(f"   - {error}")
        
        if self.warnings:
            print("\n⚠️  WARNINGS:")
            for warning in self.warnings:
                print(f"   - {warning}")
        
        if failed_tests == 0:
            print("\n🎉 ALL TESTS PASSED! The codebase is relationally consistent.")
        else:
            print(f"\n🚨 {failed_tests} tests failed. Please fix the issues before proceeding.")
        
        return failed_tests == 0

if __name__ == "__main__":
    tester = RelationalConsistencyTester()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)
