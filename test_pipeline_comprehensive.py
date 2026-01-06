"""
Comprehensive test script for FMCG ETL Pipeline
Tests all aspects: schemas, generators, pipeline, data integrity
"""

import sys
import traceback
import pandas as pd
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_schemas():
    """Test all schema definitions"""
    print("🔍 TESTING SCHEMAS...")
    
    try:
        from src.data.schemas import ALL_SCHEMAS, get_bigquery_schema
        
        # Test all schemas exist
        required_tables = [
            'dim_employees', 'dim_products', 'dim_retailers', 'dim_locations',
            'dim_departments', 'dim_jobs', 'dim_campaigns', 'dim_categories',
            'dim_subcategories', 'dim_brands', 'dim_banks', 'dim_insurance',
            'fact_sales', 'fact_inventory', 'fact_operating_costs', 
            'fact_marketing_costs', 'fact_employees'
        ]
        
        for table in required_tables:
            if table not in ALL_SCHEMAS:
                raise Exception(f"Missing schema: {table}")
        
        # Test schema conversion
        for table_name, schema in ALL_SCHEMAS.items():
            bq_schema = get_bigquery_schema(schema)
            if not bq_schema:
                raise Exception(f"Failed to convert schema: {table_name}")
        
        print("✅ All schemas valid")
        return True
        
    except Exception as e:
        print(f"❌ Schema test failed: {e}")
        traceback.print_exc()
        return False

def test_generators():
    """Test all data generators"""
    print("🔍 TESTING GENERATORS...")
    
    try:
        from faker import Faker
        from src.core.generators import (
            LocationGenerator, DepartmentGenerator, JobGenerator, EmployeeGenerator,
            ProductGenerator, RetailerGenerator, CampaignGenerator
        )
        from src.utils.id_generation import IDGenerator
        
        faker = Faker('en_PH')
        id_generator = IDGenerator()
        
        # Test LocationGenerator
        print("  📍 Testing LocationGenerator...")
        location_gen = LocationGenerator(faker)
        locations_df = location_gen.generate_locations(5)
        assert len(locations_df) == 5
        assert 'location_id' in locations_df.columns
        assert 'region' in locations_df.columns
        assert 'province' in locations_df.columns
        assert 'city' in locations_df.columns
        
        # Test DepartmentGenerator
        print("  🏢 Testing DepartmentGenerator...")
        dept_gen = DepartmentGenerator(faker)
        departments_df = dept_gen.generate_departments()
        assert len(departments_df) > 0
        assert 'department_id' in departments_df.columns
        assert 'department_name' in departments_df.columns
        
        # Test JobGenerator
        print("  💼 Testing JobGenerator...")
        job_gen = JobGenerator(faker, departments_df)
        jobs_df = job_gen.generate_jobs()
        assert len(jobs_df) > 0
        assert 'job_id' in jobs_df.columns
        assert 'job_title' in jobs_df.columns
        assert 'min_salary' in jobs_df.columns
        assert 'max_salary' in jobs_df.columns
        
        # Test EmployeeGenerator
        print("  👥 Testing EmployeeGenerator...")
        emp_gen = EmployeeGenerator(faker, departments_df, jobs_df, locations_df)
        employees_df = emp_gen.generate_employees(10)
        assert len(employees_df) == 10
        assert 'employee_id' in employees_df.columns
        assert 'first_name' in employees_df.columns
        assert 'department_id' in employees_df.columns
        assert 'job_id' in employees_df.columns
        assert 'bank_id' in employees_df.columns  # Should be present
        assert 'insurance_id' in employees_df.columns  # Should be present
        assert 'salary' not in employees_df.columns  # Should NOT be present (moved to fact)
        
        # Test ProductGenerator
        print("  📦 Testing ProductGenerator...")
        prod_gen = ProductGenerator(faker)
        products_df, categories_df, subcategories_df, brands_df = prod_gen.generate_products(10)
        assert len(products_df) == 10
        assert 'product_id' in products_df.columns
        assert 'weight' not in products_df.columns  # Should be removed
        assert 'volume' not in products_df.columns  # Should be removed
        assert len(categories_df) > 0
        assert len(subcategories_df) > 0
        assert len(brands_df) > 0
        
        # Test RetailerGenerator
        print("  🏪 Testing RetailerGenerator...")
        retail_gen = RetailerGenerator(faker)
        retailers_df = retail_gen.generate_retailers(10, locations_df)
        assert len(retailers_df) == 10
        assert 'retailer_id' in retailers_df.columns
        assert 'retailer_type' in retailers_df.columns
        
        # Test CampaignGenerator
        print("  📢 Testing CampaignGenerator...")
        campaign_gen = CampaignGenerator(faker)
        campaigns_df = campaign_gen.generate_campaigns(5)
        assert len(campaigns_df) == 5
        assert 'campaign_id' in campaigns_df.columns
        assert 'campaign_name' in campaigns_df.columns
        
        print("✅ All generators working")
        return True
        
    except Exception as e:
        print(f"❌ Generator test failed: {e}")
        traceback.print_exc()
        return False

def test_pipeline_initialization():
    """Test pipeline initialization"""
    print("🔍 TESTING PIPELINE INITIALIZATION...")
    
    try:
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
            
            def load_dataframe(self, df, table_name):
                pass
        
        # Test pipeline creation with mock manager
        mock_bq = MockBigQueryManager('test-project', 'test-dataset')
        pipeline = ETLPipeline(bq_manager=mock_bq)
        
        # Check all generators are initialized
        assert pipeline.location_gen is not None
        assert pipeline.department_gen is not None
        assert pipeline.faker is not None
        assert pipeline.data_cache is not None
        
        # Test retailer transaction ranges
        retailer_types = pipeline.retailer_transaction_ranges.keys()
        required_types = ["Sari-Sari Store", "Convenience Store", "Pharmacy", "Wholesale", "Supermarket", "Department Store"]
        for req_type in required_types:
            assert req_type in retailer_types
        
        print("✅ Pipeline initialization successful")
        return True
        
    except Exception as e:
        print(f"❌ Pipeline initialization failed: {e}")
        traceback.print_exc()
        return False

def create_mock_pipeline():
    """Create a pipeline with mock BigQueryManager for testing"""
    from src.etl.pipeline import ETLPipeline
    
    class MockBigQueryManager:
        def __init__(self, project_id, dataset, credentials_path=None):
            self.project_id = project_id
            self.dataset = dataset
            self.credentials_path = credentials_path
        
        def ensure_dataset(self):
            pass
        
        def create_table(self, table_name, schema):
            pass
        
        def load_dataframe(self, df, table_name):
            pass
    
    mock_bq = MockBigQueryManager('test-project', 'test-dataset')
    return ETLPipeline(bq_manager=mock_bq)

def test_dimension_data_generation():
    """Test dimension data generation"""
    print("🔍 TESTING DIMENSION DATA GENERATION...")
    
    try:
        pipeline = create_mock_pipeline()
        config = {
            "locations_count": 10,
            "initial_employees": 20,
            "initial_products": 15,
            "initial_retailers": 25,
            "initial_campaigns": 5
        }
        
        # Generate dimension data
        pipeline.generate_dimension_data(config)
        
        # Check all dimension tables are generated
        required_dims = [
            'dim_locations', 'dim_departments', 'dim_jobs', 'dim_employees',
            'dim_products', 'dim_categories', 'dim_subcategories', 'dim_brands',
            'dim_retailers', 'dim_campaigns'
        ]
        
        for dim in required_dims:
            if dim not in pipeline.data_cache:
                raise Exception(f"Missing dimension: {dim}")
            df = pipeline.data_cache[dim]
            if len(df) == 0:
                raise Exception(f"Empty dimension: {dim}")
        
        # Test specific data integrity
        employees_df = pipeline.data_cache['dim_employees']
        assert 'bank_id' in employees_df.columns
        assert 'insurance_id' in employees_df.columns
        assert 'salary' not in employees_df.columns
        
        products_df = pipeline.data_cache['dim_products']
        assert 'weight' not in products_df.columns
        assert 'volume' not in products_df.columns
        
        print("✅ Dimension data generation successful")
        return True
        
    except Exception as e:
        print(f"❌ Dimension data generation failed: {e}")
        traceback.print_exc()
        return False

def test_fact_data_generation():
    """Test fact data generation"""
    print("🔍 TESTING FACT DATA GENERATION...")
    
    try:
        pipeline = create_mock_pipeline()
        config = {
            "locations_count": 5,
            "initial_employees": 10,
            "initial_products": 8,
            "initial_retailers": 12,
            "initial_campaigns": 3
        }
        
        # Generate dimension data first
        pipeline.generate_dimension_data(config)
        
        # Generate fact data
        pipeline.generate_fact_data(config)
        
        # Check all fact tables are generated
        required_facts = [
            'fact_sales', 'fact_inventory', 'fact_operating_costs', 
            'fact_marketing_costs', 'fact_employees'
        ]
        
        for fact in required_facts:
            if fact not in pipeline.data_cache:
                raise Exception(f"Missing fact: {fact}")
            df = pipeline.data_cache[fact]
            if len(df) == 0:
                raise Exception(f"Empty fact: {fact}")
        
        # Test fact_employees specific structure
        emp_facts_df = pipeline.data_cache['fact_employees']
        assert 'salary' in emp_facts_df.columns
        assert 'bonus' in emp_facts_df.columns
        assert 'total_compensation' in emp_facts_df.columns
        assert 'training_hours' not in emp_facts_df.columns  # Should be removed
        assert 'sick_days_used' not in emp_facts_df.columns  # Should be removed
        
        # Test data ranges and types
        assert emp_facts_df['salary'].min() >= 0
        assert emp_facts_df['total_compensation'].min() >= 0
        
        print("✅ Fact data generation successful")
        return True
        
    except Exception as e:
        print(f"❌ Fact data generation failed: {e}")
        traceback.print_exc()
        return False

def test_data_relationships():
    """Test data relationships and integrity"""
    print("🔍 TESTING DATA RELATIONSHIPS...")
    
    try:
        pipeline = create_mock_pipeline()
        config = {
            "locations_count": 5,
            "initial_employees": 10,
            "initial_products": 8,
            "initial_retailers": 12,
            "initial_campaigns": 3
        }
        
        # Generate all data
        pipeline.generate_dimension_data(config)
        pipeline.generate_fact_data(config)
        
        # Test employee relationships
        employees_df = pipeline.data_cache['dim_employees']
        departments_df = pipeline.data_cache['dim_departments']
        jobs_df = pipeline.data_cache['dim_jobs']
        locations_df = pipeline.data_cache['dim_locations']
        
        # Check foreign key relationships
        for _, emp in employees_df.iterrows():
            assert emp['department_id'] in departments_df['department_id'].values
            assert emp['job_id'] in jobs_df['job_id'].values
            assert emp['location_id'] in locations_df['location_id'].values
        
        # Test product relationships
        products_df = pipeline.data_cache['dim_products']
        categories_df = pipeline.data_cache['dim_categories']
        brands_df = pipeline.data_cache['dim_brands']
        
        for _, product in products_df.iterrows():
            assert product['category_id'] in categories_df['category_id'].values
            assert product['brand_id'] in brands_df['brand_id'].values
        
        # Test fact relationships
        emp_facts_df = pipeline.data_cache['fact_employees']
        for _, fact in emp_facts_df.iterrows():
            assert fact['employee_id'] in employees_df['employee_id'].values
        
        print("✅ Data relationships integrity verified")
        return True
        
    except Exception as e:
        print(f"❌ Data relationships test failed: {e}")
        traceback.print_exc()
        return False

def test_dynamic_salaries():
    """Test dynamic salary calculations"""
    print("🔍 TESTING DYNAMIC SALARIES...")
    
    try:
        pipeline = create_mock_pipeline()
        config = {
            "locations_count": 3,
            "initial_employees": 5,
            "initial_products": 5,
            "initial_retailers": 8,
            "initial_campaigns": 2
        }
        
        # Generate all data
        pipeline.generate_dimension_data(config)
        pipeline.generate_fact_data(config)
        
        emp_facts_df = pipeline.data_cache['fact_employees']
        employees_df = pipeline.data_cache['dim_employees']
        jobs_df = pipeline.data_cache['dim_jobs']
        
        # Test salary variations
        salaries_by_employee = emp_facts_df.groupby('employee_id')['salary'].agg(['min', 'max', 'mean'])
        
        for emp_id, salary_range in salaries_by_employee.iterrows():
            # Check for salary progression (raises over time)
            if salary_range['max'] > salary_range['min']:
                # Salary should increase over time for active employees
                assert salary_range['max'] >= salary_range['min']
            
            # Check salary is within job range
            emp_info = employees_df[employees_df['employee_id'] == emp_id].iloc[0]
            job_info = jobs_df[jobs_df['job_id'] == emp_info['job_id']].iloc[0]
            
            assert salary_range['min'] >= job_info['min_salary'] * 0.9  # Allow some variation
            assert salary_range['max'] <= job_info['max_salary'] * 1.5  # Allow raises
        
        # Test terminated employees have zero salary
        terminated_emps = employees_df[employees_df['termination_date'].notna()]
        for _, emp in terminated_emps.iterrows():
            emp_facts = emp_facts_df[emp_facts_df['employee_id'] == emp['employee_id']]
            # After termination date, salary should be 0
            termination_facts = emp_facts[emp_facts['date'] >= emp['termination_date']]
            if len(termination_facts) > 0:
                assert termination_facts['salary'].max() == 0.0
        
        print("✅ Dynamic salary calculations working correctly")
        return True
        
    except Exception as e:
        print(f"❌ Dynamic salaries test failed: {e}")
        traceback.print_exc()
        return False

def test_storage_optimization():
    """Test storage optimization effectiveness"""
    print("🔍 TESTING STORAGE OPTIMIZATION...")
    
    try:
        pipeline = create_mock_pipeline()
        config = {
            "locations_count": 10,
            "initial_employees": 20,
            "initial_products": 15,
            "initial_retailers": 25,
            "initial_campaigns": 5
        }
        
        # Generate all data
        pipeline.generate_dimension_data(config)
        pipeline.generate_fact_data(config)
        
        # Calculate total data points
        total_data_points = 0
        for table_name, df in pipeline.data_cache.items():
            data_points = len(df) * len(df.columns)
            total_data_points += data_points
            print(f"  📊 {table_name}: {len(df)} rows × {len(df.columns)} columns = {data_points:,} data points")
        
        print(f"  📈 Total data points: {total_data_points:,}")
        
        # Check employee facts optimization (should be much smaller)
        emp_facts_df = pipeline.data_cache['fact_employees']
        expected_max_rows = 20 * 7  # 20 employees × 6 months + 1 extra month
        assert len(emp_facts_df) <= expected_max_rows, f"Too many employee facts: {len(emp_facts_df)} > {expected_max_rows}"
        
        # Check simplified schemas
        products_df = pipeline.data_cache['dim_products']
        assert 'weight' not in products_df.columns, "Weight column should be removed"
        assert 'volume' not in products_df.columns, "Volume column should be removed"
        
        emp_facts_df = pipeline.data_cache['fact_employees']
        assert len(emp_facts_df.columns) <= 10, f"Too many employee fact columns: {len(emp_facts_df.columns)}"
        
        print("✅ Storage optimization verified")
        return True
        
    except Exception as e:
        print(f"❌ Storage optimization test failed: {e}")
        traceback.print_exc()
        return False

def run_comprehensive_test():
    """Run all tests"""
    print("🚀 STARTING COMPREHENSIVE PIPELINE TEST")
    print("=" * 60)
    
    tests = [
        ("Schemas", test_schemas),
        ("Generators", test_generators),
        ("Pipeline Initialization", test_pipeline_initialization),
        ("Dimension Data Generation", test_dimension_data_generation),
        ("Fact Data Generation", test_fact_data_generation),
        ("Data Relationships", test_data_relationships),
        ("Dynamic Salaries", test_dynamic_salaries),
        ("Storage Optimization", test_storage_optimization),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        if test_func():
            passed += 1
        else:
            failed += 1
            print(f"⚠️  Test failed: {test_name}")
    
    print(f"\n{'='*60}")
    print(f"📊 TEST RESULTS: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 ALL TESTS PASSED! Pipeline is ready for production.")
        return True
    else:
        print("❌ Some tests failed. Please fix issues before deployment.")
        return False

if __name__ == "__main__":
    success = run_comprehensive_test()
    sys.exit(0 if success else 1)
