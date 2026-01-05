"""
Comprehensive test suite for FMCG Data Analytics Platform
Tests all major components and scenarios before deployment
"""

import pytest
import pandas as pd
import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, date

# Add src to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

try:
    from src.utils.bigquery_client import BigQueryManager
    from src.utils.logger import setup_logger
    from src.data.schemas import ALL_SCHEMAS, get_bigquery_schema
    from src.core.generators import (
        LocationGenerator, DepartmentGenerator, JobGenerator, EmployeeGenerator,
        ProductGenerator, RetailerGenerator, CampaignGenerator
    )
    from src.etl.pipeline import ETLPipeline
    from config.settings import settings
except ImportError as e:
    print(f"Import error: {e}")
    sys.exit(1)


class TestConfiguration:
    """Test configuration and settings"""
    
    def test_settings_defaults(self):
        """Test default settings values"""
        assert settings.gcp_project_id == "fmcg-data-generator"
        assert settings.gcp_dataset == "fmcg_warehouse"
        assert settings.initial_employees == 350
        assert settings.initial_products == 150
        assert settings.initial_retailers == 500
        assert settings.initial_sales_amount == 8000000000
        assert settings.daily_sales_amount == 2000000
    
    def test_environment_variables(self):
        """Test environment variable handling"""
        # Test with environment variables set
        with patch.dict(os.environ, {
            'GCP_PROJECT_ID': 'test-project',
            'BQ_DATASET': 'test-dataset',
            'INITIAL_EMPLOYEES': '100'
        }):
            from config.settings import Settings
            test_settings = Settings()
            assert test_settings.gcp_project_id == 'test-project'
            assert test_settings.gcp_dataset == 'test-dataset'
            assert test_settings.initial_employees == 100


class TestSchemas:
    """Test data schemas"""
    
    def test_all_schemas_defined(self):
        """Test all required schemas are defined"""
        required_tables = [
            'dim_employees', 'dim_products', 'dim_retailers', 'dim_campaigns',
            'dim_locations', 'dim_departments', 'dim_jobs',
            'fact_sales', 'fact_inventory', 'fact_operating_costs', 'fact_marketing_costs'
        ]
        
        for table in required_tables:
            assert table in ALL_SCHEMAS, f"Missing schema for {table}"
    
    def test_schema_structure(self):
        """Test schema structure and fields"""
        for table_name, schema in ALL_SCHEMAS.items():
            assert hasattr(schema, 'name'), f"Schema {table_name} missing name"
            assert hasattr(schema, 'type'), f"Schema {table_name} missing type"
            assert hasattr(schema, 'fields'), f"Schema {table_name} missing fields"
            assert len(schema.fields) > 0, f"Schema {table_name} has no fields"
            
            # Check field structure
            for field in schema.fields:
                assert 'name' in field, f"Field in {table_name} missing name"
                assert 'type' in field, f"Field in {table_name} missing type"
                assert 'mode' in field, f"Field in {table_name} missing mode"
    
    def test_bigquery_schema_conversion(self):
        """Test conversion to BigQuery schema"""
        for table_name, schema in ALL_SCHEMAS.items():
            bq_schema = get_bigquery_schema(schema)
            assert len(bq_schema) == len(schema.fields)
            
            for i, bq_field in enumerate(bq_schema):
                original_field = schema.fields[i]
                assert bq_field.name == original_field['name']
                assert bq_field.field_type == original_field['type']
                assert bq_field.mode == original_field['mode']


class TestGenerators:
    """Test data generators"""
    
    def setup_method(self):
        """Setup test fixtures"""
        from faker import Faker
        self.faker = Faker()
    
    def test_location_generator(self):
        """Test location data generation"""
        generator = LocationGenerator(self.faker)
        
        # Test small dataset
        locations = generator.generate_locations(10)
        assert isinstance(locations, pd.DataFrame)
        assert len(locations) == 10
        
        # Check required columns
        required_columns = ['location_id', 'region', 'city', 'province']
        for col in required_columns:
            assert col in locations.columns, f"Missing column: {col}"
        
        # Check data validity
        assert all(locations['location_id'] > 0)
        assert all(locations['region'].isin(generator.PHILIPPINES_REGIONS.keys()))
    
    def test_department_generator(self):
        """Test department data generation"""
        generator = DepartmentGenerator(self.faker)
        departments = generator.generate_departments()
        
        assert isinstance(departments, pd.DataFrame)
        assert len(departments) == len(generator.DEPARTMENTS)
        
        required_columns = ['department_id', 'department_name', 'budget']
        for col in required_columns:
            assert col in departments.columns, f"Missing column: {col}"
        
        assert all(departments['budget'] > 0)
    
    def test_job_generator(self):
        """Test job data generation"""
        dept_gen = DepartmentGenerator(self.faker)
        departments_df = dept_gen.generate_departments()
        
        generator = JobGenerator(self.faker, departments_df)
        jobs = generator.generate_jobs()
        
        assert isinstance(jobs, pd.DataFrame)
        assert len(jobs) == len(generator.JOBS)
        
        required_columns = ['job_id', 'job_title', 'min_salary', 'max_salary']
        for col in required_columns:
            assert col in jobs.columns, f"Missing column: {col}"
        
        # Check salary logic
        assert all(jobs['min_salary'] <= jobs['max_salary'])
        assert all(jobs['min_salary'] > 0)
    
    def test_employee_generator(self):
        """Test employee data generation"""
        # Setup dependencies
        location_gen = LocationGenerator(self.faker)
        dept_gen = DepartmentGenerator(self.faker)
        locations_df = location_gen.generate_locations(5)
        departments_df = dept_gen.generate_departments()
        jobs_df = JobGenerator(self.faker, departments_df).generate_jobs()
        
        generator = EmployeeGenerator(self.faker, departments_df, jobs_df, locations_df)
        employees = generator.generate_employees(10)
        
        assert isinstance(employees, pd.DataFrame)
        assert len(employees) == 10
        
        required_columns = ['employee_id', 'first_name', 'last_name', 'salary']
        for col in required_columns:
            assert col in employees.columns, f"Missing column: {col}"
        
        assert all(employees['salary'] > 0)
    
    def test_product_generator(self):
        """Test product data generation"""
        generator = ProductGenerator(self.faker)
        products, categories, subcategories, brands = generator.generate_products(10)
        
        # Test products
        assert isinstance(products, pd.DataFrame)
        assert len(products) == 10
        
        required_columns = ['product_id', 'product_name', 'unit_price', 'cost']
        for col in required_columns:
            assert col in products.columns, f"Missing column: {col}"
        
        assert all(products['unit_price'] > 0)
        assert all(products['cost'] > 0)
        assert all(products['cost'] < products['unit_price'])
        
        # Test related tables
        assert isinstance(categories, pd.DataFrame)
        assert isinstance(subcategories, pd.DataFrame)
        assert isinstance(brands, pd.DataFrame)
    
    def test_retailer_generator(self):
        """Test retailer data generation"""
        location_gen = LocationGenerator(self.faker)
        locations_df = location_gen.generate_locations(5)
        
        generator = RetailerGenerator(self.faker)
        retailers = generator.generate_retailers(10, locations_df)
        
        assert isinstance(retailers, pd.DataFrame)
        assert len(retailers) == 10
        
        required_columns = ['retailer_id', 'retailer_name', 'retailer_type']
        for col in required_columns:
            assert col in retailers.columns, f"Missing column: {col}"
    
    def test_campaign_generator(self):
        """Test campaign data generation"""
        generator = CampaignGenerator(self.faker)
        campaigns = generator.generate_campaigns(10)
        
        assert isinstance(campaigns, pd.DataFrame)
        assert len(campaigns) == 10
        
        required_columns = ['campaign_id', 'campaign_name', 'budget']
        for col in required_columns:
            assert col in campaigns.columns, f"Missing column: {col}"
        
        assert all(campaigns['budget'] > 0)


class TestBigQueryClient:
    """Test BigQuery client functionality"""
    
    @patch('src.utils.bigquery_client.bigquery.Client')
    def test_client_creation(self, mock_client):
        """Test BigQuery client creation"""
        mock_instance = Mock()
        mock_client.return_value = mock_instance
        
        bq_manager = BigQueryManager('test-project', 'test-dataset')
        
        mock_client.assert_called_once_with(project='test-project')
        assert bq_manager.project_id == 'test-project'
        assert bq_manager.dataset == 'test-dataset'
    
    @patch('src.utils.bigquery_client.bigquery.Client')
    def test_ensure_dataset(self, mock_client):
        """Test dataset creation"""
        mock_instance = Mock()
        mock_client.return_value = mock_instance
        
        # Mock dataset exists
        mock_instance.get_dataset.return_value = Mock(dataset_id='test-dataset')
        
        bq_manager = BigQueryManager('test-project', 'test-dataset')
        dataset = bq_manager.ensure_dataset()
        
        mock_instance.get_dataset.assert_called_once()
    
    @patch('src.utils.bigquery_client.bigquery.Client')
    def test_table_creation(self, mock_client):
        """Test table creation"""
        mock_instance = Mock()
        mock_client.return_value = mock_instance
        
        # Mock table doesn't exist
        mock_instance.get_dataset.side_effect = Exception("Not found")
        mock_instance.create_table.return_value = Mock()
        
        bq_manager = BigQueryManager('test-project', 'test-dataset')
        
        from google.cloud.bigquery import SchemaField
        schema = [SchemaField('test', 'STRING', 'NULLABLE')]
        bq_manager.create_table('test_table', schema)
        
        mock_instance.create_table.assert_called_once()


class TestETLPipeline:
    """Test ETL pipeline functionality"""
    
    @patch('src.etl.pipeline.BigQueryManager')
    def test_pipeline_initialization(self, mock_bq):
        """Test pipeline initialization"""
        mock_bq_instance = Mock()
        mock_bq.return_value = mock_bq_instance
        
        pipeline = ETLPipeline(mock_bq_instance)
        assert pipeline.bq_manager == mock_bq_instance
        assert pipeline.faker is not None
    
    @patch('src.etl.pipeline.BigQueryManager')
    def test_setup_database(self, mock_bq):
        """Test database setup"""
        mock_bq_instance = Mock()
        mock_bq.return_value = mock_bq_instance
        
        pipeline = ETLPipeline(mock_bq_instance)
        pipeline.setup_database()
        
        mock_bq_instance.ensure_dataset.assert_called_once()
    
    @patch('src.etl.pipeline.BigQueryManager')
    def test_dimension_data_generation(self, mock_bq):
        """Test dimension data generation"""
        mock_bq_instance = Mock()
        mock_bq.return_value = mock_bq_instance
        
        pipeline = ETLPipeline(mock_bq_instance)
        
        config = {
            'locations_count': 5,
            'initial_employees': 10,
            'initial_products': 5,
            'initial_retailers': 5
        }
        
        pipeline.generate_dimension_data(config)
        
        # Check that all dimension tables are generated
        expected_tables = [
            'dim_locations', 'dim_departments', 'dim_jobs', 'dim_employees',
            'dim_products', 'dim_categories', 'dim_subcategories', 'dim_brands',
            'dim_retailers', 'dim_campaigns'
        ]
        
        for table in expected_tables:
            assert table in pipeline.data_cache, f"Missing dimension table: {table}"
    
    @patch('src.etl.pipeline.BigQueryManager')
    def test_fact_data_generation(self, mock_bq):
        """Test fact data generation"""
        mock_bq_instance = Mock()
        mock_bq.return_value = mock_bq_instance
        
        pipeline = ETLPipeline(mock_bq_instance)
        
        # Mock dimension data
        from faker import Faker
        faker = Faker()
        
        # Add mock dimension data
        pipeline.data_cache['dim_products'] = pd.DataFrame({'product_id': [1, 2]})
        pipeline.data_cache['dim_retailers'] = pd.DataFrame({'retailer_id': [1, 2]})
        pipeline.data_cache['dim_employees'] = pd.DataFrame({'employee_id': [1, 2]})
        pipeline.data_cache['dim_campaigns'] = pd.DataFrame({'campaign_id': [1, 2]})
        
        config = {
            'initial_sales_amount': 1000000,
            'daily_sales_amount': 10000
        }
        
        pipeline.generate_fact_data(config)
        
        # Check that all fact tables are generated
        expected_tables = ['fact_sales', 'fact_inventory', 'fact_operating_costs', 'fact_marketing_costs']
        
        for table in expected_tables:
            assert table in pipeline.data_cache, f"Missing fact table: {table}"


class TestErrorHandling:
    """Test error handling scenarios"""
    
    def test_import_error_handling(self):
        """Test import error handling in main script"""
        # This tests the import error handling we added to main.py and setup.py
        with patch.dict(sys.path, [], clear=True):
            try:
                from src.main import main
                assert False, "Should have raised ImportError"
            except SystemExit:
                pass  # Expected behavior
    
    @patch('src.utils.bigquery_client.bigquery.Client')
    def test_bigquery_connection_error(self, mock_client):
        """Test BigQuery connection error handling"""
        mock_client.side_effect = Exception("Connection failed")
        
        try:
            BigQueryManager('test-project', 'test-dataset')
            assert False, "Should have raised exception"
        except Exception:
            pass  # Expected behavior
    
    def test_missing_environment_variables(self):
        """Test handling of missing environment variables"""
        # Clear environment variables
        with patch.dict(os.environ, {}, clear=True):
            from scripts.setup import verify_configuration
            
            # Should not fail, should use defaults
            try:
                verify_configuration()
            except SystemExit:
                assert False, "Should not exit with defaults available"


class TestDataValidation:
    """Test data validation and quality"""
    
    def test_employee_data_quality(self):
        """Test employee data quality"""
        from faker import Faker
        faker = Faker()
        
        # Setup dependencies
        location_gen = LocationGenerator(faker)
        dept_gen = DepartmentGenerator(faker)
        locations_df = location_gen.generate_locations(5)
        departments_df = dept_gen.generate_departments()
        jobs_df = JobGenerator(faker, departments_df).generate_jobs()
        
        generator = EmployeeGenerator(faker, departments_df, jobs_df, locations_df)
        employees = generator.generate_employees(50)
        
        # Data quality checks
        assert len(employees) == 50
        assert employees['employee_id'].is_unique
        assert all(employees['salary'] > 0)
        assert all(employees['first_name'].str.len() > 0)
        assert all(employees['last_name'].str.len() > 0)
        
        # Check hire dates are reasonable
        today = date.today()
        assert all(pd.to_datetime(employees['hire_date']).dt.date <= today)
    
    def test_product_data_quality(self):
        """Test product data quality"""
        from faker import Faker
        faker = Faker()
        
        generator = ProductGenerator(faker)
        products, categories, subcategories, brands = generator.generate_products(50)
        
        # Data quality checks
        assert len(products) == 50
        assert products['product_id'].is_unique
        assert all(products['unit_price'] > 0)
        assert all(products['cost'] > 0)
        assert all(products['cost'] < products['unit_price'])
        
        # Check SKU format
        assert all(products['sku'].str.startswith('SKU-'))
    
    def test_sales_data_relationships(self):
        """Test sales data foreign key relationships"""
        from faker import Faker
        faker = Faker()
        
        # Generate related data
        location_gen = LocationGenerator(faker)
        dept_gen = DepartmentGenerator(faker)
        locations_df = location_gen.generate_locations(5)
        departments_df = dept_gen.generate_departments()
        jobs_df = JobGenerator(faker, departments_df).generate_jobs()
        employees_df = EmployeeGenerator(faker, departments_df, jobs_df, locations_df).generate_employees(10)
        
        product_gen = ProductGenerator(faker)
        products_df, _, _, _ = product_gen.generate_products(10)
        
        retailer_gen = RetailerGenerator(faker)
        retailers_df = retailer_gen.generate_retailers(5, locations_df)
        
        # Generate sales data
        sales = []
        for i in range(20):
            sale = {
                'sale_id': i + 1,
                'product_id': products_df.sample(1)['product_id'].iloc[0],
                'retailer_id': retailers_df.sample(1)['retailer_id'].iloc[0],
                'employee_id': employees_df.sample(1)['employee_id'].iloc[0],
                'quantity': 1,
                'unit_price': 10.0,
                'total_amount': 10.0,
                'order_date': date.today(),
                'delivery_status': 'Pending',
                'created_at': datetime.now()
            }
            sales.append(sale)
        
        sales_df = pd.DataFrame(sales)
        
        # Check foreign key relationships
        assert all(sales_df['product_id'].isin(products_df['product_id']))
        assert all(sales_df['retailer_id'].isin(retailers_df['retailer_id']))
        assert all(sales_df['employee_id'].isin(employees_df['employee_id']))


class TestPerformance:
    """Test performance and scalability"""
    
    def test_large_dataset_generation(self):
        """Test generation of larger datasets"""
        from faker import Faker
        faker = Faker()
        
        # Test with larger numbers
        generator = LocationGenerator(faker)
        locations = generator.generate_locations(100)
        
        assert len(locations) == 100
        assert len(locations['location_id'].unique()) == 100  # Check uniqueness
        
        # Test memory usage (basic check)
        assert locations.memory_usage(deep=True).sum() < 50 * 1024 * 1024  # Less than 50MB
    
    def test_batch_processing_simulation(self):
        """Test batch processing simulation"""
        batch_size = 1000
        total_records = 5000
        
        # Simulate batch processing
        batches = []
        for i in range(0, total_records, batch_size):
            batch_end = min(i + batch_size, total_records)
            batch_size_actual = batch_end - i
            batches.append(batch_size_actual)
        
        assert sum(batches) == total_records
        assert len(batches) == 5  # 5000 / 1000 = 5 batches


if __name__ == "__main__":
    # Run comprehensive tests
    print("Running comprehensive FMCG Data Analytics Platform tests...")
    print("=" * 60)
    
    # Run pytest with verbose output
    pytest.main([__file__, "-v", "--tb=short"])
