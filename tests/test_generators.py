"""
Tests for data generators
"""

import pytest
import pandas as pd
from datetime import datetime
from faker import Faker

from src.core.generators import (
    LocationGenerator, DepartmentGenerator, JobGenerator, 
    EmployeeGenerator, ProductGenerator, RetailerGenerator, CampaignGenerator
)


class TestLocationGenerator:
    """Test LocationGenerator"""
    
    def setup_method(self):
        self.faker = Faker()
        self.generator = LocationGenerator(self.faker)
    
    def test_generate_locations(self):
        """Test location generation"""
        locations = self.generator.generate_locations(10)
        
        assert isinstance(locations, pd.DataFrame)
        assert len(locations) == 10
        assert "location_id" in locations.columns
        assert "region" in locations.columns
        assert "city" in locations.columns
        assert "province" in locations.columns
        
        # Check if all locations have valid Philippines regions
        valid_regions = set(self.generator.PHILIPPINES_REGIONS.keys())
        assert set(locations["region"]).issubset(valid_regions)


class TestDepartmentGenerator:
    """Test DepartmentGenerator"""
    
    def setup_method(self):
        self.faker = Faker()
        self.generator = DepartmentGenerator(self.faker)
    
    def test_generate_departments(self):
        """Test department generation"""
        departments = self.generator.generate_departments()
        
        assert isinstance(departments, pd.DataFrame)
        assert len(departments) == len(self.generator.DEPARTMENTS)
        assert "department_id" in departments.columns
        assert "department_name" in departments.columns
        assert "budget" in departments.columns
        
        # Check if all department names are present
        expected_names = [dept["name"] for dept in self.generator.DEPARTMENTS]
        assert set(departments["department_name"]) == set(expected_names)


class TestJobGenerator:
    """Test JobGenerator"""
    
    def setup_method(self):
        self.faker = Faker()
        self.department_gen = DepartmentGenerator(self.faker)
        self.departments_df = self.department_gen.generate_departments()
        self.generator = JobGenerator(self.faker, self.departments_df)
    
    def test_generate_jobs(self):
        """Test job generation"""
        jobs = self.generator.generate_jobs()
        
        assert isinstance(jobs, pd.DataFrame)
        assert len(jobs) == len(self.generator.JOBS)
        assert "job_id" in jobs.columns
        assert "job_title" in jobs.columns
        assert "department_id" in jobs.columns
        assert "min_salary" in jobs.columns
        assert "max_salary" in jobs.columns
        
        # Check salary ranges
        for _, job in jobs.iterrows():
            assert job["min_salary"] <= job["max_salary"]
            assert job["min_salary"] > 0
            assert job["max_salary"] > 0


class TestEmployeeGenerator:
    """Test EmployeeGenerator"""
    
    def setup_method(self):
        self.faker = Faker()
        
        # Setup dependencies
        self.location_gen = LocationGenerator(self.faker)
        self.department_gen = DepartmentGenerator(self.faker)
        self.locations_df = self.location_gen.generate_locations(5)
        self.departments_df = self.department_gen.generate_departments()
        self.jobs_df = JobGenerator(self.faker, self.departments_df).generate_jobs()
        
        self.generator = EmployeeGenerator(self.faker, self.departments_df, self.jobs_df, self.locations_df)
    
    def test_generate_employees(self):
        """Test employee generation"""
        employees = self.generator.generate_employees(10)
        
        assert isinstance(employees, pd.DataFrame)
        assert len(employees) == 10
        assert "employee_id" in employees.columns
        assert "first_name" in employees.columns
        assert "last_name" in employees.columns
        assert "department_id" in employees.columns
        assert "job_id" in employees.columns
        assert "salary" in employees.columns
        
        # Check if all employees have valid job and department assignments
        valid_job_ids = set(self.jobs_df["job_id"])
        valid_dept_ids = set(self.departments_df["department_id"])
        
        assert set(employees["job_id"]).issubset(valid_job_ids)
        assert set(employees["department_id"]).issubset(valid_dept_ids)


class TestProductGenerator:
    """Test ProductGenerator"""
    
    def setup_method(self):
        self.faker = Faker()
        self.generator = ProductGenerator(self.faker)
    
    def test_generate_products(self):
        """Test product generation"""
        products, categories, subcategories, brands = self.generator.generate_products(10)
        
        # Test products
        assert isinstance(products, pd.DataFrame)
        assert len(products) == 10
        assert "product_id" in products.columns
        assert "product_name" in products.columns
        assert "category_id" in products.columns
        assert "unit_price" in products.columns
        assert "cost" in products.columns
        
        # Test categories
        assert isinstance(categories, pd.DataFrame)
        assert len(categories) == len(self.generator.FMCG_CATEGORIES)
        
        # Test subcategories
        assert isinstance(subcategories, pd.DataFrame)
        assert len(subcategories) > 0
        
        # Test brands
        assert isinstance(brands, pd.DataFrame)
        assert len(brands) == len(self.generator.BRANDS)
        
        # Check pricing logic
        for _, product in products.iterrows():
            assert product["unit_price"] > 0
            assert product["cost"] > 0
            assert product["cost"] < product["unit_price"]


class TestRetailerGenerator:
    """Test RetailerGenerator"""
    
    def setup_method(self):
        self.faker = Faker()
        self.location_gen = LocationGenerator(self.faker)
        self.locations_df = self.location_gen.generate_locations(5)
        self.generator = RetailerGenerator(self.faker)
    
    def test_generate_retailers(self):
        """Test retailer generation"""
        retailers = self.generator.generate_retailers(10, self.locations_df)
        
        assert isinstance(retailers, pd.DataFrame)
        assert len(retailers) == 10
        assert "retailer_id" in retailers.columns
        assert "retailer_name" in retailers.columns
        assert "retailer_type" in retailers.columns
        assert "location_id" in retailers.columns
        
        # Check if all retailers have valid location assignments
        valid_location_ids = set(self.locations_df["location_id"])
        assert set(retailers["location_id"]).issubset(valid_location_ids)
        
        # Check retailer types
        valid_types = set(self.generator.RETAILER_TYPES)
        assert set(retailers["retailer_type"]).issubset(valid_types)


class TestCampaignGenerator:
    """Test CampaignGenerator"""
    
    def setup_method(self):
        self.faker = Faker()
        self.generator = CampaignGenerator(self.faker)
    
    def test_generate_campaigns(self):
        """Test campaign generation"""
        campaigns = self.generator.generate_campaigns(10)
        
        assert isinstance(campaigns, pd.DataFrame)
        assert len(campaigns) == 10
        assert "campaign_id" in campaigns.columns
        assert "campaign_name" in campaigns.columns
        assert "campaign_type" in campaigns.columns
        assert "start_date" in campaigns.columns
        assert "end_date" in campaigns.columns
        assert "budget" in campaigns.columns
        
        # Check date logic
        for _, campaign in campaigns.iterrows():
            assert campaign["start_date"] <= campaign["end_date"]
            assert campaign["budget"] > 0
        
        # Check campaign types
        valid_types = set(self.generator.CAMPAIGN_TYPES)
        assert set(campaigns["campaign_type"]).issubset(valid_types)


if __name__ == "__main__":
    pytest.main([__file__])
