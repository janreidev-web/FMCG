#!/usr/bin/env python3
"""
Pre-create all BigQuery tables with correct schema before running the data generator.
This prevents "table not found" errors during scheduled runs.
"""

import os
from google.cloud import bigquery
from datetime import date, timedelta
import pandas as pd

# BigQuery configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID") or os.getenv("BIGQUERY_PROJECT_ID")
DATASET = os.getenv("BIGQUERY_DATASET", "fmcg_analytics")

if not PROJECT_ID:
    raise ValueError("PROJECT_ID not set. Please set GCP_PROJECT_ID or BIGQUERY_PROJECT_ID environment variable.")

# Table names
DIM_LOCATIONS = f"{PROJECT_ID}.{DATASET}.dim_locations"
DIM_DEPARTMENTS = f"{PROJECT_ID}.{DATASET}.dim_departments"
DIM_JOBS = f"{PROJECT_ID}.{DATASET}.dim_jobs"
DIM_BANKS = f"{PROJECT_ID}.{DATASET}.dim_banks"
DIM_INSURANCE = f"{PROJECT_ID}.{DATASET}.dim_insurance"
DIM_CATEGORIES = f"{PROJECT_ID}.{DATASET}.dim_categories"
DIM_BRANDS = f"{PROJECT_ID}.{DATASET}.dim_brands"
DIM_SUBCATEGORIES = f"{PROJECT_ID}.{DATASET}.dim_subcategories"
DIM_PRODUCTS = f"{PROJECT_ID}.{DATASET}.dim_products"
DIM_EMPLOYEES = f"{PROJECT_ID}.{DATASET}.dim_employees"
DIM_RETAILERS = f"{PROJECT_ID}.{DATASET}.dim_retailers"
DIM_CAMPAIGNS = f"{PROJECT_ID}.{DATASET}.dim_campaigns"
DIM_DATES = f"{PROJECT_ID}.{DATASET}.dim_dates"

FACT_EMPLOYEES = f"{PROJECT_ID}.{DATASET}.fact_employees"
FACT_EMPLOYEE_WAGES = f"{PROJECT_ID}.{DATASET}.fact_employee_wages"
FACT_SALES = f"{PROJECT_ID}.{DATASET}.fact_sales"
FACT_INVENTORY = f"{PROJECT_ID}.{DATASET}.fact_inventory"
FACT_OPERATING_COSTS = f"{PROJECT_ID}.{DATASET}.fact_operating_costs"
FACT_MARKETING_COSTS = f"{PROJECT_ID}.{DATASET}.fact_marketing_costs"

def create_table_with_schema(client, table_id, schema_sample_data):
    """Create a BigQuery table with schema based on sample data"""
    try:
        # Check if table exists
        table_ref = client.get_table(table_id)
        print(f"Table {table_ref.table_id} already exists")
        return True
    except:
        # Table doesn't exist, create it
        try:
            sample_df = pd.DataFrame(schema_sample_data)
            job = client.load_table_from_dataframe(sample_df, table_id)
            job.result()  # Wait for the job to complete
            print(f"Created table: {table_id}")
            return True
        except Exception as e:
            print(f"Failed to create table {table_id}: {e}")
            return False

def main():
    """Pre-create all tables with correct schema"""
    print("Pre-creating BigQuery tables...")
    
    # Initialize BigQuery client
    try:
        client = bigquery.Client(project=PROJECT_ID)
        print(f"Connected to BigQuery project: {PROJECT_ID}")
    except Exception as e:
        print(f"Failed to connect to BigQuery: {e}")
        return
    
    # Schema definitions for each tables
    schemas = {
        DIM_LOCATIONS: [{
            "location_id": "LOC000001",
            "city": "Manila",
            "province": "Metro Manila",
            "region": "NCR",
            "country": "Philippines"
        }],
        
        DIM_DEPARTMENTS: [{
            "department_id": "DEPT001",
            "department_name": "Sales",
            "department_code": "SLS"
        }],
        
        DIM_JOBS: [{
            "job_id": "JOB000001",
            "job_title": "Sales Representative",
            "job_level": "L3",
            "department_id": "DEPT001",
            "work_setup": "On-site",
            "work_type": "Full-time",
            "base_salary_min": 15000,
            "base_salary_max": 25000
        }],
        
        DIM_BANKS: [{
            "bank_id": "BANK001",
            "bank_name": "BDO",
            "bank_code": "BDO",
            "branch_code": "001"
        }],
        
        DIM_INSURANCE: [{
            "insurance_id": "INS001",
            "provider_name": "PhilHealth",
            "provider_type": "Health",
            "coverage_level": "Standard"
        }],
        
        DIM_CATEGORIES: [{
            "category_id": "CAT001",
            "category_name": "Beverages",
            "category_code": "BEV"
        }],
        
        DIM_BRANDS: [{
            "brand_id": "BRAND001",
            "brand_name": "Coca-Cola",
            "brand_code": "COKE"
        }],
        
        DIM_SUBCATEGORIES: [{
            "subcategory_id": "SUBCAT001",
            "subcategory_name": "Soft Drinks",
            "subcategory_code": "SD",
            "category_code": "BEV"
        }],
        
        DIM_PRODUCTS: [{
            "product_id": "PROD000001",
            "product_name": "Coca-Cola 1.5L",
            "category_id": "CAT001",
            "brand_id": "BRAND001",
            "subcategory_id": "SUBCAT001",
            "wholesale_price": 45.50,
            "retail_price": 55.00,
            "status": "Active",
            "created_date": date.today()
        }],
        
        DIM_EMPLOYEES: [{
            "employee_id": "EMP000001",
            "first_name": "John",
            "last_name": "Doe",
            "employment_status": "Active",
            "hire_date": date.today(),
            "termination_date": None,
            "tenure_years": 1.5,
            "gender": "Male",
            "birth_date": date(1990, 1, 1),
            "phone": "09123456789",
            "email": "john.doe@company.com",
            "personal_email": "john.doe.personal@gmail.com",
            "tin_number": "123456789",
            "sss_number": "1234567890",
            "philhealth_number": "123456789012",
            "pagibig_number": "1234567890",
            "blood_type": "O+",
            "emergency_contact_name": "Jane Doe",
            "emergency_contact_relation": "Spouse",
            "emergency_contact_phone": "09123456788",
            "location_id": "LOC000001",
            "job_id": "JOB000001",
            "bank_id": "BANK001",
            "insurance_id": "INS001"
        }],
        
        DIM_RETAILERS: [{
            "retailer_id": "RET000001",
            "retailer_name": "SuperMart",
            "retailer_type": "Supermarket",
            "location_id": "LOC000001"
        }],
        
        DIM_CAMPAIGNS: [{
            "campaign_id": "C0001",
            "campaign_name": "Summer Sale 2024",
            "campaign_type": "Seasonal",
            "start_date": date.today(),
            "end_date": date.today() + timedelta(days=30),
            "budget": 1000000,
            "currency": "PHP"
        }],
        
        DIM_DATES: [{
            "date_id": "DATE001",
            "date": date.today(),
            "year": date.today().year,
            "year_month": f"{date.today().year}-{date.today().month:02d}",
            "month": date.today().strftime("%B"),
            "month_number": date.today().month,
            "quarter": f"Q{(date.today().month - 1) // 3 + 1}",
            "quarter_number": (date.today().month - 1) // 3 + 1,
            "day": date.today().day,
            "day_of_week": date.today().strftime("%A"),
            "day_of_week_number": date.today().weekday() + 1,
            "is_weekend": date.today().weekday() >= 5,
            "is_holiday": False,
            "is_workday": date.today().weekday() < 5
        }],
        
        FACT_EMPLOYEES: [{
            "employee_fact_id": "EF001",
            "employee_id": "EMP000001",
            "effective_date": date.today(),
            "performance_rating": 4,
            "last_review_date": date.today(),
            "promotion_eligible": True,
            "years_of_service": 1,
            "attendance_rate": 0.95,
            "overtime_hours_monthly": 5,
            "productivity_score": 85,
            "engagement_score": 8,
            "satisfaction_index": 80,
            "retention_risk_score": 2,
            "training_hours_completed": 20,
            "certifications_count": 2,
            "skill_gap_score": 3,
            "benefit_enrollment_date": date.today(),
            "health_utilization_rate": 0.5,
            "vacation_leave_balance": 10,
            "sick_leave_balance": 8,
            "personal_leave_balance": 3
        }],
        
        FACT_EMPLOYEE_WAGES: [{
            "wage_id": 1,
            "employee_id": "EMP000001",
            "effective_date": date.today(),
            "job_title": "Sales Representative",
            "job_level": "L3",
            "department": "Sales",
            "monthly_salary": 20000,
            "annual_salary": 240000,
            "currency": "PHP",
            "years_of_service": 1,
            "salary_grade": 3
        }],
        
        FACT_SALES: [{
            "sale_id": "SAL000001",
            "sale_date": date.today(),
            "product_id": "PROD000001",
            "retailer_id": "RET000001",
            "case_quantity": 10,
            "unit_price": 55.00,
            "discount_percent": 0.0,
            "discount_amount": 0.0,
            "tax_rate": 0.12,
            "tax_amount": 66.00,
            "total_amount": 616.00,
            "commission_amount": 18.48,
            "currency": "PHP",
            "payment_method": "Cash",
            "payment_status": "Paid",
            "delivery_status": "Pending",
            "expected_delivery_date": date.today() + timedelta(days=1),
            "actual_delivery_date": None
        }],
        
        FACT_INVENTORY: [{
            "inventory_id": "INV000000000000001",
            "inventory_date": date.today(),
            "product_id": "PROD000001",
            "location_id": "LOC000001",
            "cases_on_hand": 100,
            "unit_cost": 45.50,
            "currency": "PHP"
        }],
        
        FACT_OPERATING_COSTS: [{
            "cost_id": 1,
            "cost_date": date.today(),
            "category": "Salaries & Wages",
            "cost_type": "Base Salary",
            "amount": 50000.00,
            "currency": "PHP"
        }],
        
        FACT_MARKETING_COSTS: [{
            "marketing_cost_id": 1,
            "cost_date": date.today(),
            "campaign_id": "C0001",
            "campaign_type": "Seasonal",
            "cost_category": "Digital Advertising",
            "amount": 10000.00,
            "currency": "PHP"
        }]
    }
    
    # Create all tables
    success_count = 0
    total_count = len(schemas)
    
    for table_id, sample_data in schemas.items():
        if create_table_with_schema(client, table_id, sample_data):
            success_count += 1
    
    print(f"\nSummary: {success_count}/{total_count} tables created successfully")
    
    if success_count == total_count:
        print("All tables are ready! You can now run the data generator.")
    else:
        print("Some tables failed to create. Check the errors above.")

if __name__ == "__main__":
    main()
