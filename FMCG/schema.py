"""
Normalized Dimensional Model Schema for FMCG Data Analytics
Reduces data redundancy by normalizing large dimensions
"""

from config import PROJECT_ID, DATASET

# ORIGINAL DIMENSION TABLES (unchanged)
DIM_PRODUCTS = f"{PROJECT_ID}.{DATASET}.dim_products"
DIM_RETAILERS = f"{PROJECT_ID}.{DATASET}.dim_retailers"
DIM_CAMPAIGNS = f"{PROJECT_ID}.{DATASET}.dim_campaigns"

# NORMALIZED DIMENSION TABLES
# Core employee dimension - reduced to essential fields
DIM_EMPLOYEES = f"{PROJECT_ID}.{DATASET}.dim_employees"
DIM_LOCATIONS = f"{PROJECT_ID}.{DATASET}.dim_locations"
DIM_JOBS = f"{PROJECT_ID}.{DATASET}.dim_jobs"
DIM_DEPARTMENTS = f"{PROJECT_ID}.{DATASET}.dim_departments"
DIM_BANKS = f"{PROJECT_ID}.{DATASET}.dim_banks"
DIM_INSURANCE = f"{PROJECT_ID}.{DATASET}.dim_insurance"

# FACT TABLES
FACT_SALES = f"{PROJECT_ID}.{DATASET}.fact_sales"
FACT_OPERATING_COSTS = f"{PROJECT_ID}.{DATASET}.fact_operating_costs"
FACT_INVENTORY = f"{PROJECT_ID}.{DATASET}.fact_inventory"
FACT_MARKETING_COSTS = f"{PROJECT_ID}.{DATASET}.fact_marketing_costs"
FACT_EMPLOYEES = f"{PROJECT_ID}.{DATASET}.fact_employees"  # New employee fact table
FACT_EMPLOYEE_WAGES = f"{PROJECT_ID}.{DATASET}.fact_employee_wages"  # Employee wage history table

# NORMALIZED SCHEMA DEFINITIONS

# Core employee dimension - only essential personal info
DIM_EMPLOYEES_SCHEMA = [
    {"name": "employee_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "employee_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "first_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "last_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "gender", "type": "STRING", "mode": "REQUIRED"},
    {"name": "birth_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "hire_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "termination_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "employment_status", "type": "STRING", "mode": "REQUIRED"},
    {"name": "location_key", "type": "INTEGER", "mode": "REQUIRED"},  # FK to dim_locations
    {"name": "job_key", "type": "INTEGER", "mode": "REQUIRED"},       # FK to dim_jobs
    {"name": "bank_key", "type": "INTEGER", "mode": "REQUIRED"},      # FK to dim_banks
    {"name": "insurance_key", "type": "INTEGER", "mode": "REQUIRED"}, # FK to dim_insurance
    {"name": "tin_number", "type": "STRING", "mode": "REQUIRED"},
    {"name": "sss_number", "type": "STRING", "mode": "REQUIRED"},
    {"name": "philhealth_number", "type": "STRING", "mode": "REQUIRED"},
    {"name": "pagibig_number", "type": "STRING", "mode": "REQUIRED"},
    {"name": "blood_type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "phone", "type": "STRING", "mode": "REQUIRED"},
    {"name": "email", "type": "STRING", "mode": "REQUIRED"},
    {"name": "personal_email", "type": "STRING", "mode": "REQUIRED"},
    {"name": "emergency_contact_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "emergency_contact_relation", "type": "STRING", "mode": "REQUIRED"},
    {"name": "emergency_contact_phone", "type": "STRING", "mode": "REQUIRED"},
]

# Location dimension - normalized address information
DIM_LOCATIONS_SCHEMA = [
    {"name": "location_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "city", "type": "STRING", "mode": "REQUIRED"},
    {"name": "province", "type": "STRING", "mode": "REQUIRED"},
    {"name": "region", "type": "STRING", "mode": "REQUIRED"},
    {"name": "postal_code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "country", "type": "STRING", "mode": "REQUIRED"},
]

# Job dimension - normalized job information
DIM_JOBS_SCHEMA = [
    {"name": "job_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "job_title", "type": "STRING", "mode": "REQUIRED"},
    {"name": "job_level", "type": "STRING", "mode": "REQUIRED"},  # Entry, Junior, Senior, Manager, Director
    {"name": "department_key", "type": "INTEGER", "mode": "REQUIRED"},  # FK to dim_departments
    {"name": "work_setup", "type": "STRING", "mode": "REQUIRED"},  # On-site, Remote, Hybrid, Field-based
    {"name": "work_type", "type": "STRING", "mode": "REQUIRED"},   # Full-time, Part-time, Contract, Intern
    {"name": "base_salary_min", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "base_salary_max", "type": "INTEGER", "mode": "REQUIRED"},
]

# Department dimension
DIM_DEPARTMENTS_SCHEMA = [
    {"name": "department_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "department_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "department_code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "parent_department_key", "type": "INTEGER", "mode": "NULLABLE"},  # For hierarchical departments
]

# Bank dimension
DIM_BANKS_SCHEMA = [
    {"name": "bank_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "bank_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "bank_code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "branch_code", "type": "STRING", "mode": "NULLABLE"},
]

# Insurance dimension
DIM_INSURANCE_SCHEMA = [
    {"name": "insurance_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "provider_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "provider_type", "type": "STRING", "mode": "REQUIRED"},  # Health, Life, Dental, Vision
    {"name": "coverage_level", "type": "STRING", "mode": "REQUIRED"},  # Basic, Standard, Premium
]

# Employee fact table - time-varying employee metrics (simplified)
FACT_EMPLOYEES_SCHEMA = [
    {"name": "employee_fact_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "employee_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "effective_date", "type": "DATE", "mode": "REQUIRED"},
    
    # Performance metrics
    {"name": "performance_rating", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "last_review_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "promotion_eligible", "type": "BOOLEAN", "mode": "REQUIRED"},
    
    # Work metrics
    {"name": "years_of_service", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "attendance_rate", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "overtime_hours_monthly", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "productivity_score", "type": "INTEGER", "mode": "NULLABLE"},
    
    # Engagement metrics
    {"name": "engagement_score", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "satisfaction_index", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "retention_risk_score", "type": "INTEGER", "mode": "NULLABLE"},
    
    # Development metrics
    {"name": "training_hours_completed", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "certifications_count", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "skill_gap_score", "type": "INTEGER", "mode": "NULLABLE"},
    
    # Benefits metrics
    {"name": "benefit_enrollment_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "health_utilization_rate", "type": "FLOAT", "mode": "NULLABLE"},
    
    # Leave metrics
    {"name": "vacation_leave_balance", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "sick_leave_balance", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "personal_leave_balance", "type": "INTEGER", "mode": "REQUIRED"},
]

# Employee wage history table - dated compensation records
FACT_EMPLOYEE_WAGES_SCHEMA = [
    {"name": "wage_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "employee_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "effective_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "job_title", "type": "STRING", "mode": "REQUIRED"},
    {"name": "job_level", "type": "STRING", "mode": "REQUIRED"},
    {"name": "department", "type": "STRING", "mode": "REQUIRED"},
    {"name": "monthly_salary", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "annual_salary", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
    {"name": "years_of_service", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "salary_grade", "type": "INTEGER", "mode": "REQUIRED"},
]

# Existing schemas (unchanged)
DIM_PRODUCTS_SCHEMA = [
    {"name": "product_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "product_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "category", "type": "STRING", "mode": "REQUIRED"},
    {"name": "subcategory", "type": "STRING", "mode": "REQUIRED"},
    {"name": "brand", "type": "STRING", "mode": "REQUIRED"},
    {"name": "wholesale_price", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "retail_price", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "status", "type": "STRING", "mode": "REQUIRED"},
    {"name": "created_date", "type": "DATE", "mode": "REQUIRED"},
]

DIM_RETAILERS_SCHEMA = [
    {"name": "retailer_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "retailer_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "retailer_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "retailer_type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "location_key", "type": "INTEGER", "mode": "REQUIRED"},  # FK to dim_locations
]

DIM_CAMPAIGNS_SCHEMA = [
    {"name": "campaign_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "campaign_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "campaign_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "campaign_type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "start_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "end_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "budget", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
]

FACT_SALES_SCHEMA = [
    {"name": "sale_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "sale_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "product_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "employee_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "retailer_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "campaign_key", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "case_quantity", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "unit_price", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "discount_percent", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "tax_rate", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "total_amount", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "commission_amount", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
    {"name": "payment_method", "type": "STRING", "mode": "REQUIRED"},
    {"name": "payment_status", "type": "STRING", "mode": "REQUIRED"},
    {"name": "delivery_status", "type": "STRING", "mode": "REQUIRED"},
    {"name": "expected_delivery_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "actual_delivery_date", "type": "DATE", "mode": "NULLABLE"},
]

DELIVERY_STATUS_UPDATES_SCHEMA = [
    {"name": "update_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "sale_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "previous_status", "type": "STRING", "mode": "REQUIRED"},
    {"name": "new_status", "type": "STRING", "mode": "REQUIRED"},
    {"name": "previous_actual_delivery_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "new_actual_delivery_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "update_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "days_since_sale", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "update_reason", "type": "STRING", "mode": "REQUIRED"}
]

DELIVERY_UPDATES_STAGING_SCHEMA = DELIVERY_STATUS_UPDATES_SCHEMA  # Same schema for staging table

FACT_OPERATING_COSTS_SCHEMA = [
    {"name": "cost_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "cost_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "category", "type": "STRING", "mode": "REQUIRED"},
    {"name": "cost_type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "amount", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
]

FACT_INVENTORY_SCHEMA = [
    {"name": "inventory_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "inventory_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "product_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "location_key", "type": "INTEGER", "mode": "REQUIRED"},  # FK to dim_locations
    {"name": "cases_on_hand", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "unit_cost", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
]

FACT_MARKETING_COSTS_SCHEMA = [
    {"name": "marketing_cost_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "cost_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "campaign_key", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "campaign_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "campaign_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "cost_category", "type": "STRING", "mode": "REQUIRED"},
    {"name": "amount", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
]

# Storage estimates for normalized model
STORAGE_ESTIMATES = {
    "dim_products": "150 rows × ~200 bytes = ~30 KB",
    "dim_employees": "900 rows × ~400 bytes = ~360 KB (reduced from 720 KB)",
    "dim_locations": "500 rows × ~150 bytes = ~75 KB",
    "dim_jobs": "50 rows × ~120 bytes = ~6 KB",
    "dim_departments": "15 rows × ~80 bytes = ~1.2 KB",
    "dim_banks": "20 rows × ~80 bytes = ~1.6 KB",
    "dim_insurance": "10 rows × ~80 bytes = ~0.8 KB",
    "dim_retailers": "500 rows × ~120 bytes = ~60 KB (reduced from 75 KB)",
    "dim_campaigns": "50 rows × ~150 bytes = ~7.5 KB",
    "fact_employees": "900 rows × ~300 bytes = ~270 KB",
    "fact_sales": "3.6M rows × ~300 bytes = ~1.08 GB",
    "fact_operating_costs": "2,400 rows × ~150 bytes = ~360 KB",
    "fact_marketing_costs": "3,000 rows × ~180 bytes = ~540 KB",
    "fact_inventory": "750 rows × ~150 bytes = ~112.5 KB",
    "total": "~1.28 GB (reduced from ~1.29 GB, better normalization)",
}

# Normalization benefits
NORMALIZATION_BENEFITS = {
    "reduced_redundancy": "Address data stored once in dim_locations instead of repeated in dim_employees and dim_retailers",
    "better_maintainability": "Job titles and departments managed centrally",
    "improved_data_quality": "Consistent categorization through lookup tables",
    "enhanced_analytics": "Better dimensional analysis with proper granularity",
    "storage_efficiency": "Reduced storage through elimination of duplicate data",
    "scalability": "Easier to add new locations, jobs, departments without schema changes",
}

# Migration strategy
MIGRATION_STRATEGY = """
1. Create new normalized dimension tables (dim_locations, dim_jobs, dim_departments, dim_banks, dim_insurance)
2. Populate new dimensions with unique values from existing dim_employees and dim_retailers
3. Create fact_employees table with time-varying employee metrics
4. Update dim_employees to reference new dimension keys
5. Update dim_retailers to reference dim_locations
6. Update fact_inventory to reference dim_locations
7. Validate data integrity and relationships
8. Drop redundant columns from original tables
"""
