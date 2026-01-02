"""
Dimensional Model Schema for FMCG Data Analytics
Optimized for BigQuery with reduced storage requirements
"""

from config import PROJECT_ID, DATASET

# DIMENSION TABLES (small, descriptive data)
DIM_PRODUCTS = f"{PROJECT_ID}.{DATASET}.dim_products"
DIM_EMPLOYEES = f"{PROJECT_ID}.{DATASET}.dim_employees"
DIM_RETAILERS = f"{PROJECT_ID}.{DATASET}.dim_retailers"
DIM_CAMPAIGNS = f"{PROJECT_ID}.{DATASET}.dim_campaigns"

# FACT TABLES (large, transactional data)
FACT_SALES = f"{PROJECT_ID}.{DATASET}.fact_sales"
FACT_OPERATING_COSTS = f"{PROJECT_ID}.{DATASET}.fact_operating_costs"
FACT_INVENTORY = f"{PROJECT_ID}.{DATASET}.fact_inventory"
FACT_MARKETING_COSTS = f"{PROJECT_ID}.{DATASET}.fact_marketing_costs"

# Table schemas for BigQuery
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

DIM_EMPLOYEES_SCHEMA = [
    {"name": "employee_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "employee_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "full_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "department", "type": "STRING", "mode": "REQUIRED"},
    {"name": "position", "type": "STRING", "mode": "REQUIRED"},
    {"name": "employment_status", "type": "STRING", "mode": "REQUIRED"},
    {"name": "hire_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "termination_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "gender", "type": "STRING", "mode": "REQUIRED"},
    {"name": "birth_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "age", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "work_setup", "type": "STRING", "mode": "REQUIRED"},
    {"name": "work_type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "monthly_salary", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "address_street", "type": "STRING", "mode": "REQUIRED"},
    {"name": "address_city", "type": "STRING", "mode": "REQUIRED"},
    {"name": "address_province", "type": "STRING", "mode": "REQUIRED"},
    {"name": "address_region", "type": "STRING", "mode": "REQUIRED"},
    {"name": "address_postal_code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "address_country", "type": "STRING", "mode": "REQUIRED"},
    {"name": "phone", "type": "STRING", "mode": "REQUIRED"},
    {"name": "email", "type": "STRING", "mode": "REQUIRED"},
    {"name": "personal_email", "type": "STRING", "mode": "REQUIRED"},
    {"name": "tin_number", "type": "STRING", "mode": "REQUIRED"},
    {"name": "sss_number", "type": "STRING", "mode": "REQUIRED"},
    {"name": "philhealth_number", "type": "STRING", "mode": "REQUIRED"},
    {"name": "pagibig_number", "type": "STRING", "mode": "REQUIRED"},
    {"name": "blood_type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "bank_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "account_number", "type": "STRING", "mode": "REQUIRED"},
    {"name": "account_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "performance_rating", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "last_review_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "training_completed", "type": "STRING", "mode": "NULLABLE"},
    {"name": "skills", "type": "STRING", "mode": "REQUIRED"},
    {"name": "health_insurance_provider", "type": "STRING", "mode": "REQUIRED"},
    {"name": "benefit_enrollment_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "years_of_service", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "attendance_rate", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "overtime_hours_monthly", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "engagement_score", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "satisfaction_index", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "vacation_leave_balance", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "sick_leave_balance", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "personal_leave_balance", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "emergency_contact_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "emergency_contact_relation", "type": "STRING", "mode": "REQUIRED"},
    {"name": "emergency_contact_phone", "type": "STRING", "mode": "REQUIRED"},
]

DIM_RETAILERS_SCHEMA = [
    {"name": "retailer_key", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "retailer_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "retailer_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "retailer_type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "city", "type": "STRING", "mode": "REQUIRED"},
    {"name": "province", "type": "STRING", "mode": "REQUIRED"},
    {"name": "region", "type": "STRING", "mode": "REQUIRED"},
    {"name": "country", "type": "STRING", "mode": "REQUIRED"},
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

# Fact tables (minimal columns, foreign keys to dimensions)
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
    {"name": "warehouse_location", "type": "STRING", "mode": "REQUIRED"},
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

# Storage estimates (rough calculations)
STORAGE_ESTIMATES = {
    "dim_products": "150 rows × ~200 bytes = ~30 KB",
    "dim_employees": "200 rows × ~800 bytes = ~160 KB",
    "dim_retailers": "500 rows × ~150 bytes = ~75 KB",
    "dim_campaigns": "50 rows × ~150 bytes = ~7.5 KB",
    "fact_sales": "3.6M rows × ~300 bytes = ~1.08 GB",
    "fact_operating_costs": "2,400 rows × ~150 bytes = ~360 KB",
    "fact_marketing_costs": "3,000 rows × ~180 bytes = ~540 KB",
    "fact_inventory": "750 rows × ~150 bytes = ~112.5 KB",
    "total": "~1.29 GB (well under 10GB limit)"
}
