import os
import random

# ---------------- CONFIG ----------------
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "fmcg-data-simulator")
DATASET = os.environ.get("BQ_DATASET", "fmcg_analytics")

EMPLOYEES_TABLE = f"{PROJECT_ID}.{DATASET}.employees"
PRODUCTS_TABLE = f"{PROJECT_ID}.{DATASET}.products"
RETAILERS_TABLE = f"{PROJECT_ID}.{DATASET}.retailers"
SALES_TABLE = f"{PROJECT_ID}.{DATASET}.sales"
COSTS_TABLE = f"{PROJECT_ID}.{DATASET}.operating_costs"
INVENTORY_TABLE = f"{PROJECT_ID}.{DATASET}.inventory"
MARKETING_TABLE = f"{PROJECT_ID}.{DATASET}.marketing_campaigns"
DATES_TABLE = f"{PROJECT_ID}.{DATASET}.dates"

INITIAL_EMPLOYEES = 350  # Optimized for regional FMCG distributor with ₱8B revenue
INITIAL_PRODUCTS = 150   # More product variety for realistic FMCG
INITIAL_RETAILERS = 500  # Wider distribution network
INITIAL_SALES_AMOUNT = int(os.environ.get("INITIAL_SALES_AMOUNT", "8000000000"))
# Daily: Target ~₱2M for scheduled daily runs (realistic daily operations)
# This is separate from the annual target calculation
DAILY_SALES_AMOUNT = int(os.environ.get("DAILY_SALES_AMOUNT", "2000000"))  # ₱2M daily target

# Monthly update quantities
NEW_EMPLOYEES_PER_MONTH = random.randint(1, 5)  # 1-5 new employees per month
NEW_PRODUCTS_PER_MONTH = random.randint(2, 6)  # 2-6 new products per month

# Quarterly update quantities  
NEW_CAMPAIGNS_PER_QUARTER = 1  # 1 new campaign per quarter

# Dimension table names (for BigQuery)
DIM_EMPLOYEES = f"{PROJECT_ID}.{DATASET}.dim_employees"
DIM_PRODUCTS = f"{PROJECT_ID}.{DATASET}.dim_products"
DIM_RETAILERS = f"{PROJECT_ID}.{DATASET}.dim_retailers"
DIM_CAMPAIGNS = f"{PROJECT_ID}.{DATASET}.dim_campaigns"
DIM_LOCATIONS = f"{PROJECT_ID}.{DATASET}.dim_locations"
DIM_DEPARTMENTS = f"{PROJECT_ID}.{DATASET}.dim_departments"
DIM_JOBS = f"{PROJECT_ID}.{DATASET}.dim_jobs"
DIM_BANKS = f"{PROJECT_ID}.{DATASET}.dim_banks"
DIM_INSURANCE = f"{PROJECT_ID}.{DATASET}.dim_insurance"
DIM_CATEGORIES = f"{PROJECT_ID}.{DATASET}.dim_categories"
DIM_BRANDS = f"{PROJECT_ID}.{DATASET}.dim_brands"
DIM_SUBCATEGORIES = f"{PROJECT_ID}.{DATASET}.dim_subcategories"
DIM_DATES = f"{PROJECT_ID}.{DATASET}.dim_dates"

# Fact table names (for BigQuery)
FACT_SALES = f"{PROJECT_ID}.{DATASET}.fact_sales"
FACT_OPERATING_COSTS = f"{PROJECT_ID}.{DATASET}.fact_operating_costs"
FACT_INVENTORY = f"{PROJECT_ID}.{DATASET}.fact_inventory"
FACT_MARKETING_COSTS = f"{PROJECT_ID}.{DATASET}.fact_marketing_costs"
FACT_EMPLOYEES = f"{PROJECT_ID}.{DATASET}.fact_employees"
