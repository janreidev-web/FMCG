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
# Daily: ₱21.92M daily sales (₱8B/year ÷ 365 days ≈ ₱21.92M/day) - optimized for 15-20% profit margin
# Note: Daily amount is for ongoing operations, scaled proportionally to initial amount
DAILY_SALES_AMOUNT = int(os.environ.get("DAILY_SALES_AMOUNT", "2192000"))
NEW_PRODUCTS_PER_RUN = random.randint(1, 5)
NEW_HIRES_PER_RUN = random.randint(2, 12)

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
