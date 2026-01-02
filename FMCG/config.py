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

INITIAL_EMPLOYEES = 200  # Realistic for ₱500M/year FMCG company
INITIAL_PRODUCTS = 150   # More product variety for realistic FMCG
INITIAL_RETAILERS = 500  # Wider distribution network
# Initial: ₱1B total sales over 10 years (~₱100M/year average)
# Scaled to fit within BigQuery free tier (10GB limit) with optimized storage
INITIAL_SALES_AMOUNT = int(os.environ.get("INITIAL_SALES_AMOUNT", "1000000000"))
# Daily: ₱274K daily sales (₱100M/year ÷ 365 days)
DAILY_SALES_AMOUNT = int(os.environ.get("DAILY_SALES_AMOUNT", "274000"))
NEW_PRODUCTS_PER_RUN = random.randint(1, 5)
NEW_HIRES_PER_RUN = random.randint(2, 15)
