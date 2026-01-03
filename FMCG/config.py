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

INITIAL_EMPLOYEES = 500  # Realistic for ₱6B/year FMCG company
INITIAL_PRODUCTS = 150   # More product variety for realistic FMCG
INITIAL_RETAILERS = 500  # Wider distribution network
# Initial: ₱6B total sales over 10 years (~₱600M/year average)
# Adjusted for realistic FMCG scale: ₱60B total over 10 years (~₱6B/year average)
# This will generate realistic yearly sales from ₱3B to ₱8B
INITIAL_SALES_AMOUNT = int(os.environ.get("INITIAL_SALES_AMOUNT", "60000000000"))
# Daily: ₱16.44M daily sales (₱6B/year ÷ 365 days) - scaled for realistic FMCG
DAILY_SALES_AMOUNT = int(os.environ.get("DAILY_SALES_AMOUNT", "16440000"))
NEW_PRODUCTS_PER_RUN = random.randint(1, 5)
NEW_HIRES_PER_RUN = random.randint(2, 15)
