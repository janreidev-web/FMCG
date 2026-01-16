"""
ETL pipeline for FMCG Data Analytics Platform
"""

import random
import os
import pandas as pd
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional
from faker import Faker

from ..data.schemas import ALL_SCHEMAS, get_bigquery_schema
from ..utils.bigquery_client import BigQueryManager
from ..utils.logger import default_logger
from ..utils.id_generation import IDGenerator
from ..core.generators import (
    LocationGenerator, DepartmentGenerator, JobGenerator, EmployeeGenerator,
    ProductGenerator, RetailerGenerator, CampaignGenerator, BankGenerator, InsuranceGenerator
)


class ETLPipeline:
    """Main ETL pipeline for data generation and loading"""
    
    def __init__(self, bq_manager=None):
        self.logger = default_logger
        
        if bq_manager:
            self.bigquery_client = bq_manager
        else:
            # Try to get from environment or use defaults
            project_id = os.getenv('GCP_PROJECT_ID', 'fmcg-data-generator')
            dataset = os.getenv('GCP_DATASET', 'fmcg_warehouse')
            credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            self.bigquery_client = BigQueryManager(project_id, dataset, credentials_path)
        
        # Initialize faker for data generation
        self.faker = Faker('en_PH')
        
        # Initialize generators
        self.location_gen = LocationGenerator(self.faker)
        self.department_gen = DepartmentGenerator(self.faker)
        self.bank_gen = BankGenerator(self.faker)
        self.insurance_gen = InsuranceGenerator(self.faker)
        self.id_generator = IDGenerator()
        
        # Will be initialized after dependencies are created
        self.job_gen = None
        self.employee_gen = None
        self.product_gen = None
        self.retailer_gen = None
        self.campaign_gen = None
        
        # Data storage
        self.data_cache = {}
        
        # Retailer-specific transaction ranges (in PHP) - scaled for ₱20B/11years target
        self.retailer_transaction_ranges = {
            "Sari-Sari Store": {"min_qty": 10, "max_qty": 30, "min_amount": 100, "max_amount": 3000, "daily_transactions": (10, 30)},
            "Convenience Store": {"min_qty": 15, "max_qty": 100, "min_amount": 3000, "max_amount": 15000, "daily_transactions": (5, 15)},
            "Pharmacy": {"min_qty": 25, "max_qty": 40, "min_amount": 15000, "max_amount": 25000, "daily_transactions": (3, 8)},
            "Wholesale": {"min_qty": 50, "max_qty": 500, "min_amount": 25000, "max_amount": 100000, "daily_transactions": (2, 6)},
            "Supermarket": {"min_qty": 20, "max_qty": 200, "min_amount": 50000, "max_amount": 75000, "daily_transactions": (3, 10)},
            "Department Store": {"min_qty": 3, "max_qty": 30, "min_amount": 3000, "max_amount": 8000, "daily_transactions": (1, 4)}
        }
    
    def get_retailer_transaction_params(self, retailer_type: str) -> dict:
        """Get transaction parameters based on retailer type"""
        return self.retailer_transaction_ranges.get(retailer_type, self.retailer_transaction_ranges["Convenience Store"])
    
    def setup_database(self) -> None:
        """Set up BigQuery dataset and tables"""
        self.logger.info("Setting up database schema...")
        
        # Ensure dataset exists
        self.bigquery_client.ensure_dataset()
        
        # Create all tables
        for table_name, schema in ALL_SCHEMAS.items():
            bq_schema = get_bigquery_schema(schema)
            self.bigquery_client.create_table(table_name, bq_schema)
        
        self.logger.info("Database setup completed")
    
    def generate_dimension_data(self, config: Dict[str, Any]) -> None:
        """Generate all dimension tables"""
        self.logger.info("Generating dimension data...")
        
        # Generate locations
        locations_count = config.get("locations_count", 100)
        locations_df = self.location_gen.generate_locations(locations_count)
        self.data_cache["dim_locations"] = locations_df
        
        # Generate departments
        departments_df = self.department_gen.generate_departments()
        self.data_cache["dim_departments"] = departments_df
        
        # Generate banks (needed for employees)
        banks_df = self.bank_gen.generate_banks(15)
        self.data_cache["dim_banks"] = banks_df
        
        # Generate insurance (needed for employees)
        insurance_df = self.insurance_gen.generate_insurance(12)
        self.data_cache["dim_insurance"] = insurance_df
        
        # Generate jobs (depends on departments)
        self.job_gen = JobGenerator(self.faker, departments_df)
        jobs_df = self.job_gen.generate_jobs()
        self.data_cache["dim_jobs"] = jobs_df
        
        # Generate employees (depends on departments, jobs, locations, banks, insurance)
        employee_count = config.get("initial_employees", 350)
        self.employee_gen = EmployeeGenerator(self.faker, departments_df, jobs_df, locations_df)
        employees_df = self.employee_gen.generate_employees(employee_count)
        
        self.data_cache["dim_employees"] = employees_df
        
        # Generate products and related dimensions
        product_count = config.get("initial_products", 150)
        self.product_gen = ProductGenerator(self.faker)
        products_df, categories_df, subcategories_df, brands_df = self.product_gen.generate_products(product_count)
        
        self.data_cache["dim_products"] = products_df
        self.data_cache["dim_categories"] = categories_df
        self.data_cache["dim_subcategories"] = subcategories_df
        self.data_cache["dim_brands"] = brands_df
        
        # Generate retailers (depends on locations)
        retailer_count = config.get("initial_retailers", 500)
        self.retailer_gen = RetailerGenerator(self.faker)
        retailers_df = self.retailer_gen.generate_retailers(retailer_count, locations_df)
        self.data_cache["dim_retailers"] = retailers_df
        
        # Generate campaigns
        campaign_count = config.get("initial_campaigns", 50)
        self.campaign_gen = CampaignGenerator(self.faker)
        campaigns_df = self.campaign_gen.generate_campaigns(campaign_count)
        self.data_cache["dim_campaigns"] = campaigns_df
        
        self.logger.info("Dimension data generation completed")
    
    def load_dimension_data(self) -> None:
        """Load dimension data into BigQuery"""
        self.logger.info("Loading dimension data into BigQuery...")
        
        for table_name, df in self.data_cache.items():
            if table_name.startswith("dim_"):
                self.bigquery_client.load_dataframe(df, table_name)
                self.logger.info(f"Loaded {len(df)} rows into {table_name}")
        
        self.logger.info("Dimension data loading completed")
    
    def generate_fact_data(self, config: Dict[str, Any]) -> None:
        """Generate fact table data"""
        self.logger.info("Generating fact data...")
        
        # Generate sales data
        sales_df = self._generate_sales_data(config)
        self.data_cache["fact_sales"] = sales_df
        
        # Generate inventory data
        inventory_df = self._generate_inventory_data(config)
        self.data_cache["fact_inventory"] = inventory_df
        
        # Generate operating costs
        costs_df = self._generate_operating_costs(config)
        self.data_cache["fact_operating_costs"] = costs_df
        
        # Generate marketing costs
        marketing_df = self._generate_marketing_costs(config)
        self.data_cache["fact_marketing_costs"] = marketing_df
        
        # Generate employee facts
        employee_facts_df = self._generate_employee_facts(config)
        self.data_cache["fact_employees"] = employee_facts_df
        
        self.logger.info("Fact data generation completed")
    
    def _generate_sales_data(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate sales transaction data - full 500K target"""
        initial_amount = config.get("initial_sales_amount", 8000000000)
        daily_amount = config.get("daily_sales_amount", 2000000)
        
        # Get reference data
        products = self.data_cache["dim_products"]
        retailers = self.data_cache["dim_retailers"]
        employees = self.data_cache["dim_employees"]
        campaigns = self.data_cache["dim_campaigns"]
        
        # Calculate target transactions for 11 years (increased to 500K)
        target_transactions = 500000  # Increased from 100K to 500K
        
        # Set exact date range: January 1, 2015 to day before yesterday
        start_date = datetime(2015, 1, 1)
        end_date = datetime.now() - timedelta(days=2)  # Day before yesterday
        total_days = (end_date - start_date).days + 1  # Include both start and end dates
        
        # Calculate daily transaction targets
        base_daily_transactions = target_transactions // total_days
        
        # Add some variation (±20%)
        min_daily_tx = max(1, int(base_daily_transactions * 0.8))
        max_daily_tx = max(min_daily_tx + 1, int(base_daily_transactions * 1.2))
        
        self.logger.info(f"Target: {target_transactions:,} transactions")
        self.logger.info(f"Date range: {start_date.date()} to {end_date.date()} ({total_days} days)")
        self.logger.info(f"Daily range: {min_daily_tx}-{max_daily_tx} transactions")
        self.logger.info(f"Expected annual: {target_transactions // (total_days/365):,.0f} transactions")
        
        # Generate all sales in one go
        sales = []
        sale_id = 1
        total_generated = 0
        
        current_date = start_date
        today = datetime.now().date()
        jan_2026 = datetime(2026, 1, 1).date()
        
        while current_date <= end_date:
            # COVID-19 impact factor based on date
            order_date = current_date.date()
            
            # Pre-pandemic: Jan 2015 - Feb 2020 (normal)
            if order_date < date(2020, 3, 1):
                covid_impact = 1.0
            # Pandemic period: Mar 2020 - Dec 2021 (reduced revenue)
            elif order_date < date(2022, 1, 1):
                # Severe impact in early months (Mar-Jun 2020): -40% to -50%
                if order_date < date(2020, 7, 1):
                    covid_impact = random.uniform(0.50, 0.60)
                # Moderate impact (Jul 2020 - Dec 2021): -20% to -30%
                else:
                    covid_impact = random.uniform(0.70, 0.80)
            # Post-pandemic recovery: Jan 2022 - Jun 2023 (gradual recovery)
            elif order_date < date(2023, 7, 1):
                # Recovery phase: -10% to +5%
                covid_impact = random.uniform(0.90, 1.05)
            # New normal: Jul 2023 onwards (back to normal with slight growth)
            else:
                covid_impact = random.uniform(1.0, 1.10)
            
            # Calculate daily transactions with variation and COVID impact
            daily_tx = int(random.randint(min_daily_tx, max_daily_tx) * covid_impact)
            daily_tx = max(1, daily_tx)  # Ensure at least 1 transaction
            
            # Generate all transactions for this day
            for _ in range(daily_tx):
                product = products.sample(1).iloc[0]
                retailer = retailers.sample(1).iloc[0]
                employee = employees.sample(1).iloc[0]
                
                # Get retailer-specific transaction parameters
                retailer_params = self.get_retailer_transaction_params(retailer["retailer_type"])
                
                # Random campaign assignment (30% chance)
                campaign = None
                if random.random() < 0.3 and len(campaigns) > 0:
                    campaign_sample = campaigns.sample(1)
                    if len(campaign_sample) > 0:
                        campaign = campaign_sample.iloc[0]
                
                # Generate quantity and amount based on retailer type
                quantity = random.randint(retailer_params["min_qty"], retailer_params["max_qty"])
                
                # Apply COVID impact to quantity as well
                quantity = max(1, int(quantity * covid_impact))
                
                # Apply price fluctuations (Philippine economic scenario)
                base_price = float(product["unit_price"])
                
                # Calculate months since start for price trends
                months_since_start = ((current_date.year - start_date.year) * 12 + 
                                     (current_date.month - start_date.month))
                
                # Price fluctuation factors based on Philippine economic conditions:
                
                # 1. Philippine inflation trend (based on PSA actual data)
                if order_date < date(2017, 1, 1):
                    # 2015-2016: Very low inflation (~1.5%)
                    annual_inflation = 0.015
                elif order_date < date(2018, 1, 1):
                    # 2017: Low inflation (~3%)
                    annual_inflation = 0.030
                elif order_date < date(2019, 1, 1):
                    # 2018: TRAIN Law spike - peak ~6.5%
                    annual_inflation = 0.065
                elif order_date < date(2020, 1, 1):
                    # 2019: Moderate (~3%)
                    annual_inflation = 0.030
                elif order_date < date(2021, 1, 1):
                    # 2020: Pandemic - low (~2.5%)
                    annual_inflation = 0.025
                elif order_date < date(2022, 1, 1):
                    # 2021: Recovery (~4%)
                    annual_inflation = 0.040
                elif order_date < date(2023, 1, 1):
                    # 2022: High inflation spike (~6%)
                    annual_inflation = 0.060
                elif order_date < date(2024, 1, 1):
                    # 2023: Peak inflation (~8%)
                    annual_inflation = 0.080
                elif order_date < date(2025, 1, 1):
                    # 2024: Moderating (~4%)
                    annual_inflation = 0.040
                else:
                    # 2025+: Stabilizing (~2.5%)
                    annual_inflation = 0.025
                
                price_inflation = 1 + (annual_inflation * months_since_start / 12)
                
                # 2. TRAIN Law excise tax impact (Jan 2018)
                if order_date >= date(2018, 1, 1):
                    # TRAIN Law added excise taxes on sweetened beverages, fuel, etc.
                    # Affects certain product categories more
                    train_law_impact = random.uniform(1.02, 1.08)  # 2-8% price increase
                else:
                    train_law_impact = 1.0
                
                # 3. Competitive pressure (±8% random variation)
                competitive_pressure = 1 + random.uniform(-0.08, 0.08)
                
                # 4. Demand fluctuation (±6% based on seasonality)
                # Higher during Christmas (Oct-Dec), lower during lean months (Jun-Aug)
                if order_date.month in [10, 11, 12]:
                    demand_factor = 1 + random.uniform(0.02, 0.06)  # Ber months
                elif order_date.month in [6, 7, 8]:
                    demand_factor = 1 + random.uniform(-0.06, -0.02)  # Lean months
                else:
                    demand_factor = 1 + random.uniform(-0.04, 0.04)
                
                # 5. COVID pricing impact (Philippine scenario)
                if order_date < date(2020, 3, 1):
                    # Pre-pandemic: normal pricing
                    covid_price_factor = 1.0
                elif order_date < date(2020, 6, 1):
                    # ECQ period (Mar-May 2020): supply chain disruption
                    covid_price_factor = random.uniform(1.08, 1.18)
                elif order_date < date(2021, 4, 1):
                    # GCQ/MGCQ (Jun 2020-Mar 2021): stabilizing
                    covid_price_factor = random.uniform(1.03, 1.10)
                elif order_date < date(2022, 3, 1):
                    # Various lockdowns (Apr 2021-Feb 2022): moderate impact
                    covid_price_factor = random.uniform(1.02, 1.06)
                else:
                    # Alert levels/Endemic (Mar 2022+): normalizing
                    covid_price_factor = random.uniform(0.99, 1.03)
                
                # Apply all price factors
                unit_price = base_price * price_inflation * train_law_impact * competitive_pressure * demand_factor * covid_price_factor
                total_amount = quantity * unit_price
                
                # Ensure transaction is within retailer's expected range
                if total_amount > retailer_params["max_amount"]:
                    quantity = max(1, int(retailer_params["max_amount"] / unit_price))
                    total_amount = quantity * unit_price
                elif total_amount < retailer_params["min_amount"]:
                    quantity = min(retailer_params["max_qty"], max(1, int(retailer_params["min_amount"] / unit_price)))
                    total_amount = quantity * unit_price
                
                # Calculate discount and commission
                discount_rate = random.uniform(0.05, 0.15) if campaign is not None else 0
                commission_rate = random.uniform(0.02, 0.08)
                
                final_amount = total_amount * (1 - discount_rate)
                commission_amount = final_amount * commission_rate
                
                # Determine delivery status based on date
                order_date = current_date.date()
                if order_date <= datetime(2025, 12, 31).date():
                    # Orders from 2015-2025 are already delivered
                    delivery_status = "Delivered"
                    delivery_date = order_date + timedelta(days=random.randint(1, 14))
                elif order_date >= jan_2026 and order_date <= today - timedelta(days=3):
                    # Recent orders from 2026 (but not too recent) are shipped or delivered
                    delivery_status = random.choice(["Shipped", "Delivered"])
                    if delivery_status == "Delivered":
                        delivery_date = order_date + timedelta(days=random.randint(1, 7))
                    else:  # Shipped items don't have delivery date yet
                        delivery_date = None
                else:
                    # Very recent orders (first week of January 2026) are pending or shipped
                    delivery_status = random.choice(["Pending", "Shipped"])
                    delivery_date = None  # Neither pending nor shipped have delivery dates
                
                sale = {
                    "sale_id": self.id_generator.generate_id('fact_sales'),
                    "date": order_date,
                    "product_id": product["product_id"],
                    "retailer_id": retailer["retailer_id"],
                    "employee_id": employee["employee_id"],
                    "campaign_id": campaign["campaign_id"] if campaign is not None else None,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_amount": total_amount,
                    "discount_rate": discount_rate,
                    "discount_amount": total_amount * discount_rate,
                    "final_amount": final_amount,
                    "commission_rate": commission_rate,
                    "commission_amount": commission_amount,
                    "order_date": order_date,
                    "delivery_date": delivery_date,
                    "delivery_status": delivery_status,
                    "created_at": current_date
                }
                sales.append(sale)
                sale_id += 1
                total_generated += 1
            
            current_date += timedelta(days=1)
        
        # Convert to DataFrame
        sales_df = pd.DataFrame(sales)
        
        # Log final results
        self.logger.info(f"Generated {len(sales_df):,} sales transactions")
        self.logger.info(f"Date range: {sales_df['order_date'].min()} to {sales_df['order_date'].max()}")
        self.logger.info(f"Total sales value: ₱{sales_df['final_amount'].sum():,.0f}")
        
        # Estimate storage size (rough calculation: ~1KB per row)
        estimated_storage_mb = len(sales_df) * 1024 / (1024 * 1024)
        self.logger.info(f"Estimated storage: {estimated_storage_mb:.1f} MB for sales data")
        
        # Check storage requirements
        if estimated_storage_mb < 10240:  # 10GB in MB
            self.logger.info("✅ Within free tier storage limits!")
        else:
            self.logger.warning(f"⚠️ Exceeds free tier limits (~{estimated_storage_mb/1024:.1f} GB)")
        
        return sales_df
    
    def _generate_inventory_data(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate inventory data"""
        products = self.data_cache["dim_products"]
        locations = self.data_cache["dim_locations"]
        
        inventory = []
        inventory_id = 1
        
        # Generate monthly inventory snapshots from company founding (2015-01-01) to present
        start_date = datetime(2015, 1, 1)
        
        current_date = start_date
        while current_date <= datetime.now():
            # Calculate months since start for cost trend
            months_elapsed = ((current_date.year - start_date.year) * 12 + 
                            (current_date.month - start_date.month))
            
            for _, product in products.iterrows():
                base_cost = float(product["cost"])
                snapshot_date = current_date.date()
                
                # Apply cost fluctuations based on Philippine economic conditions:
                
                # 1. Philippine cost inflation (based on PSA data, slightly lower than retail)
                if snapshot_date < date(2017, 1, 1):
                    # 2015-2016: Very low (~1.2%)
                    cost_inflation_rate = 0.012
                elif snapshot_date < date(2018, 1, 1):
                    # 2017: Low (~2.5%)
                    cost_inflation_rate = 0.025
                elif snapshot_date < date(2019, 1, 1):
                    # 2018: TRAIN Law impact on inputs (~5.5%)
                    cost_inflation_rate = 0.055
                elif snapshot_date < date(2020, 1, 1):
                    # 2019: Moderate (~2.5%)
                    cost_inflation_rate = 0.025
                elif snapshot_date < date(2021, 1, 1):
                    # 2020: Pandemic - low (~2%)
                    cost_inflation_rate = 0.020
                elif snapshot_date < date(2022, 1, 1):
                    # 2021: Recovery (~3.5%)
                    cost_inflation_rate = 0.035
                elif snapshot_date < date(2023, 1, 1):
                    # 2022: High inflation (~5.5%)
                    cost_inflation_rate = 0.055
                elif snapshot_date < date(2024, 1, 1):
                    # 2023: Peak cost inflation (~7.5%)
                    cost_inflation_rate = 0.075
                elif snapshot_date < date(2025, 1, 1):
                    # 2024: Moderating (~3.5%)
                    cost_inflation_rate = 0.035
                else:
                    # 2025+: Stabilizing (~2%)
                    cost_inflation_rate = 0.020
                
                inflation_factor = 1 + (cost_inflation_rate * months_elapsed / 12)
                
                # 2. Supply chain volatility (±6%)
                supply_chain_factor = 1 + (0.06 * random.uniform(-1, 1))
                
                # 3. Import/forex impact (±5% - PHP peso fluctuations)
                forex_factor = 1 + (0.05 * random.uniform(-1, 1))
                
                # Calculate fluctuating cost
                fluctuating_cost = base_cost * inflation_factor * supply_chain_factor * forex_factor
                
                for _, location in locations.iterrows():
                    # Random inventory levels
                    opening_stock = random.randint(100, 1000)
                    stock_received = random.randint(0, 200)
                    stock_sold = random.randint(0, opening_stock + stock_received)
                    closing_stock = opening_stock + stock_received - stock_sold
                    stock_lost = random.randint(0, 10) if random.random() < 0.1 else 0
                    
                    inventory_record = {
                        "inventory_id": inventory_id,
                        "date": current_date.date(),
                        "product_id": product["product_id"],
                        "location_id": location["location_id"],
                        "opening_stock": opening_stock,
                        "closing_stock": closing_stock,
                        "stock_received": stock_received,
                        "stock_sold": stock_sold,
                        "stock_lost": stock_lost if stock_lost > 0 else None,
                        "unit_cost": round(fluctuating_cost, 2),
                        "total_value": round(closing_stock * fluctuating_cost, 2),
                        "created_at": current_date
                    }
                    inventory.append(inventory_record)
                    inventory_id += 1
            
            current_date += timedelta(days=30)  # Monthly snapshots
        
        return pd.DataFrame(inventory)
    
    def _generate_operating_costs(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate operating costs data"""
        departments = self.data_cache["dim_departments"]
        
        cost_categories = [
            "Salaries", "Rent", "Utilities", "Marketing", "Travel", 
            "Training", "Supplies", "Insurance", "Maintenance", "Legal"
        ]
        
        cost_types = ["Fixed", "Variable", "Semi-Variable"]
        
        costs = []
        cost_id = 1
        
        # Generate monthly costs from 2015 to present
        start_date = datetime(2015, 1, 1)
        
        current_date = start_date
        while current_date <= datetime.now():
            for _, department in departments.iterrows():
                cost_category = random.choice(cost_categories)
                cost_type = random.choice(cost_types)
                
                # Generate realistic cost amounts
                if cost_category == "Salaries":
                    amount = random.uniform(50000, 200000)
                elif cost_category == "Rent":
                    amount = random.uniform(20000, 80000)
                elif cost_category == "Marketing":
                    amount = random.uniform(10000, 50000)
                else:
                    amount = random.uniform(5000, 25000)
                
                cost_record = {
                    "cost_id": cost_id,
                    "date": current_date.date(),
                    "cost_category": cost_category,
                    "cost_type": cost_type,
                    "department_id": department["department_id"],
                    "amount": amount,
                    "description": f"{cost_category} - {cost_type} expense",
                    "created_at": current_date
                }
                costs.append(cost_record)
                cost_id += 1
            
            current_date += timedelta(days=30)
        
        return pd.DataFrame(costs)
    
    def _generate_marketing_costs(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate marketing costs data"""
        campaigns = self.data_cache["dim_campaigns"]
        
        cost_categories = [
            "Advertising", "Promotions", "Events", "Digital Marketing", 
            "Print Media", "TV/Radio", "Social Media", "Influencer Marketing"
        ]
        
        marketing_costs = []
        marketing_cost_id = 1
        
        # Generate costs for each campaign
        for _, campaign in campaigns.iterrows():
            # Generate costs throughout campaign duration
            current_date = campaign["start_date"]
            while current_date <= campaign["end_date"]:
                cost_category = random.choice(cost_categories)
                
                # Cost based on campaign budget
                budget = campaign["budget"]
                duration_days = (campaign["end_date"] - campaign["start_date"]).days
                daily_budget = budget / duration_days
                
                amount = daily_budget * random.uniform(0.5, 2.0)
                
                cost_record = {
                    "date": current_date,
                    "campaign_id": campaign["campaign_id"],
                    "cost_category": cost_category,
                    "amount": amount,
                    "description": f"{cost_category} expense for {campaign['campaign_name']}",
                    "created_at": current_date
                }
                marketing_costs.append(cost_record)
                
                current_date += timedelta(days=random.randint(1, 7))
        
        # Convert to DataFrame and sort by date
        marketing_costs_df = pd.DataFrame(marketing_costs)
        marketing_costs_df = marketing_costs_df.sort_values('date').reset_index(drop=True)
        
        # Assign IDs in chronological order
        for idx, row in marketing_costs_df.iterrows():
            marketing_costs_df.at[idx, 'marketing_cost_id'] = self.id_generator.generate_id('fact_marketing_costs')
        
        return marketing_costs_df
    
    def _generate_employee_facts(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate comprehensive employee fact data based on actual employee tenure"""
        employees = self.data_cache["dim_employees"]
        jobs = self.data_cache["dim_jobs"]
        
        employee_facts = []
        employee_fact_id = 1
        
        self.logger.info(f"Generating employee facts for {len(employees)} employees based on actual tenure")
        
        for _, employee in employees.iterrows():
            # Get job info for salary range
            job_info = jobs[jobs["job_id"] == employee["job_id"]].iloc[0]
            min_salary = job_info["min_salary"]
            max_salary = job_info["max_salary"]
            
            # Get employee employment details
            employment_type = employee.get("employment_type", "Regular")
            work_setup = employee.get("work_setup", "On-Site")
            hire_date = employee.get("hire_date")
            termination_date = employee.get("termination_date")
            
            # Skip if no hire date
            if not hire_date or pd.isna(hire_date):
                continue
            
            # Determine the period for this employee
            start_date = hire_date
            if termination_date and pd.notna(termination_date):
                end_date = termination_date
            else:
                end_date = datetime.now()
            
            # Ensure both dates are datetime objects for comparison
            if isinstance(start_date, pd.Timestamp):
                start_date = start_date.to_pydatetime()
            elif isinstance(start_date, date) and not isinstance(start_date, datetime):
                start_date = datetime.combine(start_date, datetime.min.time())
                
            if isinstance(end_date, pd.Timestamp):
                end_date = end_date.to_pydatetime()
            elif isinstance(end_date, date) and not isinstance(end_date, datetime):
                end_date = datetime.combine(end_date, datetime.min.time())
            
            # Generate monthly data for this employee's tenure
            current_date = start_date.replace(day=1)  # Start from first day of hire month
            
            while current_date <= end_date:
                # Skip if before hire date
                if current_date < start_date:
                    current_date = self._next_month(current_date)
                    continue
                
                # Base salary calculation with employment type adjustments
                base_salary = random.uniform(min_salary, max_salary)
                
                # Adjust base salary based on employment type
                if employment_type == "Intern":
                    base_salary = base_salary * 0.3  # 30% of regular salary
                elif employment_type == "Part-Time":
                    base_salary = base_salary * 0.6  # 60% of regular salary
                elif employment_type == "Contract":
                    base_salary = base_salary * 1.1  # 10% premium for contract
                elif employment_type == "Consultant":
                    base_salary = base_salary * 1.3  # 30% premium for consultants
                elif employment_type == "Probationary":
                    # Check if still in probation period (first 6 months)
                    months_worked = (current_date.date() - start_date.date()).days / 30.44
                    if months_worked <= 6:
                        base_salary = base_salary * 0.8  # 80% during probation
                    else:
                        # Completed probation, regular salary
                        pass
                
                # Adjust salary based on years worked (annual raises)
                years_worked = (current_date.date() - start_date.date()).days / 365.25
                annual_raise = 0.03  # 3% annual raise
                base_salary = base_salary * (1 + annual_raise * years_worked)
                
                # Cost of living adjustment (quarterly)
                cost_of_living_adjustment = base_salary * 0.02 if current_date.month % 3 == 0 else 0.0
                
                # Performance bonus (quarterly)
                if current_date.month % 3 == 0:
                    performance_rating = random.uniform(3.0, 5.0)
                    performance_bonus = base_salary * 0.1 * performance_rating / 4.0
                    quarterly_bonus = base_salary * 0.05
                else:
                    performance_rating = random.uniform(3.0, 5.0)
                    performance_bonus = 0.0
                    quarterly_bonus = 0.0
                
                # Overtime (30% chance)
                if random.random() < 0.3:
                    overtime_hours = random.uniform(5, 25)
                    overtime_rate = base_salary / 160 * 1.5  # 1.5x rate
                    overtime_pay = overtime_hours * overtime_rate
                else:
                    overtime_hours = 0.0
                    overtime_pay = 0.0
                
                # Holiday pay (if holiday in month)
                holiday_pay = base_salary / 160 * 8 * 1.5 if random.random() < 0.2 else 0.0
                
                # Night shift differential (20% chance)
                night_shift_differential = base_salary * 0.1 if random.random() < 0.2 else 0.0
                
                # Commission for sales roles
                job_title = str(job_info.get("job_title", ""))
                if "Sales" in job_title:
                    sales_target = random.uniform(50000, 200000)
                    sales_achieved = sales_target * random.uniform(0.8, 1.2)
                    commission_rate = 0.05
                    commission_earned = sales_achieved * commission_rate
                else:
                    sales_target = 0.0
                    sales_achieved = 0.0
                    commission_earned = 0.0
                
                # Various allowances based on work setup (always provide base allowances)
                if work_setup == "Remote":
                    transport_allowance = 1000  # Reduced for remote
                    meal_allowance = 1500  # Reduced for remote
                    communication_allowance = 3000  # Increased for remote
                elif work_setup == "Hybrid":
                    transport_allowance = 1500  # Partial for hybrid
                    meal_allowance = 2250  # Partial for hybrid
                    communication_allowance = 2000  # Moderate for hybrid
                else:  # On-Site, Office-Based, Field-Based
                    transport_allowance = 2000
                    meal_allowance = 3000
                    communication_allowance = 1000
                
                # Bonuses (provide base values with chance for additional)
                attendance_bonus = base_salary * 0.02 if random.random() < 0.8 else base_salary * 0.01
                productivity_bonus = base_salary * 0.03 if random.random() < 0.6 else base_salary * 0.015
                training_allowance = 5000 if random.random() < 0.3 else 2000  # Base training allowance
                
                # Hazard pay for specific work setups
                if work_setup == "Field-Based" and job_title in ["Operations", "Quality Assurance", "Driver", "Delivery"]:
                    hazard_pay = base_salary * 0.08  # Higher for field-based
                elif job_title in ["Operations", "Quality Assurance"] and random.random() < 0.5:
                    hazard_pay = base_salary * 0.05
                else:
                    hazard_pay = base_salary * 0.02  # Base hazard pay for all employees
                
                # Training and leave (provide base values)
                training_hours_completed = random.uniform(2, 20) if random.random() < 0.4 else random.uniform(0, 2)
                sick_days_used = random.uniform(0, 2) if random.random() < 0.3 else 0.0
                vacation_days_used = random.uniform(1, 3) if random.random() < 0.4 else 0.0
                
                # Calculate compensation totals
                gross_compensation = (base_salary + cost_of_living_adjustment + performance_bonus + 
                                    quarterly_bonus + overtime_pay + holiday_pay + night_shift_differential + 
                                    commission_earned + attendance_bonus + productivity_bonus + 
                                    training_allowance + transport_allowance + meal_allowance + 
                                    communication_allowance + hazard_pay)
                
                # Philippine deductions (approximately)
                tax_withheld = gross_compensation * 0.15 if gross_compensation > 20000 else gross_compensation * 0.10
                sss_contribution = min(gross_compensation * 0.045, 900) if gross_compensation > 0 else 0
                philhealth_contribution = min(gross_compensation * 0.0275, 1100) if gross_compensation > 0 else 0
                pagibig_contribution = min(gross_compensation * 0.02, 400) if gross_compensation > 0 else 0
                
                total_deductions = tax_withheld + sss_contribution + philhealth_contribution + pagibig_contribution
                net_compensation = gross_compensation - total_deductions
                total_compensation = gross_compensation  # For compatibility
                
                employee_fact = {
                    "employee_fact_id": f"EF-{employee_fact_id:08d}",
                    "employee_id": employee["employee_id"],
                    "date": current_date.date(),
                    "base_salary": round(base_salary, 2),
                    "cost_of_living_adjustment": round(cost_of_living_adjustment, 2),
                    "performance_bonus": round(performance_bonus, 2),
                    "quarterly_bonus": round(quarterly_bonus, 2),
                    "overtime_hours": round(overtime_hours, 1),
                    "overtime_pay": round(overtime_pay, 2),
                    "holiday_pay": round(holiday_pay, 2),
                    "night_shift_differential": round(night_shift_differential, 2),
                    "commission_earned": round(commission_earned, 2),
                    "sales_target": round(sales_target, 2),
                    "sales_achieved": round(sales_achieved, 2),
                    "attendance_bonus": round(attendance_bonus, 2),
                    "productivity_bonus": round(productivity_bonus, 2),
                    "training_allowance": round(training_allowance, 2),
                    "transport_allowance": round(transport_allowance, 2),
                    "meal_allowance": round(meal_allowance, 2),
                    "communication_allowance": round(communication_allowance, 2),
                    "hazard_pay": round(hazard_pay, 2),
                    "total_compensation": round(total_compensation, 2),
                    "gross_compensation": round(gross_compensation, 2),
                    "tax_withheld": round(tax_withheld, 2),
                    "sss_contribution": round(sss_contribution, 2),
                    "philhealth_contribution": round(philhealth_contribution, 2),
                    "pagibig_contribution": round(pagibig_contribution, 2),
                    "net_compensation": round(net_compensation, 2),
                    "performance_rating": round(performance_rating, 2),
                    "training_hours_completed": round(training_hours_completed, 1),
                    "sick_days_used": round(sick_days_used, 1),
                    "vacation_days_used": round(vacation_days_used, 1),
                    "created_at": datetime.now()
                }
                employee_facts.append(employee_fact)
                employee_fact_id += 1
                
                # Move to next month
                current_date = self._next_month(current_date)
        
        self.logger.info(f"Generated {len(employee_facts)} employee fact records")
        return pd.DataFrame(employee_facts)
    
    def _next_month(self, date):
        """Helper function to get first day of next month"""
        if date.month == 12:
            return date.replace(year=date.year + 1, month=1, day=1)
        else:
            return date.replace(month=date.month + 1, day=1)
    
    def load_fact_data(self) -> None:
        """Load fact data into BigQuery - optimized for free tier"""
        self.logger.info("Loading fact data into BigQuery...")
        
        for table_name, df in self.data_cache.items():
            if table_name.startswith("fact_"):
                # Load all at once for better performance
                self.logger.info(f"Loading {len(df)} rows into {table_name}")
                
                try:
                    self.bigquery_client.load_dataframe(df, table_name, "WRITE_TRUNCATE")
                    self.logger.info(f"✅ Successfully loaded {len(df)} rows into {table_name}")
                except Exception as e:
                    self.logger.error(f"❌ Failed to load {table_name}: {e}")
                    raise
        
        self.logger.info("Fact data loading completed")
    
    def run_full_pipeline(self, config: Dict[str, Any]) -> None:
        """Run the complete ETL pipeline"""
        self.logger.info("Starting full ETL pipeline...")
        
        try:
            # Setup database
            self.setup_database()
            
            # Generate and load dimension data
            self.generate_dimension_data(config)
            self.load_dimension_data()
            
            # Generate and load fact data
            self.generate_fact_data(config)
            self.load_fact_data()
            
            self.logger.info("ETL pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {e}")
            raise
    
    def run_incremental_update(self, config: Dict[str, Any]) -> None:
        """Run incremental update for new data"""
        self.logger.info("Starting incremental update...")
        
        try:
            # Step 1: Update previous day's shipped orders to delivered
            self._update_shipped_to_delivered()
            
            # Step 2: Generate new sales data for yesterday
            yesterday_sales_df = self._generate_daily_sales(config)
            
            if len(yesterday_sales_df) > 0:
                self.bigquery_client.load_dataframe(yesterday_sales_df, "fact_sales", "WRITE_APPEND")
                self.logger.info(f"Added {len(yesterday_sales_df)} new sales records")
            else:
                self.logger.info("No new sales data to append (may already exist for target date)")
            
            self.logger.info("Incremental update completed")
            
        except Exception as e:
            self.logger.error(f"Incremental update failed: {e}")
            raise
    
    def _update_shipped_to_delivered(self) -> None:
        """Update previous day's shipped orders to delivered status"""
        dataset_name = self.bigquery_client.dataset
        
        # Get orders that were shipped yesterday (should be delivered today)
        yesterday = datetime.now().date() - timedelta(days=1)
        two_days_ago = datetime.now().date() - timedelta(days=2)
        
        try:
            update_query = f"""
                UPDATE `{self.bigquery_client.project_id}.{dataset_name}.fact_sales`
                SET delivery_status = 'Delivered',
                    delivery_date = DATE('{yesterday}')
                WHERE delivery_status = 'Shipped'
                AND DATE(order_date) = DATE('{two_days_ago}')
                AND delivery_date IS NULL
            """
            
            result = self.bigquery_client.execute_query(update_query)
            self.logger.info(f"Updated shipped orders to delivered for {two_days_ago}")
            
        except Exception as e:
            self.logger.warning(f"Could not update shipped orders: {e}")
            # Continue with sales generation even if update fails
    
    def _generate_quarterly_campaigns(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate 1 new campaign for quarterly update"""
        import random
        from datetime import date, timedelta
        
        # Fixed to 1 campaign per quarter
        new_campaigns_count = 1
        
        # Get existing data for context
        dataset_name = self.bigquery_client.dataset
        
        # Get max campaign_id for sequencing
        try:
            max_id_query = f"""
                SELECT MAX(CAST(REGEXP_EXTRACT(campaign_id, r'CAM(\\d+)') AS INT64)) as max_id 
                FROM {dataset_name}.dim_campaigns 
                WHERE campaign_id LIKE 'CAM%'
            """
            max_id_result = self.bigquery_client.execute_query(max_id_query)
            if len(max_id_result) > 0 and max_id_result.iloc[0]['max_id'] is not None:
                max_id = max_id_result.iloc[0]['max_id']
            else:
                max_id = 0
        except Exception as e:
            self.logger.warning(f"Could not get max campaign_id: {e}")
            max_id = 0
        
        campaign_types = [
            "Product Launch", "Seasonal Promotion", "Brand Awareness", 
            "Discount Campaign", "Loyalty Program", "Digital Marketing",
            "In-Store Promotion", "Social Media Campaign"
        ]
        
        # Generate new campaigns
        campaigns = []
        current_year = datetime.now().year
        current_quarter = (datetime.now().month - 1) // 3 + 1
        
        # Calculate quarter start and end dates
        quarter_start_month = (current_quarter - 1) * 3 + 1
        quarter_start = date(current_year, quarter_start_month, 1)
        
        if current_quarter == 4:
            quarter_end = date(current_year + 1, 1, 1) - timedelta(days=1)
        else:
            quarter_end = date(current_year, quarter_start_month + 3, 1) - timedelta(days=1)
        
        for i in range(new_campaigns_count):
            # Generate campaign dates within the quarter
            start_day = random.randint(1, 28)
            start_date = date(current_year, quarter_start_month + random.randint(0, 2), start_day)
            
            # Ensure start_date is within quarter
            if start_date < quarter_start:
                start_date = quarter_start
            elif start_date > quarter_end:
                start_date = quarter_end
            
            # Duration of 2-3 months (60-90 days)
            duration = random.randint(60, 90)
            end_date = start_date + timedelta(days=duration)
            
            # Cap end date to quarter end
            if end_date > quarter_end:
                end_date = quarter_end
            
            # Determine status based on dates
            today = datetime.now().date()
            if end_date < today:
                status = random.choice(["Completed", "Cancelled"])
            elif start_date > today:
                status = "Planned"
            else:
                status = "Active"
            
            max_id += 1
            campaign = {
                "campaign_id": f"CAM{max_id:015d}",
                "campaign_name": f"Q{current_quarter} Campaign {max_id}: {random.choice(campaign_types)}",
                "campaign_type": random.choice(campaign_types),
                "start_date": start_date,
                "end_date": end_date,
                "budget": random.uniform(50000, 500000),
                "target_audience": random.choice(["All Customers", "Young Adults", "Families", "Business Owners"]),
                "status": status,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            campaigns.append(campaign)
        
        return pd.DataFrame(campaigns)
    
    def _generate_quarterly_marketing_costs(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate marketing costs for current quarter"""
        import random
        
        # Get all campaigns (including newly created ones)
        dataset_name = self.bigquery_client.dataset
        campaigns_df = self.bigquery_client.execute_query(f"SELECT * FROM {dataset_name}.dim_campaigns")
        
        if len(campaigns_df) == 0:
            return pd.DataFrame()
        
        cost_categories = [
            "Advertising", "Promotions", "Events", "Digital Marketing", 
            "Print Media", "TV/Radio", "Social Media", "Influencer Marketing"
        ]
        
        marketing_costs = []
        current_year = datetime.now().year
        current_quarter = (datetime.now().month - 1) // 3 + 1
        
        # Calculate quarter months
        quarter_start_month = (current_quarter - 1) * 3 + 1
        quarter_months = [quarter_start_month, quarter_start_month + 1, quarter_start_month + 2]
        
        # Generate costs for each campaign for each month in the quarter
        for _, campaign in campaigns_df.iterrows():
            for month in quarter_months:
                # Generate 3-5 cost entries per campaign per month
                num_entries = random.randint(3, 5)
                
                for i in range(num_entries):
                    cost_category = random.choice(cost_categories)
                    
                    # Cost based on campaign budget
                    budget = campaign["budget"]
                    amount = budget * random.uniform(0.02, 0.08)  # 2-8% of quarterly budget per entry
                    
                    cost_date = datetime(current_year, month, random.randint(1, 28)).date()
                    
                    cost_record = {
                        "marketing_cost_id": self.id_generator.generate_id('fact_marketing_costs'),
                        "date": cost_date,
                        "campaign_id": campaign["campaign_id"],
                        "cost_category": cost_category,
                        "amount": amount,
                        "description": f"{cost_category} expense for {campaign['campaign_name']} - Q{current_quarter}/{current_year}",
                        "created_at": datetime.combine(cost_date, datetime.min.time())
                    }
                    marketing_costs.append(cost_record)
        
        return pd.DataFrame(marketing_costs)
    
    def _generate_monthly_employees(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate 1-3 new employees for monthly update"""
        import random
        from datetime import date
        
        new_employees_count = random.randint(1, 3)  # 1-3 new employees per month
        
        # Get existing data for context
        dataset_name = self.bigquery_client.dataset
        jobs_df = self.bigquery_client.execute_query(f"SELECT * FROM {dataset_name}.dim_jobs")
        locations_df = self.bigquery_client.execute_query(f"SELECT * FROM {dataset_name}.dim_locations")
        
        # Get max employee_id for sequencing
        try:
            max_id_query = f"""
                SELECT MAX(CAST(REGEXP_EXTRACT(employee_id, r'EMP(\\d+)') AS INT64)) as max_id 
                FROM {dataset_name}.dim_employees 
                WHERE employee_id LIKE 'EMP%'
            """
            max_id_result = self.bigquery_client.execute_query(max_id_query)
            if len(max_id_result) > 0 and max_id_result.iloc[0]['max_id'] is not None:
                max_id = max_id_result.iloc[0]['max_id']
            else:
                max_id = 0
        except Exception as e:
            self.logger.warning(f"Could not get max employee_id: {e}")
            max_id = 0
        
        # Generate new employees
        employees = []
        for i in range(new_employees_count):
            job = jobs_df.sample(1).iloc[0]
            location = locations_df.sample(1).iloc[0]
            
            # Generate hire date for this month
            current_year = datetime.now().year
            current_month = datetime.now().month
            hire_date = date(current_year, current_month, random.randint(1, 28))
            
            max_id += 1
            employee = {
                "employee_id": f"EMP{max_id:015d}",
                "first_name": f"New{max_id}",
                "last_name": f"Employee{max_id}",
                "email": f"new.employee{max_id}@company.com",
                "phone": f"+639{random.randint(100000000, 999999999)}",
                "hire_date": hire_date,
                "termination_date": None,
                "job_id": job["job_id"],
                "location_id": location["location_id"],
                "employment_type": "Regular" if random.random() < 0.7 else random.choice(["Contract", "Probationary"]),
                "work_setup": random.choice(["On-Site", "Remote", "Hybrid"]),
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            employees.append(employee)
        
        return pd.DataFrame(employees)
    
    def _generate_monthly_products(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate 1-2 new products for monthly update"""
        import random
        from ..core.generators import ProductGenerator
        from ..utils.id_generation import IDGenerator
        
        new_products_count = config.get('new_products_count', 2)
        
        # Get existing data for context
        dataset_name = self.bigquery_client.dataset
        categories_df = self.bigquery_client.execute_query(f"SELECT * FROM {dataset_name}.dim_categories")
        subcategories_df = self.bigquery_client.execute_query(f"SELECT * FROM {dataset_name}.dim_subcategories")
        brands_df = self.bigquery_client.execute_query(f"SELECT * FROM {dataset_name}.dim_brands")
        
        # Get max product_id for sequencing
        try:
            max_id_query = f"""
                SELECT MAX(CAST(REGEXP_EXTRACT(product_id, r'PRO(\\d+)') AS INT64)) as max_id 
                FROM {dataset_name}.dim_products 
                WHERE product_id LIKE 'PRO%'
            """
            max_id_result = self.bigquery_client.execute_query(max_id_query)
            if len(max_id_result) > 0 and max_id_result.iloc[0]['max_id'] is not None:
                max_id = max_id_result.iloc[0]['max_id']
            else:
                max_id = 0
        except Exception as e:
            self.logger.warning(f"Could not get max product_id: {e}")
            max_id = 0
        
        # Generate new products
        products = []
        for i in range(new_products_count):
            category = categories_df.sample(1).iloc[0]
            subcategory = subcategories_df[subcategories_df["category_id"] == category["category_id"]].sample(1).iloc[0]
            brand = brands_df.sample(1).iloc[0]
            
            # Generate realistic pricing
            base_price = random.uniform(10, 500)
            cost = base_price * random.uniform(0.4, 0.6)
            
            # Generate launch date for this month
            from datetime import date
            current_year = datetime.now().year
            current_month = datetime.now().month
            launch_date = date(current_year, current_month, random.randint(1, 28))
            
            max_id += 1
            product = {
                "product_id": f"PRO{max_id:015d}",
                "product_name": f"{brand['brand_name']} {subcategory['subcategory_name']} New {max_id}",
                "sku": f"SKU-{max_id:06d}",
                "category_id": category["category_id"],
                "subcategory_id": subcategory["subcategory_id"],
                "brand_id": brand["brand_id"],
                "unit_price": round(base_price, 2),
                "cost": round(cost, 2),
                "status": "Active",
                "launch_date": launch_date,
                "discontinued_date": None,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            products.append(product)
        
        return pd.DataFrame(products)
    
    def _generate_monthly_costs(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate operating costs for current month"""
        import random
        
        # Get existing departments
        dataset_name = self.bigquery_client.dataset
        departments_df = self.bigquery_client.execute_query(f"SELECT * FROM {dataset_name}.dim_departments")
        
        # Get max cost_id for sequencing
        try:
            max_id_query = f"""
                SELECT MAX(cost_id) as max_id FROM {dataset_name}.fact_operating_costs
            """
            max_id_result = self.bigquery_client.execute_query(max_id_query)
            if len(max_id_result) > 0 and max_id_result.iloc[0]['max_id'] is not None:
                max_id = max_id_result.iloc[0]['max_id']
            else:
                max_id = 0
        except Exception as e:
            self.logger.warning(f"Could not get max cost_id: {e}")
            max_id = 0
        
        cost_categories = [
            "Salaries", "Rent", "Utilities", "Marketing", "Travel", 
            "Training", "Supplies", "Insurance", "Maintenance", "Legal"
        ]
        cost_types = ["Fixed", "Variable", "Semi-Variable"]
        
        costs = []
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # Generate costs for each department for current month
        for _, department in departments_df.iterrows():
            cost_category = random.choice(cost_categories)
            cost_type = random.choice(cost_types)
            
            # Generate realistic cost amounts
            if cost_category == "Salaries":
                amount = random.uniform(50000, 200000)
            elif cost_category == "Rent":
                amount = random.uniform(20000, 80000)
            elif cost_category == "Marketing":
                amount = random.uniform(10000, 50000)
            else:
                amount = random.uniform(5000, 25000)
            
            max_id += 1
            cost_date = datetime(current_year, current_month, random.randint(1, 28)).date()
            
            cost_record = {
                "cost_id": max_id,
                "date": cost_date,
                "cost_category": cost_category,
                "cost_type": cost_type,
                "department_id": department["department_id"],
                "amount": amount,
                "description": f"{cost_category} - {cost_type} expense for {current_month}/{current_year}",
                "created_at": datetime.combine(cost_date, datetime.min.time())
            }
            costs.append(cost_record)
        
        return pd.DataFrame(costs)
    
    def _generate_monthly_marketing_costs(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate marketing costs for current month"""
        import random
        
        # Get active campaigns
        dataset_name = self.bigquery_client.dataset
        campaigns_df = self.bigquery_client.execute_query(f"SELECT * FROM {dataset_name}.dim_campaigns WHERE status = 'Active'")
        
        if len(campaigns_df) == 0:
            return pd.DataFrame()
        
        cost_categories = [
            "Advertising", "Promotions", "Events", "Digital Marketing", 
            "Print Media", "TV/Radio", "Social Media", "Influencer Marketing"
        ]
        
        marketing_costs = []
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # Generate costs for each active campaign
        for _, campaign in campaigns_df.iterrows():
            # Generate 2-4 cost entries per campaign per month
            num_entries = random.randint(2, 4)
            
            for i in range(num_entries):
                cost_category = random.choice(cost_categories)
                
                # Cost based on campaign budget
                budget = campaign["budget"]
                amount = budget * random.uniform(0.01, 0.05)  # 1-5% of monthly budget
                
                cost_date = datetime(current_year, current_month, random.randint(1, 28)).date()
                
                cost_record = {
                    "marketing_cost_id": self.id_generator.generate_id('fact_marketing_costs'),
                    "date": cost_date,
                    "campaign_id": campaign["campaign_id"],
                    "cost_category": cost_category,
                    "amount": amount,
                    "description": f"{cost_category} expense for {campaign['campaign_name']} - {current_month}/{current_year}",
                    "created_at": datetime.combine(cost_date, datetime.min.time())
                }
                marketing_costs.append(cost_record)
        
        return pd.DataFrame(marketing_costs)
    
    def _generate_monthly_inventory(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate inventory snapshots for current month"""
        import random
        
        # Get existing products and locations
        dataset_name = self.bigquery_client.dataset
        products_df = self.bigquery_client.execute_query(f"SELECT * FROM {dataset_name}.dim_products WHERE status = 'Active'")
        locations_df = self.bigquery_client.execute_query(f"SELECT * FROM {dataset_name}.dim_locations")
        
        # Get max inventory_id for sequencing
        try:
            max_id_query = f"""
                SELECT MAX(inventory_id) as max_id FROM {dataset_name}.fact_inventory
            """
            max_id_result = self.bigquery_client.execute_query(max_id_query)
            if len(max_id_result) > 0 and max_id_result.iloc[0]['max_id'] is not None:
                max_id = max_id_result.iloc[0]['max_id']
            else:
                max_id = 0
        except Exception as e:
            self.logger.warning(f"Could not get max inventory_id: {e}")
            max_id = 0
        
        inventory = []
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # Generate one inventory snapshot per product-location combination for current month
        snapshot_date = datetime(current_year, current_month, random.randint(25, 28)).date()
        
        # Calculate months since 2015 for cost trend
        start_date = datetime(2015, 1, 1)
        current_date = datetime(current_year, current_month, 1)
        months_elapsed = ((current_date.year - start_date.year) * 12 + 
                        (current_date.month - start_date.month))
        
        for _, product in products_df.iterrows():
            base_cost = float(product["cost"])
            
            # Apply cost fluctuations:
            # 1. Long-term inflation trend (2-3% annually)
            inflation_factor = 1 + (0.025 * months_elapsed / 12)
            
            # 2. Seasonal variation (±5%)
            seasonal_factor = 1 + (0.05 * random.uniform(-1, 1))
            
            # 3. Market volatility (±8% random monthly changes)
            volatility_factor = 1 + (0.08 * random.uniform(-1, 1))
            
            # Calculate fluctuating cost
            fluctuating_cost = base_cost * inflation_factor * seasonal_factor * volatility_factor
            
            for _, location in locations_df.iterrows():
                # Random inventory levels
                opening_stock = random.randint(100, 1000)
                stock_received = random.randint(0, 200)
                stock_sold = random.randint(0, opening_stock + stock_received)
                closing_stock = opening_stock + stock_received - stock_sold
                stock_lost = random.randint(0, 10) if random.random() < 0.1 else 0
                
                max_id += 1
                inventory_record = {
                    "inventory_id": max_id,
                    "date": snapshot_date,
                    "product_id": product["product_id"],
                    "location_id": location["location_id"],
                    "opening_stock": opening_stock,
                    "closing_stock": closing_stock,
                    "stock_received": stock_received,
                    "stock_sold": stock_sold,
                    "stock_lost": stock_lost if stock_lost > 0 else None,
                    "unit_cost": round(fluctuating_cost, 2),
                    "total_value": round(closing_stock * fluctuating_cost, 2),
                    "created_at": datetime.combine(snapshot_date, datetime.min.time())
                }
                inventory.append(inventory_record)
        
        return pd.DataFrame(inventory)
    
    def _generate_daily_sales(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate daily sales for incremental updates - specifically for yesterday"""
        daily_amount = config.get("daily_sales_amount", 2000000)
        
        # Get reference data from BigQuery with proper dataset qualification
        dataset_name = self.bigquery_client.dataset
        products = self.bigquery_client.execute_query(f"SELECT * FROM {dataset_name}.dim_products WHERE status = 'Active'")
        retailers = self.bigquery_client.execute_query(f"SELECT * FROM {dataset_name}.dim_retailers WHERE status = 'Active'")
        employees = self.bigquery_client.execute_query(f"SELECT * FROM {dataset_name}.dim_employees WHERE termination_date IS NULL")
        campaigns = self.bigquery_client.execute_query(f"SELECT * FROM {dataset_name}.dim_campaigns WHERE status = 'Active'")
        
        # Sort campaigns by start_date to prioritize latest campaigns
        if len(campaigns) > 0:
            campaigns = campaigns.sort_values('start_date', ascending=False).reset_index(drop=True)
        
        # Get max sale_id from existing data to continue the sequence
        try:
            max_id_query = f"""
                SELECT MAX(CAST(REGEXP_EXTRACT(sale_id, r'SAL(\\d+)') AS INT64)) as max_id 
                FROM {dataset_name}.fact_sales 
                WHERE sale_id LIKE 'SAL%'
            """
            max_id_result = self.bigquery_client.execute_query(max_id_query)
            if len(max_id_result) > 0 and max_id_result.iloc[0]['max_id'] is not None:
                max_id = max_id_result.iloc[0]['max_id']
                self.logger.info(f"Found max sale_id: {max_id}")
            else:
                max_id = 0
                self.logger.info("No existing sales found, starting from ID 1")
        except Exception as e:
            self.logger.warning(f"Could not get max sale_id: {e}, starting from 1")
            max_id = 0
        
        # Check if sales already exist for target date to avoid duplicates
        target_date = datetime.now().date() - timedelta(days=1)
        try:
            existing_count_query = f"""
                SELECT COUNT(*) as count 
                FROM {dataset_name}.fact_sales 
                WHERE DATE(date) = DATE('{target_date}')
            """
            existing_result = self.bigquery_client.execute_query(existing_count_query)
            if len(existing_result) > 0 and existing_result.iloc[0]['count'] > 0:
                self.logger.info(f"Sales already exist for {target_date}, skipping generation")
                return pd.DataFrame()  # Return empty DataFrame
        except Exception as e:
            self.logger.warning(f"Could not check existing sales: {e}, proceeding with generation")
        
        sales = []
        # Generate for yesterday specifically (so daily workflow can run today)
        current_date = target_date
        
        self.logger.info(f"Generating daily sales for {current_date}")
        
        # Generate daily transactions in range 99-148
        total_transactions = random.randint(99, 148)
        
        self.logger.info(f"Generating {total_transactions} transactions for {current_date}")
        
        for i in range(total_transactions):
            product = products.sample(1).iloc[0]
            retailer = retailers.sample(1).iloc[0]
            employee = employees.sample(1).iloc[0]
            
            # Get retailer-specific transaction parameters
            retailer_params = self.get_retailer_transaction_params(retailer["retailer_type"])
            
            # Campaign assignment (30% chance) - prioritize latest campaigns
            campaign = None
            if random.random() < 0.3 and len(campaigns) > 0:
                # Select from top 3 latest campaigns (or fewer if less available)
                top_campaigns = campaigns.head(min(3, len(campaigns)))
                campaign = top_campaigns.iloc[0]  # Take the latest one
            
            # Generate quantity and amount based on retailer type
            quantity = random.randint(retailer_params["min_qty"], retailer_params["max_qty"])
            unit_price = float(product["unit_price"])
            total_amount = quantity * unit_price
            
            # Ensure transaction is within retailer's expected range
            if total_amount > retailer_params["max_amount"]:
                quantity = max(1, int(retailer_params["max_amount"] / unit_price))
                total_amount = quantity * unit_price
            elif total_amount < retailer_params["min_amount"]:
                quantity = min(retailer_params["max_qty"], max(1, int(retailer_params["min_amount"] / unit_price)))
                total_amount = quantity * unit_price
            
            # Calculate discount and commission
            discount_rate = random.uniform(0.05, 0.15) if campaign is not None else 0
            commission_rate = random.uniform(0.02, 0.08)
            
            final_amount = total_amount * (1 - discount_rate)
            commission_amount = final_amount * commission_rate
            
            # Generate proper sale_id continuing from max existing ID
            max_id += 1
            sale_id = f"SAL{max_id:015d}"
            
            # Delivery status logic - realistic progression for daily sales
            # For yesterday's orders: some are still pending, some shipped today
            delivery_status = random.choice(["Pending", "Shipped"])  # No delivered yet for yesterday's orders
            if delivery_status == "Pending":
                delivery_date = None  # Pending orders don't have delivery dates
            else:  # Shipped orders
                delivery_date = current_date + timedelta(days=1)  # Will be delivered tomorrow
            
            sale = {
                "sale_id": sale_id,
                "date": current_date,  # Required field
                "product_id": product["product_id"],
                "retailer_id": retailer["retailer_id"],
                "employee_id": employee["employee_id"],
                "campaign_id": campaign["campaign_id"] if campaign is not None else None,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "discount_amount": total_amount * discount_rate,
                "commission_rate": commission_rate,
                "order_date": current_date,
                "delivery_date": delivery_date,
                "delivery_status": delivery_status,
                "created_at": datetime.combine(current_date, datetime.min.time())
            }
            sales.append(sale)
        
        sales_df = pd.DataFrame(sales)
        self.logger.info(f"Generated {len(sales_df)} daily sales for {current_date}")
        
        return sales_df
