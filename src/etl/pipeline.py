"""
ETL pipeline for FMCG Data Analytics Platform
"""

import random
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from faker import Faker

from ..data.schemas import ALL_SCHEMAS, get_bigquery_schema
from ..utils.bigquery_client import BigQueryManager
from ..utils.logger import default_logger
from ..core.generators import (
    LocationGenerator, DepartmentGenerator, JobGenerator, EmployeeGenerator,
    ProductGenerator, RetailerGenerator, CampaignGenerator
)


class ETLPipeline:
    """Main ETL pipeline for data generation and loading"""
    
    def __init__(self, bq_manager=None):
        self.logger = default_logger
        self.bigquery_client = bq_manager or BigQueryManager()
        
        # Initialize faker for data generation
        self.faker = Faker('en_PH')
        
        # Initialize generators
        self.location_gen = LocationGenerator(self.faker)
        self.department_gen = DepartmentGenerator(self.faker)
        
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
            "Sari-Sari Store": {"min_qty": 1, "max_qty": 5, "min_amount": 10, "max_amount": 300, "daily_transactions": (10, 30)},
            "Convenience Store": {"min_qty": 1, "max_qty": 10, "min_amount": 300, "max_amount": 1200, "daily_transactions": (5, 15)},
            "Pharmacy": {"min_qty": 1, "max_qty": 15, "min_amount": 600, "max_amount": 3000, "daily_transactions": (3, 8)},
            "Wholesale": {"min_qty": 10, "max_qty": 100, "min_amount": 1200, "max_amount": 8000, "daily_transactions": (1, 3)},
            "Supermarket": {"min_qty": 5, "max_qty": 50, "min_amount": 2000, "max_amount": 6000, "daily_transactions": (2, 6)},
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
        
        # Generate jobs (depends on departments)
        self.job_gen = JobGenerator(self.faker, departments_df)
        jobs_df = self.job_gen.generate_jobs()
        self.data_cache["dim_jobs"] = jobs_df
        
        # Generate employees (depends on departments, jobs, locations)
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
        """Generate fact tables"""
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
        marketing_costs_df = self._generate_marketing_costs(config)
        self.data_cache["fact_marketing_costs"] = marketing_costs_df
        
        self.logger.info("Fact data generation completed")
    
    def _generate_sales_data(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate sales transaction data - optimized for free tier"""
        initial_amount = config.get("initial_sales_amount", 8000000000)
        daily_amount = config.get("daily_sales_amount", 2000000)
        
        # Get reference data
        products = self.data_cache["dim_products"]
        retailers = self.data_cache["dim_retailers"]
        employees = self.data_cache["dim_employees"]
        campaigns = self.data_cache["dim_campaigns"]
        
        # Calculate target transactions for 11 years (reduced for free tier)
        target_transactions = 100000  # Reduced from 500K to 100K
        
        # Set exact date range: January 1, 2015 to day before yesterday
        start_date = datetime(2015, 1, 1)
        end_date = datetime.now() - timedelta(days=2)  # Day before yesterday
        total_days = (end_date - start_date).days + 1  # Include both start and end dates
        
        # Calculate daily transaction targets
        base_daily_transactions = target_transactions // total_days
        
        # Add some variation (±20%)
        min_daily_tx = max(10, int(base_daily_transactions * 0.8))
        max_daily_tx = int(base_daily_transactions * 1.2)
        
        self.logger.info(f"Target: {target_transactions:,} transactions")
        self.logger.info(f"Date range: {start_date.date()} to {end_date.date()} ({total_days} days)")
        self.logger.info(f"Daily range: {min_daily_tx}-{max_daily_tx} transactions")
        self.logger.info(f"Expected annual: {target_transactions // (total_days/365):,.0f} transactions")
        
        # Generate all sales in one go
        sales = []
        sale_id = 1
        total_generated = 0
        
        current_date = start_date
        
        while current_date <= end_date:
            # Calculate daily transactions with variation
            daily_tx = random.randint(min_daily_tx, max_daily_tx)
            
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
                    campaign = campaigns.sample(1).iloc[0]
                
                # Generate quantity and amount based on retailer type
                quantity = random.randint(retailer_params["min_qty"], retailer_params["max_qty"])
                unit_price = product["unit_price"]
                total_amount = quantity * unit_price
                
                # Ensure transaction is within retailer's expected range
                if total_amount > retailer_params["max_amount"]:
                    quantity = max(1, int(retailer_params["max_amount"] / unit_price))
                    total_amount = quantity * unit_price
                elif total_amount < retailer_params["min_amount"]:
                    quantity = min(retailer_params["max_qty"], max(1, int(retailer_params["min_amount"] / unit_price)))
                    total_amount = quantity * unit_price
                
                # Calculate discount and commission
                discount_rate = random.uniform(0.05, 0.15) if campaign else 0
                commission_rate = random.uniform(0.02, 0.08)
                
                final_amount = total_amount * (1 - discount_rate)
                commission_amount = final_amount * commission_rate
                
                sale = {
                    "sale_id": f"SAL-{sale_id:08d}",
                    "product_id": product["product_id"],
                    "retailer_id": retailer["retailer_id"],
                    "employee_id": employee["employee_id"],
                    "campaign_id": campaign["campaign_id"] if campaign else None,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_amount": total_amount,
                    "discount_rate": discount_rate,
                    "discount_amount": total_amount * discount_rate,
                    "final_amount": final_amount,
                    "commission_rate": commission_rate,
                    "commission_amount": commission_amount,
                    "order_date": current_date.date(),
                    "delivery_date": current_date.date() + timedelta(days=random.randint(1, 7)),
                    "delivery_status": random.choice(["Pending", "Shipped", "Delivered"])
                }
                sales.append(sale)
                sale_id += 1
            
            total_generated += daily_tx
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
        
        # Check if within free tier limits (10GB total)
        if estimated_storage_mb < 10240:  # 10GB in MB
            self.logger.info("✅ Within free tier storage limits!")
        else:
            self.logger.warning("⚠️ May exceed free tier storage limits")
        
        return sales_df
    
    def _generate_inventory_data(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate inventory data"""
        products = self.data_cache["dim_products"]
        locations = self.data_cache["dim_locations"]
        
        inventory = []
        inventory_id = 1
        
        # Generate monthly inventory snapshots for the last 1 year (reduced from 2)
        start_date = datetime.now() - timedelta(days=365)  # 1 year ago
        
        current_date = start_date
        while current_date <= datetime.now():
            # Sample only 20% of products and locations to reduce data volume
            sample_products = products.sample(frac=0.2, random_state=42)
            sample_locations = locations.sample(frac=0.2, random_state=42)
            
            for _, product in sample_products.iterrows():
                for _, location in sample_locations.iterrows():
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
                        "unit_cost": product["cost"],
                        "total_value": closing_stock * product["cost"]
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
        
        # Generate monthly costs for the last 1 year (reduced from 2)
        start_date = datetime.now() - timedelta(days=365)
        
        current_date = start_date
        while current_date <= datetime.now():
            # Sample only 50% of departments to reduce data volume
            sample_departments = departments.sample(frac=0.5, random_state=42)
            
            for _, department in sample_departments.iterrows():
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
                    "description": f"{cost_category} - {cost_type} expense"
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
        
        # Generate costs for each campaign (sample 50% to reduce volume)
        sample_campaigns = campaigns.sample(frac=0.5, random_state=42)
        
        for _, campaign in sample_campaigns.iterrows():
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
                    "marketing_cost_id": marketing_cost_id,
                    "date": current_date,
                    "campaign_id": campaign["campaign_id"],
                    "cost_category": cost_category,
                    "amount": amount,
                    "description": f"{cost_category} expense for {campaign['campaign_name']}"
                }
                marketing_costs.append(cost_record)
                marketing_cost_id += 1
                
                current_date += timedelta(days=random.randint(1, 7))
        
        return pd.DataFrame(marketing_costs)
    
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
            # Generate new sales data for today
            today_sales_df = self._generate_daily_sales(config)
            
            if len(today_sales_df) > 0:
                self.bigquery_client.load_dataframe(today_sales_df, "fact_sales", "WRITE_APPEND")
                self.logger.info(f"Added {len(today_sales_df)} new sales records")
            
            self.logger.info("Incremental update completed")
            
        except Exception as e:
            self.logger.error(f"Incremental update failed: {e}")
            raise
    
    def _generate_daily_sales(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate daily sales for incremental updates - specifically for yesterday"""
        daily_amount = config.get("daily_sales_amount", 2000000)
        
        # Get reference data from BigQuery
        products = self.bigquery_client.execute_query("SELECT * FROM dim_products WHERE status = 'Active'")
        retailers = self.bigquery_client.execute_query("SELECT * FROM dim_retailers WHERE status = 'Active'")
        employees = self.bigquery_client.execute_query("SELECT * FROM dim_employees WHERE termination_date IS NULL")
        campaigns = self.bigquery_client.execute_query("SELECT * FROM dim_campaigns WHERE status = 'Active'")
        
        sales = []
        sale_id = 1
        # Generate for yesterday specifically (so daily workflow can run today)
        current_date = datetime.now().date() - timedelta(days=1)
        
        self.logger.info(f"Generating daily sales for {current_date}")
        
        # Calculate total transactions based on retailer distribution
        total_transactions = 0
        for _, retailer in retailers.iterrows():
            retailer_params = self.get_retailer_transaction_params(retailer["retailer_type"])
            daily_tx_range = retailer_params["daily_transactions"]
            total_transactions += random.randint(daily_tx_range[0], daily_tx_range[1])
        
        # Cap total transactions to reasonable limit for daily run
        total_transactions = min(total_transactions, 800)
        
        self.logger.info(f"Generating {total_transactions} transactions for {current_date}")
        
        for _ in range(total_transactions):
            product = products.sample(1).iloc[0]
            retailer = retailers.sample(1).iloc[0]
            employee = employees.sample(1).iloc[0]
            
            # Get retailer-specific transaction parameters
            retailer_params = self.get_retailer_transaction_params(retailer["retailer_type"])
            
            # Random campaign assignment (30% chance)
            campaign = None
            if random.random() < 0.3 and len(campaigns) > 0:
                campaign = campaigns.sample(1).iloc[0]
            
            # Generate quantity and amount based on retailer type
            quantity = random.randint(retailer_params["min_qty"], retailer_params["max_qty"])
            unit_price = product["unit_price"]
            total_amount = quantity * unit_price
            
            # Ensure transaction is within retailer's expected range
            if total_amount > retailer_params["max_amount"]:
                quantity = max(1, int(retailer_params["max_amount"] / unit_price))
                total_amount = quantity * unit_price
            elif total_amount < retailer_params["min_amount"]:
                quantity = min(retailer_params["max_qty"], max(1, int(retailer_params["min_amount"] / unit_price)))
                total_amount = quantity * unit_price
            
            # Calculate discount and commission
            discount_rate = random.uniform(0.05, 0.15) if campaign else 0
            commission_rate = random.uniform(0.02, 0.08)
            
            final_amount = total_amount * (1 - discount_rate)
            commission_amount = final_amount * commission_rate
            
            sale = {
                "sale_id": f"SAL-{sale_id:08d}",
                "product_id": product["product_id"],
                "retailer_id": retailer["retailer_id"],
                "employee_id": employee["employee_id"],
                "campaign_id": campaign["campaign_id"] if campaign else None,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "discount_rate": discount_rate,
                "discount_amount": total_amount * discount_rate,
                "final_amount": final_amount,
                "commission_rate": commission_rate,
                "commission_amount": commission_amount,
                "order_date": current_date,
                "delivery_date": current_date + timedelta(days=random.randint(1, 7)),
                "delivery_status": random.choice(["Pending", "Shipped", "Delivered"])
            }
            sales.append(sale)
            sale_id += 1
        
        sales_df = pd.DataFrame(sales)
        self.logger.info(f"Generated {len(sales_df)} daily sales for {current_date}")
        
        return sales_df
