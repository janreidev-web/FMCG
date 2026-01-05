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
    
    def __init__(self, bq_manager: BigQueryManager):
        self.bq_manager = bq_manager
        self.faker = Faker('en_PH')  # Philippine locale
        self.logger = default_logger
        
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
    
    def setup_database(self) -> None:
        """Set up BigQuery dataset and tables"""
        self.logger.info("Setting up database schema...")
        
        # Ensure dataset exists
        self.bq_manager.ensure_dataset()
        
        # Create all tables
        for table_name, schema in ALL_SCHEMAS.items():
            bq_schema = get_bigquery_schema(schema)
            self.bq_manager.create_table(table_name, bq_schema)
        
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
                self.bq_manager.load_dataframe(df, table_name)
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
        """Generate sales transaction data"""
        initial_amount = config.get("initial_sales_amount", 8000000000)
        daily_amount = config.get("daily_sales_amount", 2000000)
        
        # Get reference data
        products = self.data_cache["dim_products"]
        retailers = self.data_cache["dim_retailers"]
        employees = self.data_cache["dim_employees"]
        campaigns = self.data_cache["dim_campaigns"]
        
        # Generate historical sales (10 years)
        sales = []
        sale_id = 1
        
        start_date = datetime.now() - timedelta(days=3650)  # 10 years ago
        end_date = datetime.now()
        
        current_date = start_date
        while current_date <= end_date:
            # Generate daily sales
            daily_target = daily_amount if current_date > (datetime.now() - timedelta(days=30)) else daily_amount * 0.8
            
            # Number of transactions per day
            num_transactions = random.randint(50, 200)
            
            for _ in range(num_transactions):
                product = products.sample(1).iloc[0]
                retailer = retailers.sample(1).iloc[0]
                employee = employees.sample(1).iloc[0]
                
                # Random campaign assignment (30% chance)
                campaign = None
                if random.random() < 0.3:
                    campaign = campaigns.sample(1).iloc[0]
                
                quantity = random.randint(1, 50)
                unit_price = product["unit_price"]
                total_amount = quantity * unit_price
                
                # Apply discount (10% chance)
                discount = 0
                if random.random() < 0.1:
                    discount = total_amount * random.uniform(0.05, 0.20)
                    total_amount -= discount
                
                # Commission rate (5-15%)
                commission_rate = random.uniform(0.05, 0.15)
                
                # Delivery date (1-7 days after order)
                delivery_date = current_date.date() + timedelta(days=random.randint(1, 7))
                delivery_status = random.choice(["Pending", "Shipped", "Delivered", "Cancelled"])
                
                sale = {
                    "sale_id": sale_id,
                    "date": current_date.date(),
                    "product_id": product["product_id"],
                    "retailer_id": retailer["retailer_id"],
                    "employee_id": employee["employee_id"],
                    "campaign_id": campaign["campaign_id"] if campaign is not None else None,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_amount": total_amount,
                    "discount_amount": discount if discount > 0 else None,
                    "commission_rate": commission_rate,
                    "order_date": current_date.date(),
                    "delivery_date": delivery_date if delivery_status == "Delivered" else None,
                    "delivery_status": delivery_status,
                    "created_at": current_date
                }
                sales.append(sale)
                sale_id += 1
            
            current_date += timedelta(days=1)
        
        return pd.DataFrame(sales)
    
    def _generate_inventory_data(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate inventory data"""
        products = self.data_cache["dim_products"]
        locations = self.data_cache["dim_locations"]
        
        inventory = []
        inventory_id = 1
        
        # Generate monthly inventory snapshots for the last 2 years
        start_date = datetime.now() - timedelta(days=730)  # 2 years ago
        
        current_date = start_date
        while current_date <= datetime.now():
            for _, product in products.iterrows():
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
                        "unit_cost": product["cost"],
                        "total_value": closing_stock * product["cost"],
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
        
        # Generate monthly costs for the last 2 years
        start_date = datetime.now() - timedelta(days=730)
        
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
                    "marketing_cost_id": marketing_cost_id,
                    "date": current_date,
                    "campaign_id": campaign["campaign_id"],
                    "cost_category": cost_category,
                    "amount": amount,
                    "description": f"{cost_category} expense for {campaign['campaign_name']}",
                    "created_at": datetime.now()
                }
                marketing_costs.append(cost_record)
                marketing_cost_id += 1
                
                current_date += timedelta(days=random.randint(1, 7))
        
        return pd.DataFrame(marketing_costs)
    
    def load_fact_data(self) -> None:
        """Load fact data into BigQuery"""
        self.logger.info("Loading fact data into BigQuery...")
        
        for table_name, df in self.data_cache.items():
            if table_name.startswith("fact_"):
                # Load in batches for large datasets
                batch_size = 1000
                total_rows = len(df)
                
                for i in range(0, total_rows, batch_size):
                    batch_df = df.iloc[i:i+batch_size]
                    write_disposition = "WRITE_APPEND" if i > 0 else "WRITE_TRUNCATE"
                    self.bq_manager.load_dataframe(batch_df, table_name, write_disposition)
                
                self.logger.info(f"Loaded {total_rows} rows into {table_name}")
        
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
                self.bq_manager.load_dataframe(today_sales_df, "fact_sales", "WRITE_APPEND")
                self.logger.info(f"Added {len(today_sales_df)} new sales records")
            
            self.logger.info("Incremental update completed")
            
        except Exception as e:
            self.logger.error(f"Incremental update failed: {e}")
            raise
    
    def _generate_daily_sales(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Generate daily sales for incremental updates"""
        daily_amount = config.get("daily_sales_amount", 2000000)
        
        # Get reference data from BigQuery
        products = self.bq_manager.execute_query("SELECT * FROM dim_products WHERE status = 'Active'")
        retailers = self.bq_manager.execute_query("SELECT * FROM dim_retailers WHERE status = 'Active'")
        employees = self.bq_manager.execute_query("SELECT * FROM dim_employees WHERE termination_date IS NULL")
        campaigns = self.bq_manager.execute_query("SELECT * FROM dim_campaigns WHERE status = 'Active'")
        
        sales = []
        sale_id = 1
        current_date = datetime.now().date()
        
        # Generate daily sales
        num_transactions = random.randint(50, 200)
        amount_generated = 0
        
        while amount_generated < daily_amount and len(sales) < num_transactions:
            product = products.sample(1).iloc[0]
            retailer = retailers.sample(1).iloc[0]
            employee = employees.sample(1).iloc[0]
            
            # Random campaign assignment (30% chance)
            campaign = None
            if random.random() < 0.3 and len(campaigns) > 0:
                campaign = campaigns.sample(1).iloc[0]
            
            quantity = random.randint(1, 50)
            unit_price = product["unit_price"]
            total_amount = quantity * unit_price
            
            # Apply discount (10% chance)
            discount = 0
            if random.random() < 0.1:
                discount = total_amount * random.uniform(0.05, 0.20)
                total_amount -= discount
            
            # Commission rate (5-15%)
            commission_rate = random.uniform(0.05, 0.15)
            
            # Delivery date (1-7 days from now)
            delivery_date = current_date + timedelta(days=random.randint(1, 7))
            delivery_status = "Pending"
            
            sale = {
                "sale_id": sale_id,
                "date": current_date,
                "product_id": product["product_id"],
                "retailer_id": retailer["retailer_id"],
                "employee_id": employee["employee_id"],
                "campaign_id": campaign["campaign_id"] if campaign is not None else None,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "discount_amount": discount if discount > 0 else None,
                "commission_rate": commission_rate,
                "order_date": current_date,
                "delivery_date": delivery_date,
                "delivery_status": delivery_status,
                "created_at": datetime.now()
            }
            sales.append(sale)
            amount_generated += total_amount
            sale_id += 1
        
        return pd.DataFrame(sales)
