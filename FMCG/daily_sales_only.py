#!/usr/bin/env python3
"""
Daily Sales Only Script
Purpose: Append new daily sales data by checking the last ID in the database
This script does NOT generate dimensions or other fact tables - only sales.
"""

import os
import sys
import logging
import pandas as pd
from datetime import date, timedelta
from google.cloud import bigquery

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import required modules
from auth import get_bigquery_client
from helpers import append_df_bq, table_has_data, generate_readable_id
from dimensional import generate_fact_sales
from config import (
    PROJECT_ID, DATASET, 
    FACT_SALES, DIM_PRODUCTS, DIM_RETAILERS, DIM_EMPLOYEES, 
    INITIAL_SALES_AMOUNT, DAILY_SALES_AMOUNT
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_last_sale_id(client):
    """Get the last sale_id from the fact_sales table"""
    try:
        query = f"""
        SELECT MAX(sale_id) as last_sale_id 
        FROM `{PROJECT_ID}.{DATASET}.{FACT_SALES}`
        """
        result = client.query(query).to_dataframe()
        
        if result['last_sale_id'].iloc[0] and pd.notna(result['last_sale_id'].iloc[0]):
            return result['last_sale_id'].iloc[0]
        else:
            return None
    except Exception as e:
        logger.warning(f"Could not get last sale ID: {e}")
        return None

def get_last_sale_date(client):
    """Get the last sale_date from the fact_sales table"""
    try:
        query = f"""
        SELECT MAX(sale_date) as last_sale_date 
        FROM `{PROJECT_ID}.{DATASET}.{FACT_SALES}`
        """
        result = client.query(query).to_dataframe()
        
        if result['last_sale_date'].iloc[0] and pd.notna(result['last_sale_date'].iloc[0]):
            return result['last_sale_date'].iloc[0].date()
        else:
            return None
    except Exception as e:
        logger.warning(f"Could not get last sale date: {e}")
        return None

def load_reference_data(client):
    """Load required reference data for sales generation"""
    try:
        # Load products
        products_df = client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{DIM_PRODUCTS}`").to_dataframe()
        products = products_df.to_dict("records")
        logger.info(f"Loaded {len(products)} products")
        
        # Load retailers
        retailers_df = client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{DIM_RETAILERS}`").to_dataframe()
        retailers = retailers_df.to_dict("records")
        logger.info(f"Loaded {len(retailers)} retailers")
        
        # Load employees
        employees_df = client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{DIM_EMPLOYEES}`").to_dataframe()
        employees = employees_df.to_dict("records")
        logger.info(f"Loaded {len(employees)} employees")
        
        return products, retailers, employees
        
    except Exception as e:
        logger.error(f"Error loading reference data: {e}")
        raise

def generate_daily_sales(client, products, retailers, employees, last_sale_date=None):
    """Generate daily sales data"""
    try:
        # Determine date range for new sales
        if last_sale_date:
            start_date = last_sale_date + timedelta(days=1)
        else:
            start_date = date.today() - timedelta(days=1)  # Default to yesterday
        
        end_date = date.today() - timedelta(days=1)  # Generate up to yesterday
        
        if start_date > end_date:
            logger.info(f"No new sales needed. Last sale date: {last_sale_date}, Today: {date.today()}")
            return []
        
        logger.info(f"Generating sales from {start_date} to {end_date}")
        
        # Generate sales using existing function
        sales = generate_fact_sales(
            products=products,
            retailers=retailers,
            employees=employees,
            start_date=start_date,
            end_date=end_date,
            daily_target=DAILY_SALES_AMOUNT,
            existing_sales=[]
        )
        
        logger.info(f"Generated {len(sales):,} new sales records")
        return sales
        
    except Exception as e:
        logger.error(f"Error generating daily sales: {e}")
        raise

def main():
    """Main function for daily sales only"""
    try:
        logger.info("Starting Daily Sales Only Process")
        
        # Connect to BigQuery
        client = get_bigquery_client(PROJECT_ID)
        logger.info(f"Connected to: {PROJECT_ID}")
        
        # Check if sales table exists and has data
        if not table_has_data(client, FACT_SALES):
            logger.error("Sales table does not exist or has no data!")
            logger.error("Please run manual full refresh first to generate initial data.")
            sys.exit(1)
        
        # Get last sale info
        last_sale_id = get_last_sale_id(client)
        last_sale_date = get_last_sale_date(client)
        
        logger.info(f"Last sale ID: {last_sale_id}")
        logger.info(f"Last sale date: {last_sale_date}")
        
        # Load reference data
        products, retailers, employees = load_reference_data(client)
        
        # Generate new sales
        new_sales = generate_daily_sales(client, products, retailers, employees, last_sale_date)
        
        if not new_sales:
            logger.info("No new sales to append. Sales data is up to date.")
            return
        
        # Append new sales to BigQuery
        logger.info(f"Appending {len(new_sales):,} new sales records...")
        append_df_bq(client, pd.DataFrame(new_sales), FACT_SALES)
        
        logger.info("Daily sales process completed successfully!")
        logger.info(f"Total new sales added: {len(new_sales):,}")
        
    except Exception as e:
        logger.error(f"Daily sales process failed: {e}")
        logger.error(f"Failed at: {pd.Timestamp.now()}")
        sys.exit(1)

if __name__ == "__main__":
    main()
