#!/usr/bin/env python3
"""
Monthly update script for FMCG Data Analytics Platform
Focuses on cost-related data, products, inventory, and employees
"""

import os
import sys
import random
import logging
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.etl.pipeline import ETLPipeline
from src.utils.bigquery_client import BigQueryManager
from src.utils.logger import default_logger

def main():
    """Run monthly update for costs, products, and inventory"""
    
    logger = default_logger()
    logger.info("=== Starting Monthly Update (Costs, Products, Inventory) ===")
    
    # Get configuration from environment
    project_id = os.getenv('GCP_PROJECT_ID', 'fmcg-data-generator')
    dataset = os.getenv('BQ_DATASET', 'fmcg_warehouse')
    
    try:
        # Initialize BigQuery client
        bq_manager = BigQueryManager(project_id, dataset)
        logger.info(f"Connected to BigQuery: {project_id}.{dataset}")
        
        # Initialize pipeline
        pipeline = ETLPipeline(bq_manager)
        
        # Configuration for monthly update
        config = {
            'monthly_update': True,
            'new_products_count': 2,  # 1-2 new products per month
            'new_employees_count': random.randint(1, 3),  # 1-3 new employees per month
            'update_costs': True,
            'update_inventory': True,
            'update_products_only': True
        }
        
        logger.info("Configuration:")
        logger.info(f"  New products: {config['new_products_count']}")
        logger.info(f"  New employees: {config['new_employees_count']}")
        logger.info(f"  Update costs: {config['update_costs']}")
        logger.info(f"  Update inventory: {config['update_inventory']}")
        
        # Step 1: Generate new employees (1-3 per month)
        logger.info("Step 1: Generating new employees...")
        new_employees_df = pipeline._generate_monthly_employees(config)
        
        if len(new_employees_df) > 0:
            bq_manager.load_dataframe(new_employees_df, "dim_employees", "WRITE_APPEND")
            logger.info(f"✅ Added {len(new_employees_df)} new employees")
        else:
            logger.info("ℹ️  No new employees to add")
        
        # Step 2: Generate new products (1-2 per month)
        logger.info("Step 2: Generating new products...")
        new_products_df = pipeline._generate_monthly_products(config)
        
        if len(new_products_df) > 0:
            bq_manager.load_dataframe(new_products_df, "dim_products", "WRITE_APPEND")
            logger.info(f"✅ Added {len(new_products_df)} new products")
        else:
            logger.info("ℹ️  No new products to add")
        
        # Step 3: Generate operating costs for current month
        logger.info("Step 3: Generating operating costs...")
        costs_df = pipeline._generate_monthly_costs(config)
        
        if len(costs_df) > 0:
            bq_manager.load_dataframe(costs_df, "fact_operating_costs", "WRITE_APPEND")
            logger.info(f"✅ Added {len(costs_df)} operating cost records")
        else:
            logger.info("ℹ️  No operating costs to add")
        
        # Step 4: Generate inventory snapshots for current month
        logger.info("Step 4: Generating inventory snapshots...")
        inventory_df = pipeline._generate_monthly_inventory(config)
        
        if len(inventory_df) > 0:
            bq_manager.load_dataframe(inventory_df, "fact_inventory", "WRITE_APPEND")
            logger.info(f"✅ Added {len(inventory_df)} inventory records")
        else:
            logger.info("ℹ️  No inventory records to add")
        
        logger.info("=== Monthly Update Completed Successfully ===")
        logger.info(f"Summary:")
        logger.info(f"  New employees: {len(new_employees_df)}")
        logger.info(f"  New products: {len(new_products_df)}")
        logger.info(f"  Operating costs: {len(costs_df)}")
        logger.info(f"  Inventory records: {len(inventory_df)}")
        
    except Exception as e:
        logger.error(f"Monthly update failed: {e}")
        raise

if __name__ == '__main__':
    main()
