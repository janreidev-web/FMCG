"""
Main entry point for FMCG Data Analytics Platform
"""

import sys
import os
from pathlib import Path
from typing import Dict, Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.bigquery_client import BigQueryManager
from src.utils.logger import setup_logger
from src.etl.pipeline import ETLPipeline
from config.settings import settings


def main() -> None:
    """Main application entry point"""
    # Setup logging
    logger = setup_logger("fmcg_analytics", settings.log_level)
    
    try:
        # Initialize BigQuery manager
        bq_manager = BigQueryManager(
            project_id=settings.gcp_project_id,
            dataset=settings.gcp_dataset,
            credentials_path=settings.gcp_credentials_path
        )
        
        # Initialize ETL pipeline
        pipeline = ETLPipeline(bq_manager)
        
        # Configuration for data generation
        config = {
            "initial_employees": settings.initial_employees,
            "initial_products": settings.initial_products,
            "initial_retailers": settings.initial_retailers,
            "initial_campaigns": 50,
            "locations_count": 100,
            "initial_sales_amount": settings.initial_sales_amount,
            "daily_sales_amount": settings.daily_sales_amount,
        }
        
        # Check if this is initial run or incremental update
        if len(sys.argv) > 1 and sys.argv[1] == "--incremental":
            logger.info("Running incremental update...")
            pipeline.run_incremental_update(config)
        else:
            logger.info("Running full ETL pipeline...")
            pipeline.run_full_pipeline(config)
        
        # Print summary
        print_summary(bq_manager)
        
    except Exception as e:
        logger.error(f"Application failed: {e}")
        sys.exit(1)


def print_summary(bq_manager: BigQueryManager) -> None:
    """Print data summary after pipeline completion"""
    print("\n" + "="*60)
    print("FMCG DATA ANALYTICS PLATFORM - SUMMARY")
    print("="*60)
    
    # Table summaries
    tables = [
        ("dim_employees", "Employees"),
        ("dim_products", "Products"),
        ("dim_retailers", "Retailers"),
        ("dim_campaigns", "Campaigns"),
        ("fact_sales", "Sales Transactions"),
        ("fact_inventory", "Inventory Records"),
        ("fact_operating_costs", "Operating Costs"),
        ("fact_marketing_costs", "Marketing Costs")
    ]
    
    for table_name, description in tables:
        if bq_manager.table_exists(table_name):
            row_count = bq_manager.get_table_row_count(table_name)
            print(f"{description:.<30} {row_count:>10,} rows")
        else:
            print(f"{description:.<30} {'Not found':>10}")
    
    print("="*60)
    print("Pipeline completed successfully!")
    print("="*60)


if __name__ == "__main__":
    main()
