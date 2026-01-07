#!/usr/bin/env python3
"""
Quarterly campaign update script for FMCG Data Analytics Platform
Focuses on new campaigns and marketing costs
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
    """Run quarterly campaign update"""
    
    logger = default_logger()
    logger.info("=== Starting Quarterly Campaign Update ===")
    
    # Get configuration from environment
    project_id = os.getenv('GCP_PROJECT_ID', 'fmcg-data-generator')
    dataset = os.getenv('BQ_DATASET', 'fmcg_warehouse')
    
    try:
        # Initialize BigQuery client
        bq_manager = BigQueryManager(project_id, dataset)
        logger.info(f"Connected to BigQuery: {project_id}.{dataset}")
        
        # Initialize pipeline
        pipeline = ETLPipeline(bq_manager)
        
        # Configuration for quarterly update
        config = {
            'quarterly_update': True,
            'new_campaigns_count': 1,  # 1 campaign per quarter
            'update_campaigns': True
        }
        
        logger.info("Configuration:")
        logger.info(f"  New campaigns: {config['new_campaigns_count']}")
        logger.info(f"  Update campaigns: {config['update_campaigns']}")
        
        # Step 1: Generate new campaigns (1 per quarter)
        logger.info("Step 1: Generating new campaigns...")
        new_campaigns_df = pipeline._generate_quarterly_campaigns(config)
        
        if len(new_campaigns_df) > 0:
            bq_manager.load_dataframe(new_campaigns_df, "dim_campaigns", "WRITE_APPEND")
            logger.info(f"✅ Added {len(new_campaigns_df)} new campaigns")
        else:
            logger.info("ℹ️  No new campaigns to add")
        
        # Step 2: Generate marketing costs for current quarter
        logger.info("Step 2: Generating quarterly marketing costs...")
        marketing_df = pipeline._generate_quarterly_marketing_costs(config)
        
        if len(marketing_df) > 0:
            bq_manager.load_dataframe(marketing_df, "fact_marketing_costs", "WRITE_APPEND")
            logger.info(f"✅ Added {len(marketing_df)} marketing cost records")
        else:
            logger.info("ℹ️  No marketing costs to add")
        
        logger.info("=== Quarterly Campaign Update Completed Successfully ===")
        logger.info(f"Summary:")
        logger.info(f"  New campaigns: {len(new_campaigns_df)}")
        logger.info(f"  Marketing costs: {len(marketing_df)}")
        
    except Exception as e:
        logger.error(f"Quarterly campaign update failed: {e}")
        raise

if __name__ == '__main__':
    main()
