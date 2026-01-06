"""
Configuration settings for FMCG Data Analytics Platform
"""

import os
from typing import Optional
from pydantic import Field


class Settings:
    """Application settings using GitHub Secrets and defaults"""
    
    def __init__(self):
        # Google Cloud Configuration from GitHub Secrets
        self.gcp_project_id = os.getenv('GCP_PROJECT_ID', 'fmcg-data-generator')
        self.gcp_dataset = os.getenv('GCP_DATASET', 'fmcg_warehouse')
        self.gcp_credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        
        # Business Configuration (can be set via GitHub Secrets or use defaults)
        self.initial_employees = int(os.getenv('INITIAL_EMPLOYEES', '350'))
        self.initial_products = int(os.getenv('INITIAL_PRODUCTS', '150'))
        self.initial_retailers = int(os.getenv('INITIAL_RETAILERS', '500'))
        
        # Sales Configuration
        self.initial_sales_amount = float(os.getenv('INITIAL_SALES_AMOUNT', '8000000000'))
        self.daily_sales_amount = float(os.getenv('DAILY_SALES_AMOUNT', '2000000'))
        
        # Data Generation Configuration (hardcoded defaults)
        self.new_employees_per_month = (2, 15)
        self.new_products_per_month = (1, 5)
        self.new_campaigns_per_quarter = (2, 8)
        
        # Technical Configuration
        self.batch_size = int(os.getenv('BATCH_SIZE', '1000'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')


# Global settings instance
settings = Settings()
