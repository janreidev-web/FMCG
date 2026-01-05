"""
Configuration settings for FMCG Data Analytics Platform
"""

import os
from typing import Optional
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # Google Cloud Configuration
    gcp_project_id: str = Field(default="fmcg-data-generator", env="GCP_PROJECT_ID")
    gcp_dataset: str = Field(default="fmcg_warehouse", env="GCP_DATASET")
    gcp_credentials_path: Optional[str] = Field(default=None, env="GOOGLE_APPLICATION_CREDENTIALS")
    
    # Business Configuration
    initial_employees: int = Field(default=350, env="INITIAL_EMPLOYEES")
    initial_products: int = Field(default=150, env="INITIAL_PRODUCTS")
    initial_retailers: int = Field(default=500, env="INITIAL_RETAILERS")
    
    # Sales Configuration
    initial_sales_amount: float = Field(default=8000000000, env="INITIAL_SALES_AMOUNT")
    daily_sales_amount: float = Field(default=2000000, env="DAILY_SALES_AMOUNT")
    
    # Data Generation Configuration
    new_employees_per_month: tuple[int, int] = (2, 15)
    new_products_per_month: tuple[int, int] = (1, 5)
    new_campaigns_per_quarter: tuple[int, int] = (2, 8)
    
    # Technical Configuration
    batch_size: int = Field(default=1000, env="BATCH_SIZE")
    max_retries: int = Field(default=3, env="MAX_RETRIES")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()
