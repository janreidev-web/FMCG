"""
Setup script for FMCG Data Analytics Platform
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any

# Add parent directory to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

try:
    from src.utils.bigquery_client import BigQueryManager
    from src.utils.logger import setup_logger
    from config.settings import settings
except ImportError as e:
    print(f"Import error: {e}")
    print("Current working directory:", os.getcwd())
    print("Python path:", sys.path)
    print("Project root:", project_root)
    print("Src path:", project_root / "src")
    sys.exit(1)


def create_env_file() -> None:
    """Create .env file with default values"""
    env_content = f"""# Google Cloud Configuration
GCP_PROJECT_ID={settings.gcp_project_id}
GCP_DATASET={settings.gcp_dataset}
# GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account-key.json

# Business Configuration
INITIAL_EMPLOYEES={settings.initial_employees}
INITIAL_PRODUCTS={settings.initial_products}
INITIAL_RETAILERS={settings.initial_retailers}

# Sales Configuration
INITIAL_SALES_AMOUNT={settings.initial_sales_amount}
DAILY_SALES_AMOUNT={settings.daily_sales_amount}

# Technical Configuration
BATCH_SIZE={settings.batch_size}
MAX_RETRIES={settings.max_retries}
LOG_LEVEL={settings.log_level}
"""
    
    env_file = Path(__file__).parent.parent / ".env"
    
    if not env_file.exists():
        with open(env_file, 'w') as f:
            f.write(env_content)
        print(f"Created .env file at {env_file}")
        print("Please update the configuration values as needed")
    else:
        print(f".env file already exists at {env_file}")


def setup_bigquery() -> None:
    """Setup BigQuery dataset and tables"""
    logger = setup_logger("setup", "INFO")
    
    try:
        # Initialize BigQuery manager
        bq_manager = BigQueryManager(
            project_id=settings.gcp_project_id,
            dataset=settings.gcp_dataset,
            credentials_path=settings.gcp_credentials_path
        )
        
        # Ensure dataset exists
        dataset = bq_manager.ensure_dataset()
        logger.info(f"Dataset '{dataset.dataset_id}' is ready")
        
        print("BigQuery setup completed successfully!")
        print(f"Project: {settings.gcp_project_id}")
        print(f"Dataset: {settings.gcp_dataset}")
        
    except Exception as e:
        logger.error(f"BigQuery setup failed: {e}")
        print(f"Setup failed: {e}")
        sys.exit(1)


def verify_configuration() -> None:
    """Verify configuration settings"""
    logger = setup_logger("setup", "INFO")
    
    print("Verifying configuration...")
    
    # Check environment variables
    required_vars = [
        "GCP_PROJECT_ID",
        "GCP_DATASET"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set these variables in your environment or .env file")
        return False
    
    # Test BigQuery connection
    try:
        bq_manager = BigQueryManager(
            project_id=settings.gcp_project_id,
            dataset=settings.gcp_dataset,
            credentials_path=settings.gcp_credentials_path
        )
        
        # Test connection with a simple query
        bq_manager.client.query("SELECT 1").result()
        print("✓ BigQuery connection successful")
        
    except Exception as e:
        print(f"✗ BigQuery connection failed: {e}")
        return False
    
    print("✓ Configuration verified successfully")
    return True


def main() -> None:
    """Main setup function"""
    print("FMCG Data Analytics Platform - Setup")
    print("="*50)
    
    # Create .env file
    create_env_file()
    
    # Verify configuration
    if not verify_configuration():
        print("\nSetup failed. Please fix configuration issues and try again.")
        sys.exit(1)
    
    # Setup BigQuery
    setup_bigquery()
    
    print("\nSetup completed successfully!")
    print("\nNext steps:")
    print("1. Update .env file with your configuration")
    print("2. Run: python src/main.py")
    print("3. For incremental updates: python src/main.py --incremental")


if __name__ == "__main__":
    main()
