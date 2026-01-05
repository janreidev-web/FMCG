"""
Quick validation script for FMCG Data Analytics Platform
Fast pre-deployment checks
"""

import sys
import os
from pathlib import Path

def check_imports():
    """Check all critical imports"""
    print("🔍 Checking imports...")
    
    try:
        # Add src to path
        project_root = Path(__file__).parent.parent
        sys.path.insert(0, str(project_root))
        sys.path.insert(0, str(project_root / "src"))
        
        # Test imports
        from src.utils.bigquery_client import BigQueryManager
        from src.utils.logger import setup_logger
        from src.data.schemas import ALL_SCHEMAS
        from src.core.generators import LocationGenerator
        from src.etl.pipeline import ETLPipeline
        from config.settings import settings
        
        print("✅ All imports successful")
        return True
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

def check_configuration():
    """Check configuration"""
    print("🔍 Checking configuration...")
    
    try:
        from config.settings import settings
        
        print(f"  Project ID: {settings.gcp_project_id}")
        print(f"  Dataset: {settings.gcp_dataset}")
        print(f"  Employees: {settings.initial_employees}")
        print(f"  Products: {settings.initial_products}")
        
        print("✅ Configuration OK")
        return True
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

def check_schemas():
    """Check data schemas"""
    print("🔍 Checking schemas...")
    
    try:
        from src.data.schemas import ALL_SCHEMAS
        
        required_tables = [
            'dim_employees', 'dim_products', 'dim_retailers', 'dim_campaigns',
            'fact_sales', 'fact_inventory', 'fact_operating_costs'
        ]
        
        for table in required_tables:
            if table not in ALL_SCHEMAS:
                print(f"❌ Missing schema: {table}")
                return False
        
        print(f"✅ All {len(ALL_SCHEMAS)} schemas present")
        return True
        
    except Exception as e:
        print(f"❌ Schema error: {e}")
        return False

def check_generators():
    """Check data generators"""
    print("🔍 Checking generators...")
    
    try:
        from faker import Faker
        from src.core.generators import LocationGenerator, DepartmentGenerator
        
        faker = Faker()
        
        # Test location generator
        loc_gen = LocationGenerator(faker)
        locations = loc_gen.generate_locations(2)
        assert len(locations) == 2
        
        # Test department generator
        dept_gen = DepartmentGenerator(faker)
        departments = dept_gen.generate_departments()
        assert len(departments) > 0
        
        print("✅ Generators working")
        return True
        
    except Exception as e:
        print(f"❌ Generator error: {e}")
        return False

def check_dependencies():
    """Check required dependencies"""
    print("🔍 Checking dependencies...")
    
    required_packages = [
        'pandas', 'faker', 'google.cloud.bigquery', 
        'pydantic', 'pydantic_settings', 'rich'
    ]
    
    missing = []
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing.append(package)
    
    if missing:
        print(f"❌ Missing packages: {', '.join(missing)}")
        return False
    
    print("✅ All dependencies available")
    return True

def main():
    """Run quick validation"""
    print("FMCG Data Analytics Platform - Quick Validation")
    print("=" * 50)
    
    checks = [
        check_dependencies,
        check_imports,
        check_configuration,
        check_schemas,
        check_generators,
    ]
    
    results = []
    for check in checks:
        results.append(check())
        print()
    
    passed = sum(results)
    total = len(results)
    
    print("=" * 50)
    print(f"Validation Results: {passed}/{total} checks passed")
    
    if passed == total:
        print("🎉 Quick validation PASSED!")
        return 0
    else:
        print("⚠️  Quick validation FAILED!")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
