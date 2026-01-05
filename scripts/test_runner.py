"""
Quick test runner for FMCG Data Analytics Platform
Run all tests and provide comprehensive debugging information
"""

import sys
import os
import subprocess
from pathlib import Path
import time

def run_command(command, description):
    """Run a command and return success status"""
    print(f"\n{'='*60}")
    print(f"TEST: {description}")
    print(f"COMMAND: {command}")
    print('='*60)
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            print(f"✅ PASSED ({duration:.2f}s)")
            if result.stdout:
                print("OUTPUT:")
                print(result.stdout[-500:])  # Last 500 chars
        else:
            print(f"❌ FAILED ({duration:.2f}s)")
            print("ERROR OUTPUT:")
            print(result.stderr[-1000:])  # Last 1000 chars
            if result.stdout:
                print("STDOUT:")
                print(result.stdout[-500:])
        
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print(f"⏰ TIMEOUT (300s)")
        return False
    except Exception as e:
        print(f"💥 ERROR: {e}")
        return False

def main():
    """Run comprehensive test suite"""
    print("FMCG Data Analytics Platform - Comprehensive Test Runner")
    print("=" * 60)
    
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)
    
    # Test results
    results = []
    
    # 1. Python Syntax and Import Tests
    tests = [
        ("py -m py_compile src/main.py", "Main script syntax check"),
        ("py -m py_compile src/utils/bigquery_client.py", "BigQuery client syntax check"),
        ("py -m py_compile src/etl/pipeline.py", "ETL pipeline syntax check"),
        ("py -m py_compile src/core/generators.py", "Generators syntax check"),
        ("py -m py_compile src/data/schemas.py", "Schemas syntax check"),
        ("py -m py_compile config/settings.py", "Settings syntax check"),
        ("py -m py_compile scripts/setup.py", "Setup script syntax check"),
    ]
    
    for cmd, desc in tests:
        results.append((run_command(cmd, desc), desc))
    
    # 2. Import Tests
    import_tests = [
        ("py -c \"from src.utils.bigquery_client import BigQueryManager; print('✅ BigQuery client import OK')\"", "BigQuery client import"),
        ("py -c \"from src.etl.pipeline import ETLPipeline; print('✅ ETL pipeline import OK')\"", "ETL pipeline import"),
        ("py -c \"from src.core.generators import LocationGenerator; print('✅ Generators import OK')\"", "Generators import"),
        ("py -c \"from src.data.schemas import ALL_SCHEMAS; print('✅ Schemas import OK')\"", "Schemas import"),
        ("py -c \"from config.settings import settings; print('✅ Settings import OK')\"", "Settings import"),
    ]
    
    for cmd, desc in import_tests:
        results.append((run_command(cmd, desc), desc))
    
    # 3. Configuration Tests
    config_tests = [
        ("py -c \"from config.settings import settings; print(f'Project: {settings.gcp_project_id}'); print(f'Dataset: {settings.gcp_dataset}')\"", "Default configuration"),
        ("py -c \"import os; os.environ['GCP_PROJECT_ID']='test'; from config.settings import Settings; s=Settings(); print(f'Test project: {s.gcp_project_id}')\"", "Environment variable override"),
    ]
    
    for cmd, desc in config_tests:
        results.append((run_command(cmd, desc), desc))
    
    # 4. Data Generation Tests (small scale)
    data_tests = [
        ("py -c \"from src.core.generators import LocationGenerator; from faker import Faker; g=LocationGenerator(Faker()); df=g.generate_locations(5); print(f'Generated {len(df)} locations')\"", "Location generation test"),
        ("py -c \"from src.core.generators import DepartmentGenerator; from faker import Faker; g=DepartmentGenerator(Faker()); df=g.generate_departments(); print(f'Generated {len(df)} departments')\"", "Department generation test"),
        ("py -c \"from src.core.generators import ProductGenerator; from faker import Faker; g=ProductGenerator(Faker()); p,c,s,b=g.generate_products(3); print(f'Generated {len(p)} products, {len(c)} categories')\"", "Product generation test"),
    ]
    
    for cmd, desc in data_tests:
        results.append((run_command(cmd, desc), desc))
    
    # 5. Schema Validation Tests
    schema_tests = [
        ("py -c \"from src.data.schemas import ALL_SCHEMAS; print(f'Total schemas: {len(ALL_SCHEMAS)}'); [print(f'  - {name}') for name in ALL_SCHEMAS.keys()]\"", "Schema definitions check"),
        ("py -c \"from src.data.schemas import get_bigquery_schema; from src.data.schemas import ALL_SCHEMAS; schema=list(ALL_SCHEMAS.values())[0]; bq_schema=get_bigquery_schema(schema); print(f'BigQuery schema fields: {len(bq_schema)}')\"", "BigQuery schema conversion"),
    ]
    
    for cmd, desc in schema_tests:
        results.append((run_command(cmd, desc), desc))
    
    # 6. Setup Script Tests
    setup_tests = [
        ("py scripts/setup.py --help 2>/dev/null || py scripts/setup.py", "Setup script execution"),
    ]
    
    for cmd, desc in setup_tests:
        results.append((run_command(cmd, desc), desc))
    
    # 7. Pytest Tests (if available)
    pytest_cmd = "py -m pytest tests/ -v --tb=short --maxfail=5"
    results.append((run_command(pytest_cmd, "Pytest comprehensive test suite"), "Pytest test suite"))
    
    # 8. Dependency Check
    dep_check = "py -c \"import pandas, faker, google.cloud.bigquery, pydantic, pydantic_settings, rich; print('✅ All dependencies available')\""
    results.append((run_command(dep_check, "Dependency availability check"), "Dependencies check"))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for passed, _ in results if passed)
    total = len(results)
    
    for passed, desc in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {desc}")
    
    print(f"\nResults: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! Ready for deployment.")
        return 0
    else:
        print(f"⚠️  {total - passed} tests failed. Fix issues before deployment.")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
