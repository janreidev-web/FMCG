#!/usr/bin/env python3
"""
Comprehensive Deep Check for Referencing Issues
Validates all field references, queries, and data consistency
"""

import sys
import os
import re
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'FMCG'))

class ReferenceChecker:
    def __init__(self):
        self.issues = []
        self.warnings = []
        self.fixes = []
        
    def log_issue(self, severity, component, description, details=None, fix=None):
        """Log an issue found"""
        issue = {
            'severity': severity,
            'component': component,
            'description': description,
            'details': details or {},
            'fix': fix
        }
        
        if severity == 'CRITICAL':
            self.issues.append(issue)
        else:
            self.warnings.append(issue)
            
        if fix:
            self.fixes.append(fix)
    
    def check_config_references(self):
        """Check configuration file for consistency"""
        print("🔍 Checking Configuration References...")
        
        try:
            with open('FMCG/config.py', 'r', encoding='utf-8') as f:
                config_content = f.read()
            
            # Check for old field references
            old_fields = ['category', 'subcategory', 'brand']
            new_fields = ['category_key', 'subcategory_key', 'brand_key']
            
            for old_field in old_fields:
                # Only flag if it's not in table names or config variables
                if old_field in config_content and old_field not in ['DAILY_SALES_AMOUNT', 'INITIAL_SALES_AMOUNT', 'DIM_BRANDS']:
                    # Check if it's used as a field reference (not table name)
                    lines = config_content.split('\n')
                    for i, line in enumerate(lines):
                        if old_field in line and not any(prefix in line for prefix in ['DIM_', 'FACT_', 'TABLE']):
                            self.log_issue('HIGH', 'config.py', f'Old field reference found: {old_field}',
                                         {'line': i + 1},
                                         f'Replace {old_field} with appropriate new field')
            
            # Check for table name consistency
            expected_tables = [
                'DIM_EMPLOYEES', 'DIM_PRODUCTS', 'DIM_RETAILERS', 'DIM_CAMPAIGNS',
                'DIM_LOCATIONS', 'DIM_DEPARTMENTS', 'DIM_JOBS', 'DIM_BANKS', 'DIM_INSURANCE',
                'DIM_CATEGORIES', 'DIM_BRANDS', 'DIM_SUBCATEGORIES', 'DIM_DATES'
            ]
            
            for table in expected_tables:
                if table not in config_content:
                    self.log_issue('CRITICAL', 'config.py', f'Missing table reference: {table}',
                                 fix=f'Add {table} = f"{PROJECT_ID}.{DATASET}.table_name"')
            
            print(f"✅ Configuration check completed")
            
        except Exception as e:
            self.log_issue('CRITICAL', 'config.py', f'Error reading config: {e}')
    
    def check_main_py_references(self):
        """Check main.py for field reference issues"""
        print("🔍 Checking Main.py References...")
        
        try:
            with open('FMCG/main.py', 'r', encoding='utf-8') as f:
                main_content = f.read()
            
            # Check for old field names in queries
            old_field_patterns = [
                r'SELECT.*\bcategory\b(?!\w)', 
                r'SELECT.*\bsubcategory\b(?!\w)',
                r'SELECT.*\bbrand\b(?!\w)',
                r'\bcategory\b(?!\w).*FROM',
                r'\bsubcategory\b(?!\w).*FROM',
                r'\bbrand\b(?!\w).*FROM'
            ]
            
            for pattern in old_field_patterns:
                matches = re.findall(pattern, main_content, re.IGNORECASE)
                if matches:
                    for match in matches:
                        self.log_issue('CRITICAL', 'main.py', f'Old field reference in query: {match}',
                                     {'pattern': pattern},
                                     f'Update to use *_key fields (category_key, subcategory_key, brand_key)')
            
            # Check for proper table references
            table_references = re.findall(r'FROM\s+`([^`]+)`', main_content)
            expected_table_patterns = [
                'dim_employees', 'dim_products', 'dim_retailers', 'dim_campaigns',
                'dim_locations', 'dim_departments', 'dim_jobs', 'dim_banks', 'dim_insurance',
                'dim_categories', 'dim_brands', 'dim_subcategories', 'dim_dates',
                'fact_sales', 'fact_employees', 'fact_employee_wages',
                'fact_operating_costs', 'fact_marketing_costs', 'fact_inventory'
            ]
            
            for ref in table_references:
                table_name = ref.split('.')[-1]  # Get just the table name
                if not any(pattern in table_name for pattern in expected_table_patterns):
                    self.log_issue('WARNING', 'main.py', f'Unexpected table reference: {ref}',
                                 {'table': table_name})
            
            # Check for hardcoded field names that should be normalized
            hardcoded_patterns = [
                r'product\["category"\]',
                r'product\["subcategory"\]',
                r'product\["brand"\]',
                r'\.category\s*=', 
                r'\.subcategory\s*=',
                r'\.brand\s*='
            ]
            
            for pattern in hardcoded_patterns:
                if re.search(pattern, main_content):
                    self.log_issue('HIGH', 'main.py', f'Hardcoded old field reference: {pattern}',
                                 fix='Update to use foreign key fields')
            
            print(f"✅ Main.py check completed")
            
        except Exception as e:
            self.log_issue('CRITICAL', 'main.py', f'Error reading main.py: {e}')
    
    def check_dimensional_py_references(self):
        """Check dimensional.py for consistency"""
        print("🔍 Checking Dimensional.py References...")
        
        try:
            with open('FMCG/generators/dimensional.py', 'r', encoding='utf-8') as f:
                dim_content = f.read()
            
            # Check product generation function
            if 'generate_dim_products' in dim_content:
                # Check if it uses the new signature
                if 'categories, brands, subcategories' not in dim_content:
                    self.log_issue('CRITICAL', 'dimensional.py', 
                                 'generate_dim_products function signature not updated',
                                 fix='Update function to accept categories, brands, subcategories parameters')
                
                # Check for old field usage in product generation (but allow it in product_data lookup)
                old_field_usage = re.findall(r'product\[["\'](category|subcategory|brand)["\']\]', dim_content)
                if old_field_usage:
                    # Check if these are in the context of lookups (which is correct)
                    context_lines = dim_content.split('\n')
                    for i, line in enumerate(context_lines):
                        if 'product[' in line and any(field in line for field in ['category', 'subcategory', 'brand']):
                            # Check if this is a lookup context (which is correct)
                            if 'lookup.get(' in line or 'category_lookup' in line or 'brand_lookup' in line or 'subcategory_lookup' in line:
                                continue  # This is correct usage
                            else:
                                self.log_issue('CRITICAL', 'dimensional.py', 
                                             f'Incorrect old field usage in product generation: {line.strip()}',
                                             fix='Use foreign key lookups instead')
                            break
            
            # Check validation function signature
            if 'validate_relationships' in dim_content:
                expected_params = 'categories, brands, subcategories'
                if expected_params not in dim_content:
                    self.log_issue('HIGH', 'dimensional.py', 
                                 'validate_relationships missing new parameters',
                                 fix=f'Add {expected_params} to function signature')
            
            # Check for proper foreign key usage
            fk_patterns = [
                r'category_key',
                r'brand_key', 
                r'subcategory_key'
            ]
            
            for pattern in fk_patterns:
                if pattern not in dim_content:
                    self.log_issue('CRITICAL', 'dimensional.py', 
                                 f'Missing foreign key field: {pattern}',
                                 fix=f'Add {pattern} to relevant functions')
            
            print(f"✅ Dimensional.py check completed")
            
        except Exception as e:
            self.log_issue('CRITICAL', 'dimensional.py', f'Error reading dimensional.py: {e}')
    
    def check_helpers_py_references(self):
        """Check helpers.py for BigQuery compatibility"""
        print("🔍 Checking Helpers.py References...")
        
        try:
            with open('FMCG/helpers.py', 'r', encoding='utf-8') as f:
                helpers_content = f.read()
            
            # Check for timeout handling
            if 'timeout' not in helpers_content.lower():
                self.log_issue('HIGH', 'helpers.py', 'Missing timeout handling in BigQuery operations',
                             fix='Add timeout parameter to job.result() calls')
            
            # Check for proper error handling
            if 'TimeoutError' not in helpers_content:
                self.log_issue('WARNING', 'helpers.py', 'Missing TimeoutError handling',
                             fix='Add proper timeout error handling')
            
            print(f"✅ Helpers.py check completed")
            
        except Exception as e:
            self.log_issue('CRITICAL', 'helpers.py', f'Error reading helpers.py: {e}')
    
    def check_github_actions_references(self):
        """Check GitHub Actions workflow"""
        print("🔍 Checking GitHub Actions References...")
        
        try:
            with open('.github/workflows/simulator.yml', 'r', encoding='utf-8') as f:
                workflow_content = f.read()
            
            # Check for environment variable usage
            env_vars = [
                'INITIAL_SALES_AMOUNT',
                'DAILY_SALES_AMOUNT',
                'GCP_PROJECT_ID',
                'BQ_DATASET'
            ]
            
            for var in env_vars:
                if var not in workflow_content:
                    self.log_issue('WARNING', 'simulator.yml', f'Missing environment variable: {var}',
                                 fix=f'Add {var} to environment variables')
            
            # Check for proper timeout settings
            if 'timeout' not in workflow_content.lower():
                self.log_issue('WARNING', 'simulator.yml', 'Missing timeout configuration',
                             fix='Add timeout to python execution')
            
            print(f"✅ GitHub Actions check completed")
            
        except Exception as e:
            self.log_issue('CRITICAL', 'simulator.yml', f'Error reading workflow: {e}')
    
    def check_database_schema_consistency(self):
        """Check for schema consistency issues"""
        print("🔍 Checking Database Schema Consistency...")
        
        # Expected field mappings
        expected_mappings = {
            'dim_products': {
                'old_fields': ['category', 'subcategory', 'brand'],
                'new_fields': ['category_key', 'brand_key', 'subcategory_key']
            },
            'dim_employees': {
                'expected_fks': ['location_key', 'job_key', 'bank_key', 'insurance_key']
            },
            'dim_retailers': {
                'expected_fks': ['location_key']
            }
        }
        
        # This would require actual database connection for full validation
        # For now, we'll check the code for consistency
        try:
            with open('FMCG/main.py', 'r', encoding='utf-8') as f:
                main_content = f.read()
            
            # Check if queries use the right field names
            for table, mapping in expected_mappings.items():
                if 'old_fields' in mapping:
                    for old_field in mapping['old_fields']:
                        # Look for SELECT statements with old fields (more precise pattern)
                        pattern = f'SELECT.*\\b{old_field}\\b(?=\\s|,|$).*FROM.*{table}'
                        if re.search(pattern, main_content, re.IGNORECASE):
                            self.log_issue('CRITICAL', 'Schema', f'Query uses old field {old_field} for {table}',
                                         fix=f'Update to use {mapping["new_fields"]}')
            
            print(f"✅ Schema consistency check completed")
            
        except Exception as e:
            self.log_issue('WARNING', 'Schema', f'Could not fully validate schema: {e}')
    
    def check_import_consistency(self):
        """Check import statements for consistency"""
        print("🔍 Checking Import Consistency...")
        
        try:
            with open('FMCG/main.py', 'r', encoding='utf-8') as f:
                main_content = f.read()
            
            # Check for new dimension imports
            required_imports = [
                'generate_dim_categories',
                'generate_dim_brands', 
                'generate_dim_subcategories'
            ]
            
            for import_name in required_imports:
                if import_name not in main_content:
                    self.log_issue('CRITICAL', 'main.py', f'Missing import: {import_name}',
                                 fix=f'Add {import_name} to dimensional imports')
            
            # Check for config imports
            required_config = [
                'DIM_CATEGORIES',
                'DIM_BRANDS',
                'DIM_SUBCATEGORIES'
            ]
            
            for config_name in required_config:
                if config_name not in main_content:
                    self.log_issue('CRITICAL', 'main.py', f'Missing config import: {config_name}',
                                 fix=f'Add {config_name} to config imports')
            
            print(f"✅ Import consistency check completed")
            
        except Exception as e:
            self.log_issue('CRITICAL', 'Imports', f'Error checking imports: {e}')
    
    def find_line_number(self, content, search_text):
        """Find line number of text in content"""
        lines = content.split('\n')
        for i, line in enumerate(lines, 1):
            if search_text in line:
                return i
        return 0
    
    def run_comprehensive_check(self):
        """Run all reference checks"""
        print("🚀 Starting Comprehensive Reference Check")
        print("=" * 60)
        
        start_time = time.time()
        
        # Run all checks
        self.check_config_references()
        self.check_main_py_references()
        self.check_dimensional_py_references()
        self.check_helpers_py_references()
        self.check_github_actions_references()
        self.check_database_schema_consistency()
        self.check_import_consistency()
        
        elapsed_time = time.time() - start_time
        
        # Generate report
        self.generate_report(elapsed_time)
    
    def generate_report(self, elapsed_time):
        """Generate comprehensive report"""
        print("\n" + "=" * 60)
        print("📊 COMPREHENSIVE REFERENCE CHECK REPORT")
        print("=" * 60)
        
        total_issues = len(self.issues) + len(self.warnings)
        critical_issues = len([i for i in self.issues if i['severity'] == 'CRITICAL'])
        high_issues = len([i for i in self.issues if i['severity'] == 'HIGH'])
        
        print(f"\n📈 Summary:")
        print(f"   Total Issues Found: {total_issues}")
        print(f"   Critical Issues: {critical_issues} 🚨")
        print(f"   High Priority Issues: {high_issues} ⚠️")
        print(f"   Warnings: {len(self.warnings)} ℹ️")
        print(f"   Check Time: {elapsed_time:.2f}s")
        
        if critical_issues > 0:
            print(f"\n🚨 CRITICAL ISSUES (Must Fix):")
            for issue in self.issues:
                if issue['severity'] == 'CRITICAL':
                    print(f"   • {issue['component']}: {issue['description']}")
                    if issue['details']:
                        for key, value in issue['details'].items():
                            print(f"     - {key}: {value}")
                    if issue['fix']:
                        print(f"     💡 Fix: {issue['fix']}")
        
        if high_issues > 0:
            print(f"\n⚠️  HIGH PRIORITY ISSUES:")
            for issue in self.issues:
                if issue['severity'] == 'HIGH':
                    print(f"   • {issue['component']}: {issue['description']}")
                    if issue['fix']:
                        print(f"     💡 Fix: {issue['fix']}")
        
        if self.warnings:
            print(f"\nℹ️  WARNINGS:")
            for warning in self.warnings:
                print(f"   • {warning['component']}: {warning['description']}")
        
        if self.fixes:
            print(f"\n🔧 RECOMMENDED FIXES:")
            for i, fix in enumerate(self.fixes, 1):
                print(f"   {i}. {fix}")
        
        # Overall status
        if critical_issues == 0 and high_issues == 0:
            status = "✅ READY FOR PRODUCTION"
            print(f"\n🎉 Overall Status: {status}")
        elif critical_issues == 0:
            status = "⚠️  NEEDS ATTENTION"
            print(f"\n⚠️  Overall Status: {status}")
        else:
            status = "🚨 CRITICAL ISSUES FOUND"
            print(f"\n🚨 Overall Status: {status}")
        
        print("=" * 60)
        
        return critical_issues == 0 and high_issues == 0

if __name__ == "__main__":
    import time
    
    checker = ReferenceChecker()
    is_ready = checker.run_comprehensive_check()
    
    sys.exit(0 if is_ready else 1)
