"""
FMCG Data Simulator - Dimensional Model Version
Optimized for BigQuery with reduced storage using star schema
"""

import sys
import os
import logging
import pandas as pd
from datetime import datetime, timedelta, date
import time

# Handle both relative and absolute imports
try:
    from .config import (
        PROJECT_ID, DATASET, INITIAL_EMPLOYEES, INITIAL_PRODUCTS, INITIAL_RETAILERS,
        EMPLOYEES_TABLE, PRODUCTS_TABLE, RETAILERS_TABLE, SALES_TABLE, COSTS_TABLE, INVENTORY_TABLE, MARKETING_TABLE, DATES_TABLE,
        DIM_EMPLOYEES, DIM_PRODUCTS, DIM_RETAILERS, DIM_CAMPAIGNS,
        DIM_LOCATIONS, DIM_DEPARTMENTS, DIM_JOBS, DIM_BANKS, DIM_INSURANCE,
        DIM_CATEGORIES, DIM_BRANDS, DIM_SUBCATEGORIES, DIM_DATES,
        FACT_SALES, FACT_OPERATING_COSTS, FACT_INVENTORY, FACT_MARKETING_COSTS, FACT_EMPLOYEES,
        INITIAL_SALES_AMOUNT, DAILY_SALES_AMOUNT
    )
    from .auth import get_bigquery_client
    from .helpers import table_has_data, append_df_bq, append_df_bq_safe, update_delivery_status
    from .generators.dimensional import (
        generate_dim_products, generate_dim_employees_normalized, generate_dim_locations,
        generate_dim_departments, generate_dim_jobs, generate_dim_banks, generate_dim_insurance,
        generate_fact_employees, generate_fact_employee_wages, generate_dim_retailers_normalized,
        generate_dim_campaigns, generate_fact_sales, generate_daily_sales_with_delivery_updates,
        generate_fact_operating_costs, generate_fact_inventory, generate_fact_marketing_costs,
        generate_dim_dates, generate_dim_categories, generate_dim_brands, generate_dim_subcategories,
        validate_relationships
    )
    from .generators.bigquery_updates import (
        execute_method_1_overwrite, execute_method_2_append, execute_method_3_staging,
        create_current_delivery_status_view, get_delivery_status_summary,
        compare_update_methods
    )
except ImportError:
    # Fallback to absolute imports when running as script
    from config import (
        PROJECT_ID, DATASET, INITIAL_EMPLOYEES, INITIAL_PRODUCTS, INITIAL_RETAILERS,
        EMPLOYEES_TABLE, PRODUCTS_TABLE, RETAILERS_TABLE, SALES_TABLE, COSTS_TABLE, INVENTORY_TABLE, MARKETING_TABLE, DATES_TABLE,
        DIM_EMPLOYEES, DIM_PRODUCTS, DIM_RETAILERS, DIM_CAMPAIGNS,
        DIM_LOCATIONS, DIM_DEPARTMENTS, DIM_JOBS, DIM_BANKS, DIM_INSURANCE,
        DIM_CATEGORIES, DIM_BRANDS, DIM_SUBCATEGORIES, DIM_DATES,
        FACT_SALES, FACT_OPERATING_COSTS, FACT_INVENTORY, FACT_MARKETING_COSTS, FACT_EMPLOYEES,
        INITIAL_SALES_AMOUNT, DAILY_SALES_AMOUNT
    )
    from auth import get_bigquery_client
    from helpers import table_has_data, append_df_bq, append_df_bq_safe, update_delivery_status
    from generators.dimensional import (
        generate_dim_products, generate_dim_employees_normalized, generate_dim_locations,
        generate_dim_departments, generate_dim_jobs, generate_dim_banks, generate_dim_insurance,
        generate_fact_employees, generate_fact_employee_wages, generate_dim_retailers_normalized,
        generate_dim_campaigns, generate_fact_sales, generate_daily_sales_with_delivery_updates,
        generate_fact_operating_costs, generate_fact_inventory, generate_fact_marketing_costs,
        generate_dim_dates, generate_dim_categories, generate_dim_brands, generate_dim_subcategories,
        validate_relationships
    )
    from generators.bigquery_updates import (
        execute_method_1_overwrite, execute_method_2_append, execute_method_3_staging,
        create_current_delivery_status_view, get_delivery_status_summary,
        compare_update_methods
    )

# Configure simplified logging for GitHub Actions
is_github_actions = os.environ.get('GITHUB_ACTIONS') == 'true'

if is_github_actions:
    # Simplified logging for GitHub Actions
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s: %(message)s',
        handlers=[logging.StreamHandler()]
    )
else:
    # Enhanced logging for local development
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )

logger = logging.getLogger(__name__)

def log_progress(step, total_steps, message, start_time=None):
    """Log progress with percentage and elapsed time"""
    if not is_github_actions:
        progress = (step / total_steps) * 100
        elapsed = time.time() - start_time if start_time else 0
        logger.info(f"[{step}/{total_steps}] {progress:.1f}% - {message} (Elapsed: {elapsed:.1f}s)")
    else:
        # Simplified progress for GitHub Actions
        logger.info(f"[OK] {message} [{step}/{total_steps}]")

def is_last_day_of_month():
    """Check if today is the last day of the month"""
    from datetime import date, timedelta
    today = date.today()
    next_month = today.replace(day=28) + timedelta(days=4)  # This will always get us to the next month
    last_day_of_month = next_month - timedelta(days=next_month.day)
    return today.day == last_day_of_month.day

def main():
    """
    Main entry point for FMCG Data Simulator - Dimensional Model
    """
    # Ensure datetime imports are available in local scope
    from datetime import datetime, timedelta, date
    
    start_time = time.time()
    
    if not is_github_actions:
        logger.info(f"{'='*60}")
        logger.info(f"FMCG Data Simulator - Dimensional Model")
        logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}\n")
    
    try:
        # Initialize BigQuery client
        logger.info("Connecting to Google Cloud...")
        client = get_bigquery_client(PROJECT_ID)
        logger.info(f"Connected to: {PROJECT_ID}")
        
        # Check if this is a scheduled run
        is_scheduled = os.environ.get("SCHEDULED_RUN", "false").lower() == "true"
        force_refresh = os.environ.get("FORCE_REFRESH", "false").lower() == "true"
        is_last_day = is_last_day_of_month()
        
        if is_scheduled:
            if is_last_day:
                logger.info("Monthly update - Full refresh")
            else:
                logger.info("Daily run - Sales data only")
        else:
            logger.info("Manual run - Full refresh")
            if force_refresh:
                logger.info("FORCE_REFRESH: Will regenerate all data regardless of existing tables")
        
        if is_scheduled and not table_has_data(client, FACT_SALES):
            logger.warning("SCHEDULED RUN SKIPPED: No initial data found.")
            logger.warning("Please run manually first to generate the initial dataset.")
            logger.info("="*60 + "\n")
            sys.exit(0)
        
        # ==================== DIMENSION TABLES ====================
        # Only generate dimensions on manual runs or last day of month
        should_update_dimensions = not is_scheduled or is_last_day or force_refresh
        
        if should_update_dimensions:
            logger.info("Building dimensions...")
            dim_start = time.time()
        else:
            logger.info("Skipping dimensions (daily run)")
        
        if should_update_dimensions:
            # Generate core dimensions first (dependencies)
            if not table_has_data(client, DIM_LOCATIONS) or force_refresh:
                logger.info("Creating locations...")
                if force_refresh:
                    logger.info("FORCE_REFRESH: Regenerating locations table")
                locations = generate_dim_locations(num_locations=500)
                append_df_bq(client, pd.DataFrame(locations), DIM_LOCATIONS)
            else:
                logger.info("Locations ready")
                # Load existing locations for dependency
                locations_df = client.query(f"SELECT * FROM `{DIM_LOCATIONS}`").to_dataframe()
                locations = locations_df.to_dict("records")
            
            if not table_has_data(client, DIM_DEPARTMENTS) or force_refresh:
                logger.info("Generating departments dimension...")
                departments = generate_dim_departments()
                append_df_bq(client, pd.DataFrame(departments), DIM_DEPARTMENTS)
            else:
                logger.info("Departments dimension already exists. Skipping.")
                departments_df = client.query(f"SELECT * FROM `{DIM_DEPARTMENTS}`").to_dataframe()
                departments = departments_df.to_dict("records")
            
            if not table_has_data(client, DIM_JOBS) or force_refresh:
                logger.info("Generating jobs dimension...")
                if force_refresh:
                    logger.info("FORCE_REFRESH: Regenerating jobs table")
                jobs = generate_dim_jobs(departments)
                append_df_bq(client, pd.DataFrame(jobs), DIM_JOBS)
            else:
                logger.info("Jobs dimension already exists. Skipping.")
                jobs_df = client.query(f"SELECT * FROM `{DIM_JOBS}`").to_dataframe()
                jobs = jobs_df.to_dict("records")
            
            if not table_has_data(client, DIM_BANKS) or force_refresh:
                logger.info("Generating banks dimension...")
                if force_refresh:
                    logger.info("FORCE_REFRESH: Regenerating banks table")
                banks = generate_dim_banks()
                append_df_bq(client, pd.DataFrame(banks), DIM_BANKS)
            else:
                logger.info("Banks dimension already exists. Skipping.")
                banks_df = client.query(f"SELECT * FROM `{DIM_BANKS}`").to_dataframe()
                banks = banks_df.to_dict("records")
            
            if not table_has_data(client, DIM_INSURANCE) or force_refresh:
                logger.info("Generating insurance dimension...")
                if force_refresh:
                    logger.info("FORCE_REFRESH: Regenerating insurance table")
                insurance = generate_dim_insurance()
                append_df_bq(client, pd.DataFrame(insurance), DIM_INSURANCE)
            else:
                logger.info("Insurance dimension already exists. Skipping.")
                insurance_df = client.query(f"SELECT * FROM `{DIM_INSURANCE}`").to_dataframe()
                insurance = insurance_df.to_dict("records")
            
            # Generate normalized reference dimensions first
            if not table_has_data(client, DIM_CATEGORIES) or force_refresh:
                logger.info("Generating categories dimension...")
                categories = generate_dim_categories()
                append_df_bq(client, pd.DataFrame(categories), DIM_CATEGORIES)
            else:
                logger.info("Categories dimension already exists. Skipping.")
                categories_df = client.query(f"SELECT * FROM `{DIM_CATEGORIES}`").to_dataframe()
                categories = categories_df.to_dict("records")
            
            if not table_has_data(client, DIM_BRANDS) or force_refresh:
                logger.info("Generating brands dimension...")
                brands = generate_dim_brands()
                append_df_bq(client, pd.DataFrame(brands), DIM_BRANDS)
            else:
                logger.info("Brands dimension already exists. Skipping.")
                brands_df = client.query(f"SELECT brand_id, brand_name, brand_code FROM `{DIM_BRANDS}`").to_dataframe()
                brands = brands_df.to_dict("records")
            
            if not table_has_data(client, DIM_SUBCATEGORIES) or force_refresh:
                logger.info("Generating subcategories dimension...")
                subcategories = generate_dim_subcategories()
                append_df_bq(client, pd.DataFrame(subcategories), DIM_SUBCATEGORIES)
            else:
                logger.info("Subcategories dimension already exists. Skipping.")
                subcategories_df = client.query(f"SELECT * FROM `{DIM_SUBCATEGORIES}`").to_dataframe()
                subcategories = subcategories_df.to_dict("records")
            
            # Generate dependent dimensions
            if not table_has_data(client, DIM_PRODUCTS) or force_refresh:
                logger.info("Generating products dimension with foreign keys...")
                products = generate_dim_products(
                    categories=categories,
                    brands=brands,
                    subcategories=subcategories,
                    num_products=25
                )
                append_df_bq(client, pd.DataFrame(products), DIM_PRODUCTS)
            else:
                logger.info("Products dimension already exists. Skipping.")
            
            if not table_has_data(client, DIM_EMPLOYEES) or force_refresh:
                logger.info("Generating normalized employees dimension...")
                employees = generate_dim_employees_normalized(
                    locations=locations,
                    departments=departments,
                    jobs=jobs,
                    banks=banks,
                    insurance=insurance,
                    num_employees=350
                )
                # Convert None values to appropriate types for BigQuery compatibility
                employees_df = pd.DataFrame(employees)
                
                # Handle all date fields - convert to datetime with proper null handling
                date_columns = ['hire_date', 'birth_date']
                for col in date_columns:
                    if col in employees_df.columns:
                        # Convert to datetime, coercing errors to NaT
                        employees_df[col] = pd.to_datetime(employees_df[col], errors='coerce')
                        logger.info(f"Converted {col}: {employees_df[col].dtype}, null count: {employees_df[col].isna().sum()}")
                
                # Handle termination_date separately - convert to string to avoid PyArrow issues
                if 'termination_date' in employees_df.columns:
                    # Convert to datetime first
                    employees_df['termination_date'] = pd.to_datetime(employees_df['termination_date'], errors='coerce')
                    # Convert to string format YYYY-MM-DD for BigQuery compatibility
                    employees_df['termination_date'] = employees_df['termination_date'].dt.strftime('%Y-%m-%d')
                    # Replace NaT (which becomes 'NaT') with None for proper null handling
                    employees_df['termination_date'] = employees_df['termination_date'].replace('NaT', None)
                    logger.info(f"termination_date converted to string, null count: {employees_df['termination_date'].isna().sum()}")
                
                # Add tenure column (years since hire date)
                if 'hire_date' in employees_df.columns:
                    today = pd.Timestamp.now().normalize()
                    employees_df['tenure_years'] = ((today - employees_df['hire_date']).dt.days / 365.25).round(2)
                    logger.info(f"Added tenure_years, range: {employees_df['tenure_years'].min():.2f} to {employees_df['tenure_years'].max():.2f} years")
                
                # Handle job_id - ensure it's never empty, use first job as default
                if 'job_id' in employees_df.columns:
                    # Find first valid job_id as default
                    default_job_id = jobs[0]['job_id'] if jobs else ''
                    employees_df['job_id'] = employees_df['job_id'].fillna(default_job_id)
                    # Replace any empty strings with default
                    employees_df['job_id'] = employees_df['job_id'].replace('', default_job_id)
                
                # Handle other potential None values in string columns
                string_columns = ['gender', 'phone', 'email', 'personal_email', 'employment_status', 
                                'tin_number', 'sss_number', 'philhealth_number', 'pagibig_number', 'blood_type', 
                                'emergency_contact_name', 'emergency_contact_relation', 'emergency_contact_phone']
                
                for col in string_columns:
                    if col in employees_df.columns:
                        employees_df[col] = employees_df[col].fillna('')
                
                # Ensure all foreign key IDs are never empty
                fk_columns = {
                    'location_id': locations[0]['location_id'] if locations else '',
                    'bank_id': banks[0]['bank_id'] if banks else '',
                    'insurance_id': insurance[0]['insurance_id'] if insurance else ''
                }
                
                for col, default_value in fk_columns.items():
                    if col in employees_df.columns:
                        employees_df[col] = employees_df[col].fillna(default_value)
                        employees_df[col] = employees_df[col].replace('', default_value)
                
                append_df_bq(client, employees_df, DIM_EMPLOYEES)
            else:
                logger.info("Employees dimension already exists. Skipping.")
            
            if not table_has_data(client, DIM_RETAILERS) or force_refresh:
                logger.info("Creating retailers...")
                retailers = generate_dim_retailers_normalized(
                    num_retailers=INITIAL_RETAILERS, 
                    locations=locations
                )
                logger.info(f"Generated {len(retailers)} retailers, starting BigQuery upload...")
                append_df_bq(client, pd.DataFrame(retailers), DIM_RETAILERS)
                logger.info("Retailers completed successfully")
            else:
                logger.info("Retailers ready")
                # Load existing retailers for dependency
                retailers_df = client.query(f"SELECT * FROM `{DIM_RETAILERS}`").to_dataframe()
                retailers = retailers_df.to_dict("records")
            
            if not table_has_data(client, DIM_CAMPAIGNS) or force_refresh:
                logger.info("Creating campaigns...")
                campaigns = generate_dim_campaigns()
                append_df_bq(client, pd.DataFrame(campaigns), DIM_CAMPAIGNS)
            else:
                logger.info("Campaigns ready")
                # Load existing campaigns for dependency
                campaigns_df = client.query(f"SELECT * FROM `{DIM_CAMPAIGNS}`").to_dataframe()
                campaigns = campaigns_df.to_dict("records")
            
            if not table_has_data(client, DIM_DATES) or force_refresh:
                logger.info("Creating dates...")
                dates = generate_dim_dates()
                # Convert date_id to string to match schema and avoid PyArrow issues
                dates_df = pd.DataFrame(dates)
                if 'date_id' in dates_df.columns:
                    dates_df['date_id'] = dates_df['date_id'].astype(str)
                    logger.info(f"Converted date_id to string, sample: {dates_df['date_id'].iloc[0]}")
                append_df_bq(client, dates_df, DIM_DATES)
            else:
                logger.info("Dates ready")
            
            logger.info("All dimensions completed successfully!")
            
            # Generate employee facts if employees exist
            if table_has_data(client, DIM_EMPLOYEES):
                logger.info("Processing employee data...")
                # Load all employees (active and terminated) for historical wage data
                employees_df = client.query(f"SELECT * FROM `{DIM_EMPLOYEES}`").to_dataframe()
                employees_all = employees_df.to_dict("records")
                
                # Convert termination_date back to datetime for wage generation
                for emp in employees_all:
                    if emp.get('termination_date') and emp['termination_date'] != '':
                        try:
                            emp['termination_date'] = pd.to_datetime(emp['termination_date']).date()
                        except:
                            emp['termination_date'] = None
                    else:
                        emp['termination_date'] = None
                
                # Load active employees for fact table
                employees_active_df = client.query(f"SELECT * FROM `{DIM_EMPLOYEES}` WHERE employment_status = 'Active'").to_dataframe()
                employees_active = employees_active_df.to_dict("records")
                
                # Load jobs for salary ranges
                jobs_df = client.query(f"SELECT * FROM `{DIM_JOBS}`").to_dataframe()
                jobs_data = jobs_df.to_dict("records")
                
                # Load departments for wage generation
                departments_df = client.query(f"SELECT * FROM `{DIM_DEPARTMENTS}`").to_dataframe()
                departments_data = departments_df.to_dict("records")
                
                # Check and regenerate employee facts if needed
                if not table_has_data(client, FACT_EMPLOYEES) or force_refresh:
                    if force_refresh:
                        logger.info("FORCE_REFRESH: Regenerating employee facts...")
                    else:
                        logger.info("Generating employee facts...")
                    employee_facts = generate_fact_employees(employees_active, jobs_data)
                    append_df_bq(client, pd.DataFrame(employee_facts), FACT_EMPLOYEES)
                else:
                    logger.info("Employee facts already exist. Skipping.")
                
                # Always regenerate employee wages with historical data
                wages_table = f"{PROJECT_ID}.{DATASET}.fact_employee_wages"
                logger.info("Regenerating employee wage history with historical data from 2015 to present...")
                try:
                    # Drop existing wages table to regenerate with historical data
                    client.delete_table(wages_table)
                    logger.info("Dropped existing wages table to regenerate with historical data")
                except Exception as e:
                    logger.info(f"Wages table doesn't exist or couldn't drop: {e}")
                
                # Generate historical wage data for all employees (active and terminated)
                employee_wages = generate_fact_employee_wages(employees_all, jobs_data, departments_data, start_date=date(2015, 1, 1), end_date=date.today())
                append_df_bq(client, pd.DataFrame(employee_wages), wages_table)
                logger.info(f"Generated {len(employee_wages)} historical wage records")
            else:
                logger.info("No employees found. Skipping employee data generation.")
        else:
            logger.info("Skipping dimension and employee data generation (daily run - sales only)")
        
        # Load existing dimensions for fact table generation (optimized queries)
        try:
            # Load all dimension data for relationship validation
            locations_df = client.query(f"SELECT * FROM `{DIM_LOCATIONS}`").to_dataframe()
            departments_df = client.query(f"SELECT * FROM `{DIM_DEPARTMENTS}`").to_dataframe()
            jobs_df = client.query(f"SELECT * FROM `{DIM_JOBS}`").to_dataframe()
            banks_df = client.query(f"SELECT * FROM `{DIM_BANKS}`").to_dataframe()
            insurance_df = client.query(f"SELECT * FROM `{DIM_INSURANCE}`").to_dataframe()
            categories_df = client.query(f"SELECT * FROM `{DIM_CATEGORIES}`").to_dataframe()
            brands_df = client.query(f"SELECT * FROM `{DIM_BRANDS}`").to_dataframe()
            subcategories_df = client.query(f"SELECT * FROM `{DIM_SUBCATEGORIES}`").to_dataframe()
            
            # Convert to dictionaries for validation
            locations = locations_df.to_dict("records")
            departments = departments_df.to_dict("records")
            jobs = jobs_df.to_dict("records")
            banks = banks_df.to_dict("records")
            insurance = insurance_df.to_dict("records")
            categories = categories_df.to_dict("records")
            brands = brands_df.to_dict("records")
            subcategories = subcategories_df.to_dict("records")
            
            # Use more efficient queries with specific fields only
            products_df = client.query(f"SELECT product_id, product_name, category_id, brand_id, subcategory_id, wholesale_price, retail_price, status, created_date FROM `{DIM_PRODUCTS}` WHERE status = 'Active'").to_dataframe()
            
            # Load normalized employee data with joins
            employees_df = client.query(f"""
                SELECT 
                    e.employee_id, e.first_name, e.last_name, e.employment_status, 
                    e.hire_date, e.termination_date, e.gender, e.birth_date,
                    e.phone, e.email, e.personal_email,
                    e.tin_number, e.sss_number, e.philhealth_number, e.pagibig_number, e.blood_type,
                    e.emergency_contact_name, e.emergency_contact_relation, e.emergency_contact_phone,
                    j.job_title, j.work_setup, j.work_type,
                    d.department_name,
                    l.city, l.province, l.region, l.country,
                    b.bank_name,
                    i.provider_name as health_insurance_provider,
                    ef.performance_rating, ef.last_review_date,
                    ef.training_hours_completed, ef.certifications_count, ef.benefit_enrollment_date,
                    ef.years_of_service, ef.attendance_rate, ef.overtime_hours_monthly,
                    ef.engagement_score, ef.satisfaction_index,
                    ef.vacation_leave_balance, ef.sick_leave_balance, ef.personal_leave_balance,
                    ef.productivity_score, ef.retention_risk_score, ef.skill_gap_score,
                    ef.health_utilization_rate
                FROM `{DIM_EMPLOYEES}` e
                LEFT JOIN `{DIM_JOBS}` j ON e.job_id = j.job_id
                LEFT JOIN `{DIM_DEPARTMENTS}` d ON j.department_id = d.department_id
                LEFT JOIN `{DIM_LOCATIONS}` l ON e.location_id = l.location_id
                LEFT JOIN `{DIM_BANKS}` b ON e.bank_id = b.bank_id
                LEFT JOIN `{DIM_INSURANCE}` i ON e.insurance_id = i.insurance_id
                LEFT JOIN `{FACT_EMPLOYEES}` ef ON e.employee_id = ef.employee_id
                WHERE e.employment_status = 'Active'
            """).to_dataframe()
            
            # Load normalized retailer data
            retailers_df = client.query(f"""
                SELECT 
                    r.retailer_id, r.retailer_name, r.retailer_type,
                    l.city, l.province, l.region, l.country
                FROM `{DIM_RETAILERS}` r
                LEFT JOIN `{DIM_LOCATIONS}` l ON r.location_id = l.location_id
            """).to_dataframe()
            
            campaigns_df = client.query(f"SELECT campaign_id, campaign_name, campaign_type, start_date, end_date, budget, currency FROM `{DIM_CAMPAIGNS}`").to_dataframe()
            
            # Convert to dictionaries for validation and fact generation
            employees = employees_df.to_dict("records")
            products = products_df.to_dict("records")
            retailers = retailers_df.to_dict("records")
            campaigns = campaigns_df.to_dict("records")
            
            # Validate all relationships before fact table generation
            logger.info("Validating table relationships...")
            if not validate_relationships(employees, products, retailers, campaigns, locations, departments, jobs, banks, insurance, categories, brands, subcategories):
                logger.error("Relationship validation failed! Skipping fact table generation.")
                return
            else:
                logger.info("All relationships validated successfully!")
        except Exception as e:
            if "readsessions.create" in str(e):
                logger.warning(f"BigQuery read sessions permission error. Using alternative approach...")
                # Use smaller queries without read sessions
                products_df = client.query(f"SELECT product_id, product_name, category_id, brand_id, subcategory_id, wholesale_price, retail_price, status, created_date FROM `{DIM_PRODUCTS}`").to_dataframe()
                
                # Load dimensions for fallback
                locations_df = client.query(f"SELECT * FROM `{DIM_LOCATIONS}`").to_dataframe()
                departments_df = client.query(f"SELECT * FROM `{DIM_DEPARTMENTS}`").to_dataframe()
                jobs_df = client.query(f"SELECT * FROM `{DIM_JOBS}`").to_dataframe()
                banks_df = client.query(f"SELECT * FROM `{DIM_BANKS}`").to_dataframe()
                insurance_df = client.query(f"SELECT * FROM `{DIM_INSURANCE}`").to_dataframe()
                categories_df = client.query(f"SELECT * FROM `{DIM_CATEGORIES}`").to_dataframe()
                brands_df = client.query(f"SELECT * FROM `{DIM_BRANDS}`").to_dataframe()
                subcategories_df = client.query(f"SELECT * FROM `{DIM_SUBCATEGORIES}`").to_dataframe()
                
                # Convert to dictionaries
                locations = locations_df.to_dict("records")
                departments = departments_df.to_dict("records")
                jobs = jobs_df.to_dict("records")
                banks = banks_df.to_dict("records")
                insurance = insurance_df.to_dict("records")
                categories = categories_df.to_dict("records")
                brands = brands_df.to_dict("records")
                subcategories = subcategories_df.to_dict("records")
                
                # Simplified employee query for fallback
                employees_df = client.query(f"""
                    SELECT 
                        e.employee_id, e.first_name, e.last_name, e.employment_status, 
                        e.hire_date, e.termination_date, e.gender, e.birth_date,
                        e.phone, e.email, e.personal_email,
                        e.tin_number, e.sss_number, e.philhealth_number, e.pagibig_number, e.blood_type,
                        e.emergency_contact_name, e.emergency_contact_relation, e.emergency_contact_phone,
                        j.job_title, j.work_setup, j.work_type,
                        d.department_name,
                        l.city, l.province, l.region, l.country,
                        b.bank_name,
                        i.provider_name as health_insurance_provider
                    FROM `{DIM_EMPLOYEES}` e
                    LEFT JOIN `{DIM_JOBS}` j ON e.job_id = j.job_id
                    LEFT JOIN `{DIM_DEPARTMENTS}` d ON j.department_id = d.department_id
                    LEFT JOIN `{DIM_LOCATIONS}` l ON e.location_id = l.location_id
                    LEFT JOIN `{DIM_BANKS}` b ON e.bank_id = b.bank_id
                    LEFT JOIN `{DIM_INSURANCE}` i ON e.insurance_id = i.insurance_id
                """).to_dataframe()
                
                retailers_df = client.query(f"""
                    SELECT 
                        r.retailer_id, r.retailer_name, r.retailer_type,
                        l.city, l.province, l.region, l.country
                    FROM `{DIM_RETAILERS}` r
                    LEFT JOIN `{DIM_LOCATIONS}` l ON r.location_id = l.location_id
                """).to_dataframe()
                
                campaigns_df = client.query(f"SELECT campaign_id, campaign_name, campaign_type, start_date, end_date, budget, currency FROM `{DIM_CAMPAIGNS}`").to_dataframe()
            else:
                raise
        
        products = products_df.to_dict("records")
        employees = employees_df.to_dict("records")
        retailers = retailers_df.to_dict("records")
        campaigns = campaigns_df.to_dict("records")
        
        logger.info(f"\nLoaded dimensions:")
        logger.info(f"   Products: {len(products):,}")
        logger.info(f"   Employees: {len(employees):,}")
        logger.info(f"   Retailers: {len(retailers):,}")
        logger.info(f"   Campaigns: {len(campaigns):,}")
        
        # ==================== FACT TABLES ====================
        logger.info("Generating fact tables...")
        
        # Generate fact tables - FORCE SALES GENERATION
        logger.info("FORCE: About to generate sales data regardless of table existence...")
        
        # Always try to generate sales data (bypass table existence check for debugging)
        yesterday = date.today() - timedelta(days=1)
        
        # Use different sales targets for scheduled vs manual runs
        if is_scheduled:
            # Daily run: generate only yesterday's sales (~₱2M)
            sales_target = DAILY_SALES_AMOUNT
            start_date = yesterday
            end_date = yesterday
            logger.info(f"Daily run: Generating ₱{sales_target:,.0f} in sales for {yesterday}...")
        else:
            # Manual run: generate full historical data (₱8B total)
            sales_target = INITIAL_SALES_AMOUNT
            start_date = date(2015, 1, 1)
            end_date = yesterday
            logger.info(f"Manual run: Generating ₱{sales_target:,.0f} in total sales from {start_date} to {yesterday}...")
        
        logger.info(f"INITIAL_SALES_AMOUNT from config: {INITIAL_SALES_AMOUNT:,}")
        logger.info(f"DAILY_SALES_AMOUNT from config: {DAILY_SALES_AMOUNT:,}")
        
        # Add progress monitoring for large data generation
        sales_start = time.time()
        logger.info("Starting sales data generation...")
        
        sales = []
        try:
            logger.info("About to call generate_fact_sales...")
            sales = generate_fact_sales(
                employees, products, retailers, campaigns,
                sales_target,
                start_date=start_date,
                end_date=end_date
            )
            
            sales_elapsed = time.time() - sales_start
            logger.info(f"Sales generation completed in {sales_elapsed:.1f} seconds")
            logger.info(f"Generated {len(sales):,} sales records")
            
            # Debug: check if sales list is empty
            if not sales:
                logger.warning("Sales generation returned empty list!")
            else:
                logger.info(f"Sales generation successful: {len(sales)} records generated")
                # Show first few records details
                for i, sale in enumerate(sales[:3]):
                    logger.info(f"  Sample {i+1}: Date={sale['sale_date']}, Amount=₱{sale['total_amount']:,.0f}")
                
        except Exception as e:
            logger.error(f"Error during sales generation: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            sales = []
        
        # FORCE: If no sales were generated, create minimal sample data
        if not sales:
            logger.warning("FORCING creation of sample sales data...")
            try:
                # Create minimal sample sales data
                if is_scheduled:
                    # Daily run: use yesterday's date
                    sample_date = yesterday
                    target_amount = DAILY_SALES_AMOUNT
                else:
                    # Manual run: use mid-year date for historical data
                    sample_date = date(2015, 6, 1)
                    target_amount = 1000.0  # Small sample amount for manual runs
                
                # Get first available employee, product, retailer
                available_employees = [e for e in employees if e.get('hire_date') and e.get('hire_date') <= sample_date]
                available_products = [p for p in products if p.get('created_date') and p.get('created_date') <= sample_date]
                
                if available_employees and available_products and retailers:
                    # Calculate amount per sale to reach target
                    num_sales = min(10, len(available_employees), len(available_products), len(retailers))
                    amount_per_sale = target_amount / num_sales
                    
                    for i in range(num_sales):
                        sample_sale = {
                            "sale_id": 1000000 + i,  # Simple sequential keys
                            "sale_date": sample_date,
                            "product_id": available_products[i % len(available_products)]["product_id"],
                            "retailer_id": retailers[i % len(retailers)]["retailer_id"],
                            "case_quantity": 10,
                            "unit_price": amount_per_sale / 10,  # Base price
                            "discount_percent": 0.05,
                            "tax_rate": 0.12,
                            "total_amount": amount_per_sale,
                            "commission_amount": amount_per_sale * 0.05,  # 5% commission
                            "currency": "PHP",
                            "payment_method": "Cash",
                            "payment_status": "Paid",
                            "delivery_status": "Delivered",
                            "expected_delivery_date": sample_date + timedelta(days=1),
                            "actual_delivery_date": sample_date + timedelta(days=1)
                        }
                        sales.append(sample_sale)
                    
                    logger.info(f"Created {len(sales)} sample sales records")
                else:
                    logger.error("Cannot create sample sales - insufficient dimension data")
                    logger.info(f"Available employees: {len(available_employees)}")
                    logger.info(f"Available products: {len(available_products)}")
                    logger.info(f"Available retailers: {len(retailers)}")
                    
            except Exception as e:
                logger.error(f"Error creating sample sales: {e}")
                import traceback
                logger.error(f"Sample creation traceback: {traceback.format_exc()}")
        
        # Always try to append the sales data (even if empty, this will create the table)
        logger.info("About to append sales data to BigQuery...")
        try:
            logger.info(f"Creating DataFrame with {len(sales)} sales records...")
            sales_df = pd.DataFrame(sales)
            logger.info(f"DataFrame shape: {sales_df.shape}")
            
            if not sales_df.empty:
                logger.info(f"DataFrame columns: {list(sales_df.columns)}")
                logger.info(f"Sample data: {sales_df.head(1).to_dict()}")
            else:
                logger.warning("Empty DataFrame - will create table structure only")
            
            # Check if table exists first, if not create it
            if not table_has_data(client, FACT_SALES):
                logger.info(f"Table {FACT_SALES} does not exist. Creating table structure...")
                # Create empty DataFrame with correct schema to establish table
                if sales_df.empty:
                    # Create a sample record to establish schema
                    sample_schema_df = pd.DataFrame([{
                        "sale_id": "SAL000001",
                        "sale_date": date.today(),
                        "product_id": "PROD000001",
                        "retailer_id": "RET000001",
                        "case_quantity": 1,
                        "unit_price": 100.0,
                        "discount_percent": 0.0,
                        "tax_rate": 0.12,
                        "total_amount": 112.0,
                        "commission_amount": 3.36,
                        "currency": "PHP",
                        "payment_method": "Cash",
                        "payment_status": "Paid",
                        "delivery_status": "Pending",
                        "expected_delivery_date": date.today() + timedelta(days=1),
                        "actual_delivery_date": None
                    }])
                    append_df_bq_safe(client, sample_schema_df, FACT_SALES, "sale_id")
                    logger.info("Created table schema with sample record")
            
            # Use appropriate loading method based on force_refresh
            if force_refresh:
                # Force refresh: truncate table and load fresh data (no duplicate checking)
                logger.info("FORCE_REFRESH: Truncating sales table and loading fresh data...")
                try:
                    # Delete existing sales data
                    client.delete_table(FACT_SALES)
                    logger.info("Deleted existing sales table for force refresh")
                except Exception:
                    logger.info("Sales table doesn't exist or couldn't delete (continuing)")
                
                # Load fresh data without duplicate checking
                append_df_bq(client, sales_df, FACT_SALES, write_disposition="WRITE_TRUNCATE")
                logger.info("Sales data force refresh completed")
            else:
                # Normal run: check for duplicates and append
                append_df_bq_safe(client, sales_df, FACT_SALES, "sale_id")
                logger.info("Sales data append completed")
        except Exception as e:
            logger.error(f"Error appending sales data: {e}")
            import traceback
            logger.error(f"Append traceback: {traceback.format_exc()}")
        
        # Log sales generation summary
        logger.info(f"Sales generation completed:")
        logger.info(f"  Total sales records: {len(sales):,}")
        logger.info(f"  Total sales amount: ₱{sum(s['total_amount'] for s in sales):,.2f}")
        
        # Update delivery status for all runs (daily and scheduled)
        logger.info("Checking delivery status...")
        # For free tier, we can only monitor, not update directly
        # The actual status updates will be handled using BigQuery free tier methods
        update_delivery_status(client, FACT_SALES)
        
        # For scheduled runs, update delivery statuses using official BigQuery methods
        if is_scheduled:
            logger.info("Updating delivery statuses using BigQuery free tier methods...")
            
            try:
                # Method 1: Direct table overwrite (simple, no audit trail)
                logger.info("Method 1: Direct table overwrite...")
                method1_success = execute_method_1_overwrite(client, PROJECT_ID, DATASET, FACT_SALES)
                
                if method1_success:
                    logger.info("Delivery statuses updated successfully")
                    
                    # Get updated status summary
                    summary_df = get_delivery_status_summary(client, PROJECT_ID, DATASET)
                    if not summary_df.empty:
                        logger.info("Current Delivery Status Summary:")
                        for _, row in summary_df.iterrows():
                            logger.info(f"   {row['current_delivery_status']}: {row['order_count']:,} orders (PHP {row['total_value']:,.2f})")
                else:
                    logger.warning("Method 1 failed, trying Method 2...")
                    
                    # Method 2: Append update records (with audit trail)
                    logger.info("Method 2: Append update records...")
                    method2_success = execute_method_2_append(client, PROJECT_ID, DATASET, 'delivery_status_updates')
                    
                    if method2_success:
                        # Create current status view
                        view_query = create_current_delivery_status_view(PROJECT_ID, DATASET, FACT_SALES, 'delivery_status_updates')
                        client.query(view_query).result()
                        logger.info("Delivery status updates appended and view created")
                    else:
                        logger.warning("All update methods failed, continuing with run...")
                        
            except Exception as e:
                logger.warning(f"Could not update delivery statuses: {e}")
                logger.info("Continuing with scheduled run...")
                logger.info("Tip: You can manually update statuses using BigQuery Console with WRITE_TRUNCATE")
        
        # Generate other fact tables (only on manual runs or last day of month)
        should_update_facts = not is_scheduled or is_last_day
        
        if should_update_facts:
            logger.info("Generating additional fact tables...")
        else:
            logger.info("Skipping additional fact tables (daily run - sales only)")
        
        if should_update_facts:
            if not table_has_data(client, FACT_OPERATING_COSTS):
                logger.info("\nGenerating operating costs fact...")
                # Generate operating costs from 2015 to present with reduced values (15% of revenue)
                costs = generate_fact_operating_costs(
                    INITIAL_SALES_AMOUNT * 0.15,  # Reduced from 25% to 15% of revenue
                    start_date=date(2015, 1, 1),
                    end_date=date.today()
                )
                append_df_bq(client, pd.DataFrame(costs), FACT_OPERATING_COSTS)
            else:
                logger.info("Dropping existing operating costs table to regenerate with correct ratios...")
                try:
                    client.delete_table(FACT_OPERATING_COSTS)
                    logger.info("Operating costs table dropped successfully")
                    logger.info("\nGenerating operating costs fact...")
                    costs = generate_fact_operating_costs(
                        INITIAL_SALES_AMOUNT * 0.15,  # Reduced from 25% to 15% of revenue
                        start_date=date(2015, 1, 1),
                        end_date=date.today()
                    )
                    append_df_bq(client, pd.DataFrame(costs), FACT_OPERATING_COSTS)
                    logger.info("Operating costs regenerated with correct ratios")
                except Exception as e:
                    logger.warning(f"Could not regenerate operating costs table: {e}")
                    logger.info("Operating costs table already exists. Skipping.")
            
            # Marketing costs generation
            logger.info(f"\nChecking marketing costs table: {FACT_MARKETING_COSTS}")
            marketing_table_exists = table_has_data(client, FACT_MARKETING_COSTS)
            logger.info(f"Marketing costs table exists: {marketing_table_exists}")
            
            # Drop existing marketing costs table to force regeneration with new campaigns
            if marketing_table_exists:
                logger.info("Dropping existing marketing costs table to regenerate with new campaigns...")
                try:
                    client.delete_table(FACT_MARKETING_COSTS)
                    logger.info("Marketing costs table dropped successfully")
                    marketing_table_exists = False
                except Exception as e:
                    logger.warning(f"Could not drop marketing costs table: {e}")
            
            # Force regenerate marketing costs if it's missing or empty
            if not marketing_table_exists:
                logger.info("Generating marketing costs fact...")
                try:
                    # Always use historical range for marketing costs to match campaigns
                    # Marketing costs should cover the full campaign period regardless of sales table status
                    start_date = date(2015, 1, 1)
                    end_date = date.today() - timedelta(days=1)
                    
                    logger.info(f"Marketing costs date range: {start_date} to {end_date}")
                    logger.info(f"Number of campaigns available: {len(campaigns)}")
                    
                    # Debug: Show campaign date ranges
                    for i, campaign in enumerate(campaigns[:3]):  # Show first 3 campaigns
                        logger.info(f"Campaign {i+1}: {campaign['campaign_name']} ({campaign['start_date']} to {campaign['end_date']})")
                    
                    marketing_costs = generate_fact_marketing_costs(
                        campaigns, 
                        INITIAL_SALES_AMOUNT * 0.08,  # 8% of revenue (realistic marketing spend)
                        start_date=start_date,
                        end_date=end_date
                    )
                    logger.info(f"Generated {len(marketing_costs):,} marketing cost records")
                    if len(marketing_costs) > 0:
                        append_df_bq(client, pd.DataFrame(marketing_costs), FACT_MARKETING_COSTS)
                        logger.info("Marketing costs loaded successfully")
                    else:
                        logger.warning("No marketing costs generated - skipping table creation")
                        logger.warning("This might be due to date range mismatch with campaigns")
                except Exception as e:
                    logger.error(f"Error generating marketing costs: {str(e)}")
                    raise
            else:
                logger.info("Marketing costs table already exists. Skipping.")
            
            if not table_has_data(client, FACT_INVENTORY) or force_refresh:
                if force_refresh:
                    logger.info("\nFORCE_REFRESH: Regenerating inventory fact...")
                else:
                    logger.info("\nGenerating inventory fact...")
                inventory = generate_fact_inventory(products)
                append_df_bq(client, pd.DataFrame(inventory), FACT_INVENTORY)
            else:
                logger.info("Inventory table already exists. Skipping.")
        
        # ==================== SUMMARY ====================
        logger.info("Load complete!")
        
        # Final timing and memory summary
        total_elapsed = time.time() - start_time
        logger.info(f"\n{'='*60}")
        logger.info(f"EXECUTION SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total execution time: {total_elapsed:.1f} seconds ({total_elapsed/60:.1f} minutes)")
        
        # Log table sizes
        try:
            logger.info("\nTable sizes:")
            tables = [
                ("Locations", DIM_LOCATIONS),
                ("Departments", DIM_DEPARTMENTS),
                ("Jobs", DIM_JOBS),
                ("Banks", DIM_BANKS),
                ("Insurance", DIM_INSURANCE),
                ("Products", DIM_PRODUCTS),
                ("Retailers", DIM_RETAILERS),
                ("Campaigns", DIM_CAMPAIGNS),
                ("Employees", DIM_EMPLOYEES),
                ("Sales", FACT_SALES),
                ("Employee Facts", FACT_EMPLOYEES),
                ("Operating Costs", FACT_OPERATING_COSTS),
                ("Marketing Costs", FACT_MARKETING_COSTS),
                ("Inventory", FACT_INVENTORY)
            ]
            
            for name, table in tables:
                try:
                    result = client.query(f"SELECT COUNT(*) as count FROM `{table}`").to_dataframe()
                    count = result['count'].iloc[0]
                    logger.info(f"  {name}: {count:,} records")
                except Exception as e:
                    logger.warning(f"  {name}: Error getting count - {str(e)}")
                    
        except Exception as e:
            logger.warning(f"Error getting table sizes: {str(e)}")
        
        logger.info(f"{'='*60}\n")
        
    except Exception as e:
        logger.error(f"\nERROR: {str(e)}")
        logger.error(f"Failed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.error(f"Elapsed time: {time.time() - start_time:.1f} seconds")
        
        # Log system information for debugging
        try:
            import psutil
            memory = psutil.virtual_memory()
            logger.error(f"Memory usage: {memory.percent}% ({memory.used/1024/1024:.1f} MB used)")
        except ImportError:
            pass
            
        sys.exit(1)

if __name__ == "__main__":
    main()
