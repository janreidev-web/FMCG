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

from config import PROJECT_ID, DATASET, INITIAL_SALES_AMOUNT, DAILY_SALES_AMOUNT

# Force correct values for debugging
INITIAL_SALES_AMOUNT = 6000000000  # ₱6B
DAILY_SALES_AMOUNT = 1640000      # ₱1.64M
from schema import (
    DIM_PRODUCTS, DIM_EMPLOYEES, DIM_RETAILERS, DIM_CAMPAIGNS,
    DIM_LOCATIONS, DIM_DEPARTMENTS, DIM_JOBS, DIM_BANKS, DIM_INSURANCE,
    FACT_SALES, FACT_OPERATING_COSTS, FACT_INVENTORY, FACT_MARKETING_COSTS, FACT_EMPLOYEES
)
from auth import get_bigquery_client
from helpers import table_has_data, append_df_bq, append_df_bq_safe, update_delivery_status
from generators.dimensional import (
    generate_dim_products, generate_dim_employees_normalized, generate_dim_locations,
    generate_dim_departments, generate_dim_jobs, generate_dim_banks, generate_dim_insurance,
    generate_fact_employees, generate_dim_retailers_normalized,
    generate_dim_campaigns, generate_fact_sales,
    generate_fact_operating_costs, generate_fact_inventory, generate_fact_marketing_costs
)

# Configure enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def log_progress(step, total_steps, message, start_time=None):
    """Log progress with percentage and elapsed time"""
    progress = (step / total_steps) * 100
    elapsed = time.time() - start_time if start_time else 0
    logger.info(f"[{step}/{total_steps}] {progress:.1f}% - {message} (Elapsed: {elapsed:.1f}s)")

def main():
    """
    Main entry point for FMCG Data Simulator - Dimensional Model
    """
    start_time = time.time()
    logger.info(f"{'='*60}")
    logger.info(f"FMCG Data Simulator - Dimensional Model")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*60}\n")
    
    try:
        # Initialize BigQuery client
        logger.info("Authenticating with Google Cloud...")
        client = get_bigquery_client(PROJECT_ID)
        logger.info(f"Connected to project: {PROJECT_ID}")
        
        # Check if this is a scheduled run
        is_scheduled = os.environ.get("SCHEDULED_RUN", "false").lower() == "true"
        run_type = "SCHEDULED RUN" if is_scheduled else "MANUAL RUN"
        logger.info(f"Run type: {run_type}")
        
        if is_scheduled and not table_has_data(client, FACT_SALES):
            logger.warning("⚠ SCHEDULED RUN SKIPPED: No initial data found.")
            logger.warning("Please run manually first to generate the initial dataset.")
            logger.info("="*60 + "\n")
            sys.exit(0)
        
        # ==================== DIMENSION TABLES ====================
        logger.info("Generating normalized dimension tables...")
        dim_start = time.time()
        
        # Generate core dimensions first (dependencies)
        if not table_has_data(client, DIM_LOCATIONS):
            logger.info("Generating locations dimension...")
            locations = generate_dim_locations(num_locations=500)
            append_df_bq(client, pd.DataFrame(locations), DIM_LOCATIONS)
        else:
            logger.info("Locations dimension already exists. Skipping.")
            # Load existing locations for dependency
            locations_df = client.query(f"SELECT * FROM `{DIM_LOCATIONS}`").to_dataframe()
            locations = locations_df.to_dict("records")
        
        if not table_has_data(client, DIM_DEPARTMENTS):
            logger.info("Generating departments dimension...")
            departments = generate_dim_departments()
            append_df_bq(client, pd.DataFrame(departments), DIM_DEPARTMENTS)
        else:
            logger.info("Departments dimension already exists. Skipping.")
            departments_df = client.query(f"SELECT * FROM `{DIM_DEPARTMENTS}`").to_dataframe()
            departments = departments_df.to_dict("records")
        
        if not table_has_data(client, DIM_JOBS):
            logger.info("Generating jobs dimension...")
            jobs = generate_dim_jobs(departments)
            append_df_bq(client, pd.DataFrame(jobs), DIM_JOBS)
        else:
            logger.info("Jobs dimension already exists. Skipping.")
            jobs_df = client.query(f"SELECT * FROM `{DIM_JOBS}`").to_dataframe()
            jobs = jobs_df.to_dict("records")
        
        if not table_has_data(client, DIM_BANKS):
            logger.info("Generating banks dimension...")
            banks = generate_dim_banks()
            append_df_bq(client, pd.DataFrame(banks), DIM_BANKS)
        else:
            logger.info("Banks dimension already exists. Skipping.")
            banks_df = client.query(f"SELECT * FROM `{DIM_BANKS}`").to_dataframe()
            banks = banks_df.to_dict("records")
        
        if not table_has_data(client, DIM_INSURANCE):
            logger.info("Generating insurance dimension...")
            insurance = generate_dim_insurance()
            append_df_bq(client, pd.DataFrame(insurance), DIM_INSURANCE)
        else:
            logger.info("Insurance dimension already exists. Skipping.")
            insurance_df = client.query(f"SELECT * FROM `{DIM_INSURANCE}`").to_dataframe()
            insurance = insurance_df.to_dict("records")
        
        # Generate dependent dimensions
        if not table_has_data(client, DIM_PRODUCTS):
            logger.info("Generating products dimension...")
            products = generate_dim_products()
            append_df_bq(client, pd.DataFrame(products), DIM_PRODUCTS)
        else:
            logger.info("Products dimension already exists. Skipping.")
        
        if not table_has_data(client, DIM_EMPLOYEES):
            logger.info("Generating normalized employees dimension...")
            employees = generate_dim_employees_normalized(
                num_employees=250, 
                locations=locations, 
                jobs=jobs, 
                banks=banks, 
                insurance=insurance
            )
            append_df_bq(client, pd.DataFrame(employees), DIM_EMPLOYEES)
        else:
            logger.info("Employees dimension already exists. Skipping.")
        
        if not table_has_data(client, DIM_RETAILERS):
            logger.info("Generating normalized retailers dimension...")
            retailers = generate_dim_retailers_normalized(
                num_retailers=500, 
                locations=locations
            )
            append_df_bq(client, pd.DataFrame(retailers), DIM_RETAILERS)
        else:
            logger.info("Retailers dimension already exists. Skipping.")
        
        if not table_has_data(client, DIM_CAMPAIGNS):
            logger.info("Generating campaigns dimension...")
            campaigns = generate_dim_campaigns()
            append_df_bq(client, pd.DataFrame(campaigns), DIM_CAMPAIGNS)
        else:
            logger.info("Campaigns dimension already exists. Skipping.")
        
        # Generate employee facts if employees exist
        if table_has_data(client, DIM_EMPLOYEES) and not table_has_data(client, FACT_EMPLOYEES):
            logger.info("Generating employee facts...")
            # Load current employees
            employees_df = client.query(f"SELECT * FROM `{DIM_EMPLOYEES}` WHERE employment_status = 'Active'").to_dataframe()
            employees = employees_df.to_dict("records")
            
            # Load jobs for salary ranges
            jobs_df = client.query(f"SELECT * FROM `{DIM_JOBS}`").to_dataframe()
            jobs_data = jobs_df.to_dict("records")
            
            employee_facts = generate_fact_employees(employees, jobs_data)
            append_df_bq(client, pd.DataFrame(employee_facts), FACT_EMPLOYEES)
        else:
            logger.info("Employee facts already exist or no employees found. Skipping.")
        
        # Load existing dimensions for fact table generation (optimized queries)
        try:
            # Use more efficient queries with specific fields only
            products_df = client.query(f"SELECT product_key, product_id, product_name, category, subcategory, brand, wholesale_price, retail_price, status FROM `{DIM_PRODUCTS}` WHERE status = 'Active'").to_dataframe()
            
            # Load normalized employee data with joins
            employees_df = client.query(f"""
                SELECT 
                    e.employee_key, e.employee_id, e.full_name, e.employment_status, 
                    e.hire_date, e.termination_date, e.gender, e.birth_date, e.age,
                    e.phone, e.email, e.personal_email,
                    e.tin_number, e.sss_number, e.philhealth_number, e.pagibig_number, e.blood_type,
                    e.emergency_contact_name, e.emergency_contact_relation, e.emergency_contact_phone,
                    j.job_title, j.work_setup, j.work_type,
                    d.department_name,
                    l.city, l.province, l.region, l.country,
                    b.bank_name,
                    i.provider_name as health_insurance_provider,
                    ef.monthly_salary, ef.performance_rating, ef.last_review_date,
                    ef.training_hours_completed, ef.certifications_count, ef.benefit_enrollment_date,
                    ef.years_of_service, ef.attendance_rate, ef.overtime_hours_monthly,
                    ef.engagement_score, ef.satisfaction_index,
                    ef.vacation_leave_balance, ef.sick_leave_balance, ef.personal_leave_balance,
                    ef.annual_bonus, ef.total_compensation, ef.promotion_eligible,
                    ef.productivity_score, ef.retention_risk_score, ef.skill_gap_score,
                    ef.health_utilization_rate, ef.salary_grade, ef.cost_center_allocation
                FROM `{DIM_EMPLOYEES}` e
                LEFT JOIN `{DIM_JOBS}` j ON e.job_key = j.job_key
                LEFT JOIN `{DIM_DEPARTMENTS}` d ON j.department_key = d.department_key
                LEFT JOIN `{DIM_LOCATIONS}` l ON e.location_key = l.location_key
                LEFT JOIN `{DIM_BANKS}` b ON e.bank_key = b.bank_key
                LEFT JOIN `{DIM_INSURANCE}` i ON e.insurance_key = i.insurance_key
                LEFT JOIN `{FACT_EMPLOYEES}` ef ON e.employee_key = ef.employee_key
                WHERE e.employment_status = 'Active'
            """).to_dataframe()
            
            # Load normalized retailer data
            retailers_df = client.query(f"""
                SELECT 
                    r.retailer_key, r.retailer_id, r.retailer_name, r.retailer_type,
                    l.city, l.province, l.region, l.country
                FROM `{DIM_RETAILERS}` r
                LEFT JOIN `{DIM_LOCATIONS}` l ON r.location_key = l.location_key
            """).to_dataframe()
            
            campaigns_df = client.query(f"SELECT campaign_key, campaign_id, campaign_name, campaign_type, start_date, end_date, budget, currency FROM `{DIM_CAMPAIGNS}` WHERE end_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)").to_dataframe()
        except Exception as e:
            if "readsessions.create" in str(e):
                logger.warning(f"BigQuery read sessions permission error. Using alternative approach...")
                # Use smaller queries without read sessions
                products_df = client.query(f"SELECT product_key, product_id, product_name, category, subcategory, brand, wholesale_price, retail_price, status FROM `{DIM_PRODUCTS}`").to_dataframe()
                
                # Simplified employee query for fallback
                employees_df = client.query(f"""
                    SELECT 
                        e.employee_key, e.employee_id, e.full_name, e.employment_status, 
                        e.hire_date, e.termination_date, e.gender, e.birth_date, e.age,
                        e.phone, e.email, e.personal_email,
                        e.tin_number, e.sss_number, e.philhealth_number, e.pagibig_number, e.blood_type,
                        e.emergency_contact_name, e.emergency_contact_relation, e.emergency_contact_phone,
                        j.job_title, j.work_setup, j.work_type,
                        d.department_name,
                        l.city, l.province, l.region, l.country,
                        b.bank_name,
                        i.provider_name as health_insurance_provider
                    FROM `{DIM_EMPLOYEES}` e
                    LEFT JOIN `{DIM_JOBS}` j ON e.job_key = j.job_key
                    LEFT JOIN `{DIM_DEPARTMENTS}` d ON j.department_key = d.department_key
                    LEFT JOIN `{DIM_LOCATIONS}` l ON e.location_key = l.location_key
                    LEFT JOIN `{DIM_BANKS}` b ON e.bank_key = b.bank_key
                    LEFT JOIN `{DIM_INSURANCE}` i ON e.insurance_key = i.insurance_key
                """).to_dataframe()
                
                retailers_df = client.query(f"""
                    SELECT 
                        r.retailer_key, r.retailer_id, r.retailer_name, r.retailer_type,
                        l.city, l.province, l.region, l.country
                    FROM `{DIM_RETAILERS}` r
                    LEFT JOIN `{DIM_LOCATIONS}` l ON r.location_key = l.location_key
                """).to_dataframe()
                
                campaigns_df = client.query(f"SELECT campaign_key, campaign_id, campaign_name, campaign_type, start_date, end_date, budget, currency FROM `{DIM_CAMPAIGNS}`").to_dataframe()
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
        
        # Generate fact tables
        if not table_has_data(client, FACT_SALES):
            # Initial run: generate historical sales
            yesterday = date.today() - timedelta(days=1)
            logger.info(f"INITIAL_SALES_AMOUNT from config: {INITIAL_SALES_AMOUNT:,}")
            logger.info(f"Generating initial sales fact targeting ₱{INITIAL_SALES_AMOUNT:,.0f} (2015-01-01 to {yesterday})...")
            
            # Add progress monitoring for large data generation
            sales_start = time.time()
            logger.info("Starting sales data generation (this may take several minutes)...")
            
            sales = generate_fact_sales(
                employees, products, retailers, campaigns,
                INITIAL_SALES_AMOUNT,
                start_date=date(2015, 1, 1),
                end_date=yesterday
            )
            
            sales_elapsed = time.time() - sales_start
            logger.info(f"Sales generation completed in {sales_elapsed:.1f} seconds")
            logger.info(f"Generated {len(sales):,} sales records")
        else:
            # Daily run: generate today's sales
            today = date.today()
            logger.info(f"DAILY_SALES_AMOUNT from config: {DAILY_SALES_AMOUNT:,}")
            logger.info(f"Generating daily sales fact targeting ₱{DAILY_SALES_AMOUNT:,.0f} for {today}...")
            
            # Log scheduled run details
            if is_scheduled:
                logger.info(f"SCHEDULED RUN DETAILS:")
                logger.info(f"  Run date: {today}")
                logger.info(f"  Target amount: ₱{DAILY_SALES_AMOUNT:,.0f}")
                logger.info(f"  Active employees: {len([e for e in employees if e['employment_status'] == 'Active']):,}")
                logger.info(f"  Active products: {len([p for p in products if p['status'] == 'Active']):,}")
                logger.info(f"  Total retailers: {len(retailers):,}")
            
            # Get max sale key for continuity (optimized query)
            try:
                max_key_result = client.query(f"SELECT COALESCE(MAX(`sale_key`), 0) as max_key FROM `{FACT_SALES}` LIMIT 1").to_dataframe()
            except Exception as e:
                if "readsessions.create" in str(e):
                    logger.warning(f"BigQuery read sessions permission error. Using default start ID...")
                    max_key_result = pd.DataFrame({"max_key": [None]})
                else:
                    raise
            
            start_id = 1
            if not max_key_result.empty and pd.notna(max_key_result['max_key'].iloc[0]):
                start_id = int(max_key_result['max_key'].iloc[0]) + 1
            
            sales = generate_fact_sales(
                employees, products, retailers, campaigns,
                DAILY_SALES_AMOUNT,
                start_date=today,
                end_date=today,
                start_id=start_id
            )
        
        append_df_bq_safe(client, pd.DataFrame(sales), FACT_SALES, "sale_key")
        
        # Log sales generation summary
        logger.info(f"Sales generation completed:")
        logger.info(f"  Total sales records: {len(sales):,}")
        logger.info(f"  Total sales amount: ₱{sum(s['total_amount'] for s in sales):,.2f}")
        
        # Update delivery status only for scheduled runs
        if is_scheduled:
            logger.info("Updating delivery status...")
            update_delivery_status(client, FACT_SALES)
        
        # Generate other fact tables (only on initial run)
        logger.info("Generating additional fact tables...")
        
        if not table_has_data(client, FACT_OPERATING_COSTS):
            logger.info("\nGenerating operating costs fact...")
            costs = generate_fact_operating_costs(INITIAL_SALES_AMOUNT * 0.6)  # 60% of revenue
            append_df_bq(client, pd.DataFrame(costs), FACT_OPERATING_COSTS)
        else:
            logger.info("Operating costs table already exists. Skipping.")
        
        # Marketing costs generation
        logger.info(f"\nChecking marketing costs table: {FACT_MARKETING_COSTS}")
        marketing_table_exists = table_has_data(client, FACT_MARKETING_COSTS)
        logger.info(f"Marketing costs table exists: {marketing_table_exists}")
        
        if not marketing_table_exists:
            logger.info("Generating marketing costs fact...")
            try:
                # Use the same date range as sales data for consistency
                if not table_has_data(client, FACT_SALES):
                    # Initial run - use historical range
                    start_date = date(2015, 1, 1)
                    end_date = date.today() - timedelta(days=1)
                else:
                    # Daily run - use today's date
                    start_date = date.today()
                    end_date = date.today()
                
                marketing_costs = generate_fact_marketing_costs(
                    campaigns, 
                    INITIAL_SALES_AMOUNT * 0.15,  # 15% of revenue
                    start_date=start_date,
                    end_date=end_date
                )
                logger.info(f"Generated {len(marketing_costs):,} marketing cost records")
                if len(marketing_costs) > 0:
                    append_df_bq(client, pd.DataFrame(marketing_costs), FACT_MARKETING_COSTS)
                    logger.info("Marketing costs loaded successfully")
                else:
                    logger.warning("No marketing costs generated - skipping table creation")
            except Exception as e:
                logger.error(f"Error generating marketing costs: {str(e)}")
                raise
        else:
            logger.info("Marketing costs table already exists. Skipping.")
        
        if not table_has_data(client, FACT_INVENTORY):
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
