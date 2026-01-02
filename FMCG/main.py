"""
FMCG Data Simulator - Dimensional Model Version
Optimized for BigQuery with reduced storage using star schema
"""

import sys
import os
import logging
import pandas as pd
from datetime import datetime, timedelta, date

from config import PROJECT_ID, DATASET, INITIAL_SALES_AMOUNT, DAILY_SALES_AMOUNT

# Force correct values for debugging
INITIAL_SALES_AMOUNT = 6000000000  # ₱6B
DAILY_SALES_AMOUNT = 1640000      # ₱1.64M
from schema import (
    DIM_PRODUCTS, DIM_EMPLOYEES, DIM_RETAILERS, DIM_CAMPAIGNS,
    FACT_SALES, FACT_OPERATING_COSTS, FACT_INVENTORY, FACT_MARKETING_COSTS
)
from auth import get_bigquery_client
from helpers import table_has_data, append_df_bq, append_df_bq_safe, update_delivery_status
from generators.dimensional import (
    generate_dim_products, generate_dim_employees, generate_historical_employees, generate_dim_retailers,
    generate_dim_campaigns, generate_fact_sales,
    generate_fact_operating_costs, generate_fact_inventory, generate_fact_marketing_costs
)

# Configure simplified logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """
    Main entry point for FMCG Data Simulator - Dimensional Model
    """
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
        logger.info("Generating dimension tables...")
        
        # Generate dimension tables only if they don't exist
        if not table_has_data(client, DIM_PRODUCTS):
            logger.info("Generating products dimension...")
            products = generate_dim_products()
            append_df_bq(client, pd.DataFrame(products), DIM_PRODUCTS)
        # Generate employees dimension
        if not table_has_data(client, DIM_EMPLOYEES):
            logger.info("Generating employees dimension...")
            # Use historical employee generation for realistic growth patterns
            employees = generate_historical_employees(total_employees=900, current_active=250)
            append_df_bq(client, pd.DataFrame(employees), DIM_EMPLOYEES)
        else:
            logger.info("Employees dimension already exists. Skipping.")
        
        if not table_has_data(client, DIM_RETAILERS):
            logger.info("Generating retailers dimension...")
            retailers = generate_dim_retailers()
            append_df_bq(client, pd.DataFrame(retailers), DIM_RETAILERS)
        else:
            logger.info("Retailers dimension already exists. Skipping.")
        
        if not table_has_data(client, DIM_CAMPAIGNS):
            logger.info("Generating campaigns dimension...")
            campaigns = generate_dim_campaigns()
            append_df_bq(client, pd.DataFrame(campaigns), DIM_CAMPAIGNS)
        else:
            logger.info("Campaigns dimension already exists. Skipping.")
        
        # Load existing dimensions for fact table generation (optimized queries)
        try:
            # Use more efficient queries with specific fields only
            products_df = client.query(f"SELECT product_key, product_id, product_name, category, subcategory, brand, wholesale_price, retail_price, status FROM `{DIM_PRODUCTS}` WHERE status = 'Active'").to_dataframe()
            employees_df = client.query(f"SELECT employee_key, employee_id, full_name, department, position, employment_status, hire_date, termination_date, gender, birth_date, age, work_setup, work_type, monthly_salary, address_street, address_city, address_province, address_region, address_postal_code, address_country, phone, email, personal_email, tin_number, sss_number, philhealth_number, pagibig_number, blood_type, bank_name, account_number, account_name, performance_rating, last_review_date, training_completed, skills, health_insurance_provider, benefit_enrollment_date, years_of_service, attendance_rate, overtime_hours_monthly, engagement_score, satisfaction_index, vacation_leave_balance, sick_leave_balance, personal_leave_balance, emergency_contact_name, emergency_contact_relation, emergency_contact_phone FROM `{DIM_EMPLOYEES}` WHERE employment_status = 'Active'").to_dataframe()
            retailers_df = client.query(f"SELECT retailer_key, retailer_id, retailer_name, retailer_type, city, province, region, country FROM `{DIM_RETAILERS}`").to_dataframe()
            campaigns_df = client.query(f"SELECT campaign_key, campaign_id, campaign_name, campaign_type, start_date, end_date, budget, currency FROM `{DIM_CAMPAIGNS}` WHERE end_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)").to_dataframe()
        except Exception as e:
            if "readsessions.create" in str(e):
                logger.warning(f"BigQuery read sessions permission error. Using alternative approach...")
                # Use smaller queries without read sessions
                products_df = client.query(f"SELECT product_key, product_id, product_name, category, subcategory, brand, wholesale_price, retail_price, status FROM `{DIM_PRODUCTS}`").to_dataframe()
                employees_df = client.query(f"SELECT employee_key, employee_id, full_name, department, position, employment_status, hire_date, termination_date, gender, birth_date, age, work_setup, work_type, monthly_salary, address_street, address_city, address_province, address_region, address_postal_code, address_country, phone, email, personal_email, tin_number, sss_number, philhealth_number, pagibig_number, blood_type, bank_name, account_number, account_name, performance_rating, last_review_date, training_completed, skills, health_insurance_provider, benefit_enrollment_date, years_of_service, attendance_rate, overtime_hours_monthly, engagement_score, satisfaction_index, vacation_leave_balance, sick_leave_balance, personal_leave_balance, emergency_contact_name, emergency_contact_relation, emergency_contact_phone FROM `{DIM_EMPLOYEES}`").to_dataframe()
                retailers_df = client.query(f"SELECT retailer_key, retailer_id, retailer_name, retailer_type, city, province, region, country FROM `{DIM_RETAILERS}`").to_dataframe()
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
            sales = generate_fact_sales(
                employees, products, retailers, campaigns,
                INITIAL_SALES_AMOUNT,
                start_date=date(2015, 1, 1),
                end_date=yesterday
            )
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
                marketing_costs = generate_fact_marketing_costs(campaigns, INITIAL_SALES_AMOUNT * 0.15)  # 15% of revenue
                logger.info(f"Generated {len(marketing_costs):,} marketing cost records")
                append_df_bq(client, pd.DataFrame(marketing_costs), FACT_MARKETING_COSTS)
                logger.info("Marketing costs loaded successfully")
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
        
    except Exception as e:
        logger.error(f"\nERROR: {str(e)}")
        logger.error(f"Failed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        sys.exit(1)

if __name__ == "__main__":
    main()
