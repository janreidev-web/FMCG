import logging
import random
import pandas as pd
from datetime import datetime, timedelta, date
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('fmcg_simulator.log')
    ]
)
logger = logging.getLogger(__name__)

def table_has_data(client, table_id):
    """Check if a BigQuery table has data"""
    try:
        logger.info(f"Checking if table {table_id} has data...")
        df = client.query(f"SELECT COUNT(1) AS cnt FROM `{table_id}` LIMIT 1").to_dataframe()
        has_data = df['cnt'][0] > 0
        logger.info(f"Table {table_id} has data: {has_data}")
        return has_data
    except Exception as e:
        logger.error(f"Error checking table {table_id}: {str(e)}")
        return False

def ensure_bigquery_dtypes(df, table_id):
    """
    Ensure DataFrame columns have proper data types for BigQuery compatibility
    
    Args:
        df: pandas DataFrame to convert
        table_id: Target BigQuery table name (for logging)
    
    Returns:
        DataFrame with corrected data types
    """
    logger.info(f"Ensuring BigQuery data type compatibility for {table_id}...")
    
    # Import schema definitions
    try:
        from schema import (
            FACT_EMPLOYEES_SCHEMA, FACT_MARKETING_COSTS_SCHEMA, 
            FACT_SALES_SCHEMA, FACT_INVENTORY_SCHEMA,
            FACT_EMPLOYEE_WAGES_SCHEMA, FACT_OPERATING_COSTS_SCHEMA,
            DIM_JOBS_SCHEMA, DIM_EMPLOYEES_SCHEMA, DIM_PRODUCTS_SCHEMA,
            DIM_RETAILERS_SCHEMA, DIM_CAMPAIGNS_SCHEMA, DIM_LOCATIONS_SCHEMA,
            DIM_DEPARTMENTS_SCHEMA, DIM_BANKS_SCHEMA, DIM_INSURANCE_SCHEMA,
            DELIVERY_STATUS_UPDATES_SCHEMA
        )
        
        # Schema mapping for all tables with integer columns
        schema_map = {
            'fact_employees': FACT_EMPLOYEES_SCHEMA,
            'fact_marketing_costs': FACT_MARKETING_COSTS_SCHEMA,
            'fact_sales': FACT_SALES_SCHEMA,
            'fact_inventory': FACT_INVENTORY_SCHEMA,
            'fact_employee_wages': FACT_EMPLOYEE_WAGES_SCHEMA,
            'fact_operating_costs': FACT_OPERATING_COSTS_SCHEMA,
            'dim_jobs': DIM_JOBS_SCHEMA,
            'dim_employees': DIM_EMPLOYEES_SCHEMA,
            'dim_products': DIM_PRODUCTS_SCHEMA,
            'dim_retailers': DIM_RETAILERS_SCHEMA,
            'dim_campaigns': DIM_CAMPAIGNS_SCHEMA,
            'dim_locations': DIM_LOCATIONS_SCHEMA,
            'dim_departments': DIM_DEPARTMENTS_SCHEMA,
            'dim_banks': DIM_BANKS_SCHEMA,
            'dim_insurance': DIM_INSURANCE_SCHEMA,
            'delivery_status_updates': DELIVERY_STATUS_UPDATES_SCHEMA
        }
        
        # Get table name from full table_id
        table_name = table_id.split('.')[-1] if '.' in table_id else table_id
        
        if table_name in schema_map:
            schema = schema_map[table_name]
            
            # Convert ID columns to proper dtype (string) and other integer columns to int64
            for field in schema:
                if field['name'] in df.columns:
                    col_name = field['name']
                    field_type = field['type']
                    
                    if field_type == 'STRING':
                        # Convert ID columns and other string columns to string
                        logger.info(f"Converting column '{col_name}' to STRING for BigQuery compatibility")
                        df[col_name] = df[col_name].astype(str)
                    elif field_type == 'INTEGER':
                        # Convert metric columns to integer
                        logger.info(f"Converting column '{col_name}' to INTEGER for BigQuery compatibility")
                        # Convert to regular integer type for BigQuery compatibility
                        # First handle any missing values by filling with 0 or appropriate default
                        df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                        # Fill NaN values with 0 for integer columns
                        df[col_name] = df[col_name].fillna(0).astype('int64')
                    
        logger.info(f"Data type conversion completed for {table_name}")
        
    except ImportError:
        logger.warning("Could not import schema definitions for data type conversion")
    except Exception as e:
        logger.warning(f"Error during data type conversion: {e}")
    
    return df

def append_df_bq(client, df, table_id, write_disposition="WRITE_APPEND"):
    """Append DataFrame to BigQuery table with proper null handling for Power BI compatibility"""
    logger.info(f"Preparing to load {len(df):,} rows into {table_id}...")
    
    try:
        # Ensure proper null handling - pandas will convert pd.NaT and None to NULL in BigQuery
        df = df.copy()
        
        # Ensure proper data types for BigQuery compatibility
        df = ensure_bigquery_dtypes(df, table_id)
        
        # Replace pd.NaT in non-datetime columns with None for better compatibility
        for col in df.columns:
            if df[col].dtype == 'object':  # String/object columns
                df[col] = df[col].replace({pd.NaT: None})
        
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition, autodetect=True)
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        
        # Add timeout to prevent hanging
        logger.info(f"Starting BigQuery load job for {table_id}...")
        try:
            # Wait for job completion with timeout (5 minutes)
            job.result(timeout=300)
            logger.info(f"✓ Loaded {len(df):,} rows → {table_id}")
        except Exception as timeout_error:
            if "timeout" in str(timeout_error).lower():
                logger.error(f"Timeout loading data into {table_id}. Job may still be running...")
                logger.error(f"Job ID: {job.job_id}")
                logger.error(f"Check BigQuery console for job status")
                raise TimeoutError(f"BigQuery load job timed out for {table_id}")
            else:
                raise
        
    except Exception as e:
        logger.error(f"Failed to load data into {table_id}: {str(e)}")
        raise

def append_df_bq_safe(client, df, table_id, id_column, write_disposition="WRITE_APPEND"):
    """
    Append DataFrame to BigQuery table with duplicate ID prevention.
    Checks for existing IDs and only appends unique records.
    
    Args:
        client: BigQuery client
        df: DataFrame to append
        table_id: Full BigQuery table ID (project.dataset.table)
        id_column: Name of the ID column to check for duplicates (e.g., 'Product ID')
        write_disposition: BigQuery write disposition (default: WRITE_APPEND)
    
    Returns:
        Number of rows actually appended (after filtering duplicates)
    """
    if df.empty:
        logger.warning(f"No data to append to {table_id}")
        # For empty DataFrame, we still want to ensure the table exists
        # Check if table exists, and if not, create it with schema
        try:
            client.get_table(table_id)
            logger.info(f"Table {table_id} already exists")
        except Exception:
            logger.info(f"Table {table_id} doesn't exist, creating empty table...")
            # Import schema for fact_sales table
            try:
                from schema import FACT_SALES_SCHEMA
                # Create table with schema
                table = bigquery.Table(table_id, schema=FACT_SALES_SCHEMA)
                client.create_table(table)
                logger.info(f"✓ Created empty table {table_id} with schema")
            except ImportError:
                logger.warning(f"Cannot create empty table {table_id} - schema not available")
            except Exception as e:
                logger.error(f"Failed to create table {table_id}: {e}")
        return 0
    
    logger.info(f"Checking for duplicates in {table_id} using {id_column}...")
    df = df.copy()
    
    # Ensure proper data types for BigQuery compatibility
    df = ensure_bigquery_dtypes(df, table_id)
    
    # Replace pd.NaT in non-datetime columns with None for better compatibility
    for col in df.columns:
        if df[col].dtype == 'object':  # String/object columns
            df[col] = df[col].replace({pd.NaT: None})
    
    # Check if ID column exists
    if id_column not in df.columns:
        error_msg = f"ID column '{id_column}' not found in DataFrame columns: {list(df.columns)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    original_count = len(df)
    
    # Query existing IDs from BigQuery
    try:
        # Get list of IDs to check
        ids_to_check = df[id_column].unique().tolist()
        
        if ids_to_check:
            logger.info(f"Checking {len(ids_to_check):,} IDs for duplicates...")
            
            # Process IDs in batches to avoid query size limits
            batch_size = 1000  # Process 1000 IDs at a time
            existing_ids_set = set()
            
            for i in range(0, len(ids_to_check), batch_size):
                batch_ids = ids_to_check[i:i + batch_size]
                escaped_ids = [str(id_val).replace("'", "''") for id_val in batch_ids]
                id_list_str = ", ".join([f"'{id_val}'" for id_val in escaped_ids])
                
                existing_ids_query = f"""
                    SELECT DISTINCT `{id_column}` as existing_id
                    FROM `{table_id}`
                    WHERE `{id_column}` IN ({id_list_str})
                """
                
                try:
                    existing_ids_df = client.query(existing_ids_query).to_dataframe()
                    if not existing_ids_df.empty:
                        existing_ids_set.update(existing_ids_df['existing_id'].tolist())
                except Exception as batch_error:
                    if "too large" in str(batch_error).lower() or "limit" in str(batch_error).lower():
                        # If still too large, reduce batch size further
                        smaller_batch_size = 500
                        for j in range(0, len(batch_ids), smaller_batch_size):
                            smaller_batch = batch_ids[j:j + smaller_batch_size]
                            smaller_escaped = [str(id_val).replace("'", "''") for id_val in smaller_batch]
                            smaller_id_list = ", ".join([f"'{id_val}'" for id_val in smaller_escaped])
                            
                            smaller_query = f"""
                                SELECT DISTINCT `{id_column}` as existing_id
                                FROM `{table_id}`
                                WHERE `{id_column}` IN ({smaller_id_list})
                                LIMIT 100
                            """
                            
                            try:
                                smaller_df = client.query(smaller_query).to_dataframe()
                                if not smaller_df.empty:
                                    existing_ids_set.update(smaller_df['existing_id'].tolist())
                            except Exception:
                                # If still fails, skip this batch and continue
                                logger.warning(f"Could not check batch {j//smaller_batch_size + 1}, skipping...")
                                continue
                    else:
                        logger.warning(f"Batch {i//batch_size + 1} failed: {batch_error}")
                        continue
            
            if existing_ids_set:
                duplicate_count = len(df[df[id_column].isin(existing_ids_set)])
                
                if duplicate_count > 0:
                    logger.warning(f"Found {duplicate_count:,} duplicate {id_column}(s). Filtering them out...")
                    # Filter out rows with existing IDs
                    df = df[~df[id_column].isin(existing_ids_set)]
                    
                    if df.empty:
                        logger.warning(f"All records were duplicates. Nothing to append to {table_id}")
                        return 0
                    
                    logger.info(f"✓ Filtered to {len(df):,} unique records (removed {duplicate_count:,} duplicates)")
                else:
                    logger.info(f"✓ No duplicates found. All {len(df):,} records are unique.")
            else:
                logger.info(f"✓ No existing IDs found. All {len(df):,} records are new.")
    except Exception as e:
        # If table doesn't exist or query fails, proceed with append (first load scenario)
        if "Not found" in str(e) or "does not exist" in str(e).lower():
            logger.info(f"Table doesn't exist yet or is empty. Proceeding with initial load...")
        else:
            # For other errors, log but proceed (better to try than fail)
            logger.warning(f"Could not check for duplicates: {str(e)}. Proceeding with append...")
    
    # Append the filtered dataframe
    if not df.empty:
        try:
            job_config = bigquery.LoadJobConfig(write_disposition=write_disposition, autodetect=True)
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            logger.info(f"✓ Loaded {len(df):,} rows → {table_id}")
            return len(df)
        except Exception as e:
            logger.error(f"Failed to load data into {table_id}: {str(e)}")
            raise
    else:
        return 0

def random_date(start_year=2015, end_year=2025):
    """Generate random date between years"""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end-start).days))

def random_birth_date(min_age=20, max_age=65):
    """Generate random birth date based on age range"""
    today = datetime.now()
    birth_year = today.year - random.randint(min_age, max_age)
    return datetime(birth_year, random.randint(1,12), random.randint(1,28))

def random_termination_date(hire_date):
    """Generate random termination date after hire date"""
    min_date = hire_date + timedelta(days=30)
    max_date = datetime.now()
    if min_date > max_date:
        return None
    return min_date + timedelta(days=random.randint(0, (max_date-min_date).days))

def random_date_range(start_date, end_date):
    """Generate random date between start_date and end_date (inclusive)"""
    if isinstance(start_date, date) and not isinstance(start_date, datetime):
        start_date = datetime.combine(start_date, datetime.min.time())
    if isinstance(end_date, date) and not isinstance(end_date, datetime):
        end_date = datetime.combine(end_date, datetime.min.time())
    delta = (end_date - start_date).days
    if delta < 0:
        delta = 0
    return start_date + timedelta(days=random.randint(0, delta))

def update_delivery_status(client, fact_sales_table):
    """Check and report delivery status without DML operations (free tier compatible)"""
    logger.info("="*60)
    logger.info("DELIVERY STATUS MONITOR")
    logger.info("="*60)
    
    try:
        today = datetime.now().date()
        logger.info(f"Checking delivery status for orders as of {today}")
        
        # Read-only query to check orders by status
        # This provides a summary of current delivery statuses
        check_query = f"""
            SELECT 
                `sale_id`,
                `sale_date`,
                `delivery_status`,
                `total_amount`,
                `retailer_id`,
                CASE 
                    WHEN `delivery_status` IN ('In Transit', 'Processing', 'Pending')
                    THEN 'Active Order'
                    ELSE 'Completed Order'
                END as order_status
            FROM `{fact_sales_table}`
            WHERE `sale_date` >= DATE_SUB('{today}', INTERVAL 30 DAY)
            ORDER BY `sale_date` DESC
            LIMIT 100
        """
        
        # Execute the read-only query
        result_df = client.query(check_query).to_dataframe()
        
        if result_df.empty:
            logger.info("No recent orders found in the last 30 days.")
            return
        
        # Count orders by status
        status_summary = result_df.groupby(['delivery_status', 'order_status']).agg({
            'sale_id': 'count',
            'total_amount': 'sum'
        }).rename(columns={'sale_id': 'count'})
        
        logger.info(f"Found {len(result_df):,} recent orders:")
        
        for (delivery_status, order_status), group in status_summary.iterrows():
            count = int(group['count'])
            amount = float(group['total_amount'])
            logger.info(f"  {delivery_status} - {order_status}: {count:,} orders (PHP {amount:,.2f})")
        
        # Identify active orders
        active_orders = result_df[result_df['order_status'] == 'Active Order']
        
        if not active_orders.empty:
            logger.info(f"\nActive Orders ({len(active_orders)} orders):")
            total_value = active_orders['total_amount'].sum()
            logger.info(f"   Total Value: PHP {total_value:,.2f}")
            
            # Show sample of active orders
            sample_size = min(5, len(active_orders))
            logger.info(f"\nSample Active Orders (showing {sample_size} of {len(active_orders)}):")
            for _, order in active_orders.head(sample_size).iterrows():
                order_date = pd.to_datetime(order['sale_date']).date()
                days_active = (today - order_date).days
                logger.info(f"   Order #{order['sale_id']}: PHP {order['total_amount']:,.2f} ({days_active} days ago)")
        else:
            logger.info("No active orders found. All recent orders are delivered.")
        
        # Overall delivery status summary
        overall_summary = result_df.groupby('delivery_status').agg({
            'sale_id': 'count',
            'total_amount': 'sum'
        }).rename(columns={'sale_id': 'count'})
        
        logger.info(f"\nOverall Delivery Status Summary:")
        for status, group in overall_summary.iterrows():
            count = int(group['count'])
            amount = float(group['total_amount'])
            percentage = (count / len(result_df)) * 100
            logger.info(f"   {status}: {count:,} orders ({percentage:.1f}%) - PHP {amount:,.2f}")
        
        logger.info(f"\nNote: Delivery status monitoring only (free tier compatible).")
        logger.info(f"   No delivery date tracking - status only.")
        
    except Exception as e:
        logger.error(f"Error checking delivery status: {str(e)}")
        # Don't raise the error - just log it so the scheduled run can continue
        logger.info("Continuing with scheduled run despite delivery status check failure...")

