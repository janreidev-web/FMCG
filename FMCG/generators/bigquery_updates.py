"""
BigQuery Free Tier Update Methods
Implements the three official methods for updating data in BigQuery free tier
"""
import pandas as pd
from datetime import date, timedelta


def create_delivery_update_query(project_id, dataset, sales_table='fact_sales'):
    """
    Method 1: Overwrite table using WRITE_TRUNCATE
    Creates a query that updates delivery statuses and dates based on time logic
    """
    return f"""
    -- Update delivery statuses and dates using time-based progression
    SELECT 
        sale_id,
        sale_date,
        product_id,
        retailer_id,
        case_quantity,
        unit_price,
        discount_percent,
        discount_amount,
        tax_rate,
        tax_amount,
        total_amount,
        commission_amount,
        currency,
        payment_method,
        payment_status,
        -- Update delivery status based on age
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), sale_date, DAY) <= 2 THEN 'Processing'
            WHEN DATE_DIFF(CURRENT_DATE(), sale_date, DAY) <= 5 THEN 'In Transit'
            ELSE 'Delivered'
        END as delivery_status,
        -- Update delivery dates
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), sale_date, DAY) <= 2 THEN DATE_ADD(sale_date, INTERVAL 3 DAY)
            WHEN DATE_DIFF(CURRENT_DATE(), sale_date, DAY) <= 5 THEN DATE_ADD(sale_date, INTERVAL 2 DAY)
            ELSE DATE_ADD(sale_date, INTERVAL 1 DAY)
        END as expected_delivery_date,
        -- Set actual delivery date for delivered orders
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), sale_date, DAY) > 5 THEN DATE_ADD(sale_date, INTERVAL 3 DAY)
            ELSE NULL
        END as actual_delivery_date
    FROM `{project_id}.{dataset}.{sales_table}`
    WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    """


def create_delivery_update_with_new_data_query(project_id, dataset):
    """
    Method 2: Create new delivery update records
    Creates a query that generates delivery status update records
    """
    return f"""
    -- Generate delivery status update records
    SELECT 
        sale_id,
        CURRENT_DATE() as update_date,
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), sale_date, DAY) <= 2 THEN 'Processing'
            WHEN DATE_DIFF(CURRENT_DATE(), sale_date, DAY) <= 5 THEN 'In Transit'
            ELSE 'Delivered'
        END as new_status,
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), sale_date, DAY) <= 2 THEN DATE_ADD(sale_date, INTERVAL 3 DAY)
            WHEN DATE_DIFF(CURRENT_DATE(), sale_date, DAY) <= 5 THEN DATE_ADD(sale_date, INTERVAL 2 DAY)
            ELSE DATE_ADD(sale_date, INTERVAL 1 DAY)
        END as new_expected_delivery,
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), sale_date, DAY) > 5 THEN DATE_ADD(sale_date, INTERVAL 3 DAY)
            ELSE NULL
        END as new_actual_delivery
    FROM `{project_id}.{dataset}.fact_sales`
    WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    """


def create_current_delivery_status_view(project_id, dataset, sales_table='fact_sales', updates_table='delivery_status_updates'):
    """
    Method 3: Create a view that shows current delivery status
    Creates a view that combines original data with latest updates
    """
    return f"""
    -- Create view with current delivery status
    CREATE OR REPLACE VIEW `{project_id}.{dataset}.current_delivery_status` AS
    SELECT 
        s.sale_id,
        s.sale_date,
        s.product_id,
        s.retailer_id,
        s.case_quantity,
        s.unit_price,
        s.discount_percent,
        s.discount_amount,
        s.tax_rate,
        s.tax_amount,
        s.total_amount,
        s.commission_amount,
        s.currency,
        s.payment_method,
        s.payment_status,
        COALESCE(u.new_status, s.delivery_status) as delivery_status,
        COALESCE(u.new_expected_delivery, s.expected_delivery_date) as expected_delivery_date,
        COALESCE(u.new_actual_delivery, s.actual_delivery_date) as actual_delivery_date
    FROM `{project_id}.{dataset}.{sales_table}` s
    LEFT JOIN (
        SELECT DISTINCT sale_id, new_status, new_expected_delivery, new_actual_delivery
        FROM `{project_id}.{dataset}.{updates_table}`
        WHERE (sale_id, update_date) IN (
            SELECT sale_id, MAX(update_date)
            FROM `{project_id}.{dataset}.{updates_table}`
            GROUP BY sale_id
        )
    ) u ON s.sale_id = u.sale_id
    """


def execute_method_1_overwrite(client, project_id, dataset, sales_table='fact_sales'):
    """
    Execute Method 1: Overwrite table with updated delivery statuses
    """
    from google.cloud import bigquery
    
    query = create_delivery_update_query(project_id, dataset, sales_table)
    
    job_config = bigquery.QueryJobConfig(
        destination=f'{project_id}.{dataset}.{sales_table}',
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite existing table
    )
    
    try:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for completion
        
        print(f"✅ Method 1: Updated delivery statuses in {sales_table}")
        print(f"   Affected rows: {query_job.num_dml_affected_rows if hasattr(query_job, 'num_dml_affected_rows') else 'Unknown'}")
        
        return True
    except Exception as e:
        print(f"❌ Method 1 failed: {e}")
        return False


def execute_method_2_append(client, project_id, dataset, updates_table='delivery_status_updates'):
    """
    Execute Method 2: Append new delivery update records
    """
    from google.cloud import bigquery
    
    query = create_delivery_update_with_new_data_query(project_id, dataset)
    
    job_config = bigquery.QueryJobConfig(
        destination=f'{project_id}.{dataset}.{updates_table}',
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Append to existing table
    )
    
    try:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for completion
        
        rows_updated = query_job.num_dml_affected_rows if hasattr(query_job, 'num_dml_affected_rows') else 0
        print(f"✅ Method 2: Appended {rows_updated} delivery status updates to {updates_table}")
        
        return True
    except Exception as e:
        print(f"❌ Method 2 failed: {e}")
        return False


def execute_method_3_staging(client, project_id, dataset, sales_table='fact_sales'):
    """
    Execute Method 3: Create staging table and view
    """
    try:
        # Create current status view
        view_query = create_current_delivery_status_view(project_id, dataset, sales_table, 'delivery_status_updates')
        client.query(view_query).result()
        print("✅ Method 3: Created current delivery status view")
        return True
    except Exception as e:
        print(f"❌ Method 3 failed: {e}")
        return False


def get_delivery_status_summary(client, project_id, dataset):
    """
    Get summary of current delivery statuses
    """
    try:
        summary_query = f"""
        SELECT 
            delivery_status,
            COUNT(*) as order_count,
            SUM(total_amount) as total_value
        FROM `{project_id}.{dataset}.fact_sales`
        WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY delivery_status
        ORDER BY order_count DESC
        """
        
        result_df = client.query(summary_query).to_dataframe()
        return result_df
    except Exception as e:
        print(f"❌ Could not get delivery status summary: {e}")
        return pd.DataFrame()


def compare_update_methods(project_id, dataset):
    """
    Compare the three update methods and provide recommendations
    """
    print("="*60)
    print("BIGQUERY UPDATE METHODS COMPARISON")
    print("="*60)
    
    print("\n📊 Method 1: Direct Table Overwrite")
    print("   Pros: Simple, no audit trail needed")
    print("   Cons: Loses historical data, higher risk")
    print("   Best for: Small datasets, simple updates")
    
    print("\n📊 Method 2: Append Update Records")
    print("   Pros: Maintains audit trail, incremental")
    print("   Cons: More complex, requires additional storage")
    print("   Best for: Medium datasets, need history")
    
    print("\n📊 Method 3: Staging Table + View")
    print("   Pros: Full audit trail, flexible queries")
    print("   Cons: Most complex, highest storage cost")
    print("   Best for: Large datasets, complex analysis")
    
    print("\n💡 Recommendation for Free Tier:")
    print("   - Start with Method 1 for simplicity")
    print("   - Use Method 2 if you need audit trail")
    print("   - Method 3 only for complex requirements")
    
    print("="*60)
