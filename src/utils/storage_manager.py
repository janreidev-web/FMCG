"""
BigQuery Storage Management and Optimization for FMCG Data Analytics Platform
Handles storage quota issues and provides data archiving strategies
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from google.cloud import bigquery
import logging

# Simple logger fallback
try:
    from src.utils.logger import default_logger
except ImportError:
    logging.basicConfig(level=logging.INFO)
    default_logger = logging.getLogger(__name__)


class BigQueryStorageManager:
    """Manages BigQuery storage usage and optimization"""
    
    def __init__(self, project_id: str, dataset: str):
        self.project_id = project_id
        self.dataset = dataset
        self.client = bigquery.Client(project=project_id)
        self.logger = default_logger
        
        # Free tier storage limit: 10 GB
        self.free_storage_limit_gb = 10
        self.warning_threshold_gb = 8  # Warn at 80% of limit
        self.critical_threshold_gb = 9.5  # Critical at 95% of limit
    
    def get_storage_usage(self) -> Dict:
        """Get current storage usage statistics"""
        
        self.logger.info("Analyzing BigQuery storage usage...")
        
        try:
            # Get dataset storage information
            dataset_ref = self.client.dataset(self.dataset)
            dataset_info = self.client.get_dataset(dataset_ref)
            
            # Get table storage details
            tables = list(self.client.list_tables(dataset_ref))
            table_details = []
            total_storage_bytes = 0
            
            for table in tables:
                table_ref = dataset_ref.table(table.table_id)
                table_info = self.client.get_table(table_ref)
                
                storage_bytes = table_info.num_bytes or 0
                storage_mb = storage_bytes / (1024 * 1024)
                storage_gb = storage_mb / 1024
                
                row_count = table_info.num_rows or 0
                
                table_details.append({
                    'table_id': table.table_id,
                    'storage_bytes': storage_bytes,
                    'storage_mb': storage_mb,
                    'storage_gb': storage_gb,
                    'row_count': row_count,
                    'created': table_info.created,
                    'modified': table_info.modified
                })
                
                total_storage_bytes += storage_bytes
            
            total_storage_gb = total_storage_bytes / (1024 * 1024 * 1024)
            
            # Sort by storage usage (largest first)
            table_details.sort(key=lambda x: x['storage_gb'], reverse=True)
            
            # Determine status
            if total_storage_gb >= self.critical_threshold_gb:
                status = "CRITICAL"
            elif total_storage_gb >= self.warning_threshold_gb:
                status = "WARNING"
            else:
                status = "OK"
            
            return {
                'status': status,
                'total_storage_gb': total_storage_gb,
                'total_storage_bytes': total_storage_bytes,
                'free_storage_limit_gb': self.free_storage_limit_gb,
                'usage_percentage': (total_storage_gb / self.free_storage_limit_gb) * 100,
                'table_details': table_details,
                'table_count': len(tables),
                'analysis_date': datetime.now()
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get storage usage: {str(e)}")
            return {
                'status': 'ERROR',
                'error': str(e),
                'total_storage_gb': 0,
                'table_details': []
            }
    
    def identify_archiving_candidates(self, days_to_keep: int = 365) -> Dict:
        """Identify tables and records that can be archived"""
        
        self.logger.info(f"Identifying archiving candidates (keeping {days_to_keep} days)...")
        
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        archiving_candidates = {}
        
        try:
            # Check fact tables for old data
            fact_tables = ['fact_sales', 'fact_inventory', 'fact_operating_costs', 
                          'fact_marketing_costs', 'fact_employees']
            
            for table_name in fact_tables:
                # Query to count old records
                query = f"""
                SELECT 
                    COUNT(*) as old_record_count,
                    MIN(date) as oldest_date,
                    MAX(date) as newest_date,
                    COUNT(*) * 1000 as estimated_storage_mb  -- Rough estimate
                FROM `{self.project_id}.{self.dataset}.{table_name}`
                WHERE date < '{cutoff_date.date()}'
                """
                
                try:
                    result = self.client.query(query).to_dataframe()
                    
                    if len(result) > 0 and result['old_record_count'].iloc[0] > 0:
                        archiving_candidates[table_name] = {
                            'old_record_count': int(result['old_record_count'].iloc[0]),
                            'oldest_date': result['oldest_date'].iloc[0],
                            'newest_date': result['newest_date'].iloc[0],
                            'estimated_storage_mb': float(result['estimated_storage_mb'].iloc[0]),
                            'cutoff_date': cutoff_date.date()
                        }
                
                except Exception as e:
                    self.logger.warning(f"Could not analyze {table_name}: {str(e)}")
                    continue
            
            return archiving_candidates
            
        except Exception as e:
            self.logger.error(f"Failed to identify archiving candidates: {str(e)}")
            return {}
    
    def archive_old_data(self, table_name: str, cutoff_date: str, 
                        archive_table_suffix: str = "_archive") -> bool:
        """Archive old data to a separate table"""
        
        self.logger.info(f"Archiving old data from {table_name} before {cutoff_date}...")
        
        try:
            # Create archive table if it doesn't exist
            archive_table_name = f"{table_name}{archive_table_suffix}"
            
            # Get original table schema
            original_table_ref = self.client.dataset(self.dataset).table(table_name)
            original_table = self.client.get_table(original_table_ref)
            
            # Create archive table with same schema
            archive_table_ref = self.client.dataset(self.dataset).table(archive_table_name)
            
            try:
                archive_table = self.client.get_table(archive_table_ref)
                self.logger.info(f"Archive table {archive_table_name} already exists")
            except:
                # Create archive table
                archive_table = bigquery.Table(archive_table_ref, schema=original_table.schema)
                archive_table = self.client.create_table(archive_table)
                self.logger.info(f"Created archive table {archive_table_name}")
            
            # Move old data to archive table
            insert_query = f"""
            INSERT INTO `{self.project_id}.{self.dataset}.{archive_table_name}`
            SELECT *
            FROM `{self.project_id}.{self.dataset}.{table_name}`
            WHERE date < '{cutoff_date}'
            """
            
            insert_job = self.client.query(insert_query)
            insert_job.result()  # Wait for completion
            
            archived_count = insert_job.num_dml_affected_rows
            self.logger.info(f"Archived {archived_count:,} records from {table_name}")
            
            # Delete old data from main table
            delete_query = f"""
            DELETE FROM `{self.project_id}.{self.dataset}.{table_name}`
            WHERE date < '{cutoff_date}'
            """
            
            delete_job = self.client.query(delete_query)
            delete_job.result()  # Wait for completion
            
            deleted_count = delete_job.num_dml_affected_rows
            self.logger.info(f"Deleted {deleted_count:,} records from {table_name}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to archive {table_name}: {str(e)}")
            return False
    
    def create_aggregated_views(self) -> bool:
        """Create aggregated views to reduce storage needs"""
        
        self.logger.info("Creating aggregated views for historical data...")
        
        try:
            # Create monthly aggregated sales view
            monthly_sales_view = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset}.monthly_sales_summary` AS
            SELECT
                DATE_TRUNC(date, MONTH) as month,
                product_id,
                retailer_id,
                SUM(quantity) as total_quantity,
                AVG(unit_price) as avg_unit_price,
                SUM(total_amount) as total_amount,
                SUM(discount_amount) as total_discount,
                COUNT(*) as transaction_count
            FROM `{self.project_id}.{self.dataset}.fact_sales`
            GROUP BY DATE_TRUNC(date, MONTH), product_id, retailer_id
            """
            
            self.client.query(monthly_sales_view).result()
            self.logger.info("Created monthly_sales_summary view")
            
            # Create monthly inventory view
            monthly_inventory_view = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset}.monthly_inventory_summary` AS
            SELECT
                DATE_TRUNC(date, MONTH) as month,
                product_id,
                location_id,
                SUM(opening_stock) as total_opening_stock,
                SUM(closing_stock) as total_closing_stock,
                SUM(stock_received) as total_stock_received,
                SUM(stock_sold) as total_stock_sold,
                SUM(COALESCE(stock_lost, 0)) as total_stock_lost,
                AVG(unit_cost) as avg_unit_cost,
                SUM(total_value) as total_value
            FROM `{self.project_id}.{self.dataset}.fact_inventory`
            GROUP BY DATE_TRUNC(date, MONTH), product_id, location_id
            """
            
            self.client.query(monthly_inventory_view).result()
            self.logger.info("Created monthly_inventory_summary view")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create aggregated views: {str(e)}")
            return False
    
    def optimize_table_storage(self, table_name: str) -> bool:
        """Optimize table storage by clustering and partitioning"""
        
        self.logger.info(f"Optimizing storage for {table_name}...")
        
        try:
            # Get table reference
            table_ref = self.client.dataset(self.dataset).table(table_name)
            table = self.client.get_table(table_ref)
            
            # Check if table is already partitioned by date
            if not table.time_partitioning:
                # Create partitioned copy
                partitioned_table_name = f"{table_name}_partitioned"
                
                create_query = f"""
                CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset}.{partitioned_table_name}`
                PARTITION BY date
                CLUSTER BY product_id
                AS
                SELECT * FROM `{self.project_id}.{self.dataset}.{table_name}`
                """
                
                self.client.query(create_query).result()
                self.logger.info(f"Created partitioned table {partitioned_table_name}")
                
                # Note: Manual intervention required to replace original table
                self.logger.warning(f"Manual step required: Replace {table_name} with {partitioned_table_name}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to optimize {table_name}: {str(e)}")
            return False
    
    def generate_storage_report(self) -> Dict:
        """Generate comprehensive storage management report"""
        
        storage_usage = self.get_storage_usage()
        archiving_candidates = self.identify_archiving_candidates()
        
        report = {
            'storage_analysis': storage_usage,
            'archiving_opportunities': archiving_candidates,
            'recommendations': [],
            'estimated_savings': 0
        }
        
        # Generate recommendations
        if storage_usage['status'] == 'CRITICAL':
            report['recommendations'].append("URGENT: Archive old data immediately to free up storage")
            report['recommendations'].append("Consider upgrading to paid BigQuery tier")
        
        if storage_usage['status'] == 'WARNING':
            report['recommendations'].append("Archive data older than 1 year")
            report['recommendations'].append("Create aggregated views for historical analysis")
        
        # Calculate potential savings
        for table, candidates in archiving_candidates.items():
            report['estimated_savings'] += candidates['estimated_storage_mb']
        
        report['estimated_savings_gb'] = report['estimated_savings'] / 1024
        
        return report


class StorageOptimizedSynchronizer:
    """Storage-optimized version of the inventory-sales synchronizer"""
    
    def __init__(self, bigquery_client, storage_manager: BigQueryStorageManager):
        self.bq_client = bigquery_client
        self.storage_manager = storage_manager
        self.logger = default_logger
        
        # Limit data processing to avoid storage issues
        self.max_days_to_process = 30
        self.batch_size = 1000
    
    def check_storage_before_sync(self) -> bool:
        """Check storage availability before synchronization"""
        
        storage_usage = self.storage_manager.get_storage_usage()
        
        if storage_usage['status'] == 'CRITICAL':
            self.logger.error("Storage quota exceeded. Cannot proceed with synchronization.")
            return False
        elif storage_usage['status'] == 'WARNING':
            self.logger.warning("Storage usage high. Consider archiving old data.")
            return True
        else:
            self.logger.info("Storage usage OK. Proceeding with synchronization.")
            return True
    
    def run_storage_aware_sync(self, days_back: int = 30) -> Dict:
        """Run synchronization with storage constraints"""
        
        if not self.check_storage_before_sync():
            return {'status': 'FAILED', 'reason': 'Storage quota exceeded'}
        
        # Limit processing to recent data
        days_to_process = min(days_back, self.max_days_to_process)
        
        self.logger.info(f"Running storage-aware synchronization for {days_to_process} days...")
        
        # Import the original synchronizer with modified parameters
        try:
            from src.utils.inventory_sales_sync import InventorySalesSynchronizer
            
            synchronizer = InventorySalesSynchronizer(self.bq_client)
            
            # Load limited date range
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days_to_process)
            
            synchronizer.load_data(str(start_date), str(end_date))
            
            # Run analysis
            sync_analysis = synchronizer.analyze_synchronization_gaps()
            report = synchronizer.generate_synchronization_report(sync_analysis)
            
            return {
                'status': 'SUCCESS',
                'sync_analysis': sync_analysis,
                'report': report,
                'date_range': f"{start_date} to {end_date}",
                'storage_usage': self.storage_manager.get_storage_usage()
            }
            
        except Exception as e:
            self.logger.error(f"Storage-aware sync failed: {str(e)}")
            return {'status': 'FAILED', 'reason': str(e)}


def run_storage_optimization(project_id: str, dataset: str) -> Dict:
    """Run complete storage optimization workflow"""
    
    storage_manager = BigQueryStorageManager(project_id, dataset)
    
    # Generate storage report
    report = storage_manager.generate_storage_report()
    
    print("\n" + "="*80)
    print("BIGQUERY STORAGE OPTIMIZATION REPORT")
    print("="*80)
    
    storage = report['storage_analysis']
    print(f"\n📊 Storage Status: {storage['status']}")
    print(f"   Current Usage: {storage['total_storage_gb']:.2f} GB")
    print(f"   Free Limit: {storage['free_storage_limit_gb']:.0f} GB")
    print(f"   Usage Percentage: {storage['usage_percentage']:.1f}%")
    print(f"   Table Count: {storage['table_count']}")
    
    if storage['table_details']:
        print(f"\n📋 Top 5 Tables by Storage:")
        for table in storage['table_details'][:5]:
            print(f"   {table['table_id']}: {table['storage_gb']:.2f} GB ({table['row_count']:,} rows)")
    
    if report['archiving_opportunities']:
        print(f"\n🗄️  Archiving Opportunities:")
        for table, info in report['archiving_opportunities'].items():
            print(f"   {table}: {info['old_record_count']:,} records ({info['estimated_storage_mb']:.1f} MB)")
    
    print(f"\n💡 Recommendations:")
    for i, rec in enumerate(report['recommendations'], 1):
        print(f"   {i}. {rec}")
    
    print(f"\n💰 Estimated Storage Savings: {report['estimated_savings_gb']:.2f} GB")
    
    return report
