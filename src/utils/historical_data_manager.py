"""
Full Historical Data Manager for FMCG Platform
Handles complete 471K sales + 2M inventory records efficiently
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from google.cloud import bigquery
import logging
import time

# Simple logger fallback
try:
    from src.utils.logger import default_logger
except ImportError:
    logging.basicConfig(level=logging.INFO)
    default_logger = logging.getLogger(__name__)


class FullHistoricalDataManager:
    """Manages complete historical datasets efficiently"""
    
    def __init__(self, project_id: str, dataset: str):
        self.project_id = project_id
        self.dataset = dataset
        self.client = bigquery.Client(project=project_id)
        self.logger = default_logger
        
        # Historical dataset configurations
        self.total_sales_rows = 471854
        self.total_inventory_rows = 2000000
        self.total_estimated_storage_gb = 26.3
        
        # Processing configurations for full dataset
        self.large_batch_size = 100000  # 100K rows per batch
        self.query_timeout_seconds = 600  # 10 minutes timeout
        self.max_retries = 3
        self.retry_delay_seconds = 30
    
    def get_full_dataset_info(self) -> Dict:
        """Get comprehensive information about the full historical dataset"""
        
        self.logger.info("Analyzing full historical dataset...")
        
        dataset_info = {
            'scale_summary': {
                'total_sales_rows': self.total_sales_rows,
                'total_inventory_rows': self.total_inventory_rows,
                'total_records': self.total_sales_rows + self.total_inventory_rows,
                'estimated_storage_gb': self.total_estimated_storage_gb
            },
            'table_details': {},
            'date_ranges': {},
            'data_quality': {}
        }
        
        try:
            # Analyze fact_sales full dataset
            sales_analysis = self._analyze_full_table('fact_sales')
            dataset_info['table_details']['fact_sales'] = sales_analysis
            
            # Analyze fact_inventory full dataset
            inventory_analysis = self._analyze_full_table('fact_inventory')
            dataset_info['table_details']['fact_inventory'] = inventory_analysis
            
            # Calculate date ranges
            dataset_info['date_ranges'] = {
                'sales_span': self._calculate_date_span('fact_sales'),
                'inventory_span': self._calculate_date_span('fact_inventory'),
                'overall_span': self._get_overall_date_range()
            }
            
            # Data quality metrics
            dataset_info['data_quality'] = self._assess_data_quality()
            
        except Exception as e:
            self.logger.error(f"Failed to analyze full dataset: {str(e)}")
            dataset_info['error'] = str(e)
        
        return dataset_info
    
    def _analyze_full_table(self, table_name: str) -> Dict:
        """Analyze a complete table with optimized queries"""
        
        self.logger.info(f"Analyzing full {table_name} table...")
        
        analysis = {}
        
        try:
            # Basic row count and date range
            basic_query = f"""
            SELECT 
                COUNT(*) as total_rows,
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                COUNT(DISTINCT date) as unique_dates
            FROM `{self.project_id}.{self.dataset}.{table_name}`
            """
            
            basic_result = self._execute_with_retry(basic_query)
            if basic_result:
                analysis['basic_stats'] = basic_result.iloc[0].to_dict()
            
            # Cardinality analysis (sample-based for performance)
            if table_name == 'fact_sales':
                cardinality_query = f"""
                SELECT 
                    COUNT(DISTINCT product_id) as unique_products,
                    COUNT(DISTINCT retailer_id) as unique_retailers,
                    COUNT(DISTINCT employee_id) as unique_employees,
                    AVG(quantity) as avg_quantity,
                    AVG(total_amount) as avg_amount,
                    SUM(total_amount) as total_revenue
                FROM `{self.project_id}.{self.dataset}.{table_name}`
                """
            else:  # fact_inventory
                cardinality_query = f"""
                SELECT 
                    COUNT(DISTINCT product_id) as unique_products,
                    COUNT(DISTINCT location_id) as unique_locations,
                    AVG(opening_stock) as avg_opening_stock,
                    AVG(closing_stock) as avg_closing_stock,
                    SUM(total_value) as total_inventory_value
                FROM `{self.project_id}.{self.dataset}.{table_name}`
                """
            
            cardinality_result = self._execute_with_retry(cardinality_query)
            if cardinality_result:
                analysis['cardinality'] = cardinality_result.iloc[0].to_dict()
            
            # Time series analysis (monthly aggregates)
            monthly_query = f"""
            SELECT 
                DATE_TRUNC(date, MONTH) as month,
                COUNT(*) as monthly_records,
                { 
                    'SUM(quantity) as monthly_quantity, SUM(total_amount) as monthly_revenue' 
                    if table_name == 'fact_sales' 
                    else 'SUM(stock_sold) as monthly_sold, SUM(total_value) as monthly_value'
                }
            FROM `{self.project_id}.{self.dataset}.{table_name}`
            GROUP BY DATE_TRUNC(date, MONTH)
            ORDER BY month
            """
            
            monthly_result = self._execute_with_retry(monthly_query)
            if monthly_result is not None and len(monthly_result) > 0:
                analysis['monthly_trends'] = monthly_result.to_dict('records')
            
        except Exception as e:
            self.logger.error(f"Error analyzing {table_name}: {str(e)}")
            analysis['error'] = str(e)
        
        return analysis
    
    def _execute_with_retry(self, query: str) -> Optional[pd.DataFrame]:
        """Execute query with retry logic for large datasets"""
        
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Executing query (attempt {attempt + 1}/{self.max_retries})")
                
                job_config = bigquery.QueryJobConfig(
                    timeout=timedelta(seconds=self.query_timeout_seconds),
                    use_legacy_sql=False
                )
                
                query_job = self.client.query(query, job_config=job_config)
                result = query_job.result(timeout=self.query_timeout_seconds)
                
                return result.to_dataframe()
                
            except Exception as e:
                self.logger.warning(f"Query attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    self.logger.info(f"Retrying in {self.retry_delay_seconds} seconds...")
                    time.sleep(self.retry_delay_seconds)
                else:
                    self.logger.error(f"All retry attempts failed for query")
                    raise
        
        return None
    
    def load_full_historical_data(self, table_name: str, 
                                columns: List[str] = None,
                                date_filter: str = None) -> pd.DataFrame:
        """Load complete historical data with optimized batching"""
        
        self.logger.info(f"Loading full historical data from {table_name}...")
        
        if columns is None:
            # Select key columns for performance
            if table_name == 'fact_sales':
                columns = ['sale_id', 'date', 'product_id', 'retailer_id', 
                          'quantity', 'unit_price', 'total_amount', 'delivery_status']
            else:  # fact_inventory
                columns = ['inventory_id', 'date', 'product_id', 'location_id',
                          'opening_stock', 'closing_stock', 'stock_received', 'stock_sold']
        
        # Build base query
        column_list = ', '.join(columns)
        base_query = f"""
        SELECT {column_list}
        FROM `{self.project_id}.{self.dataset}.{table_name}`
        """
        
        # Add date filter if provided
        if date_filter:
            base_query += f" WHERE {date_filter}"
        
        base_query += f" ORDER BY date"
        
        # Execute with batching for large datasets
        return self._load_in_batches(base_query, table_name)
    
    def _load_in_batches(self, query: str, table_name: str) -> pd.DataFrame:
        """Load data in batches to handle large datasets"""
        
        all_data = []
        offset = 0
        batch_num = 1
        
        self.logger.info(f"Loading {table_name} in batches of {self.large_batch_size} rows...")
        
        while True:
            batch_query = f"{query} LIMIT {self.large_batch_size} OFFSET {offset}"
            
            try:
                batch_data = self._execute_with_retry(batch_query)
                
                if batch_data is None or len(batch_data) == 0:
                    break
                
                all_data.append(batch_data)
                self.logger.info(f"Loaded batch {batch_num}: {len(batch_data)} rows")
                
                offset += self.large_batch_size
                batch_num += 1
                
                # Safety check to prevent infinite loops
                if batch_num > 50:  # Max 50 batches (5M rows)
                    self.logger.warning("Reached maximum batch limit, stopping...")
                    break
                
            except Exception as e:
                self.logger.error(f"Failed to load batch {batch_num}: {str(e)}")
                break
        
        if all_data:
            full_data = pd.concat(all_data, ignore_index=True)
            self.logger.info(f"Successfully loaded {len(full_data)} total rows from {table_name}")
            return full_data
        else:
            self.logger.warning(f"No data loaded from {table_name}")
            return pd.DataFrame()
    
    def create_historical_aggregated_views(self) -> bool:
        """Create comprehensive aggregated views for historical analysis"""
        
        self.logger.info("Creating historical aggregated views...")
        
        try:
            # Daily historical sales view
            daily_sales_view = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset}.historical_daily_sales`
            AS
            SELECT 
                date,
                product_id,
                retailer_id,
                SUM(quantity) as daily_quantity,
                AVG(unit_price) as avg_unit_price,
                SUM(total_amount) as daily_revenue,
                COUNT(*) as daily_transactions,
                SUM(discount_amount) as daily_discount
            FROM `{self.project_id}.{self.dataset}.fact_sales`
            GROUP BY date, product_id, retailer_id
            """
            
            self._execute_with_retry(daily_sales_view)
            self.logger.info("Created historical_daily_sales view")
            
            # Weekly historical sales view
            weekly_sales_view = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset}.historical_weekly_sales`
            AS
            SELECT 
                DATE_TRUNC(date, WEEK) as week,
                product_id,
                retailer_id,
                SUM(quantity) as weekly_quantity,
                AVG(unit_price) as avg_unit_price,
                SUM(total_amount) as weekly_revenue,
                COUNT(DISTINCT date) as active_days,
                COUNT(*) as weekly_transactions
            FROM `{self.project_id}.{self.dataset}.fact_sales`
            GROUP BY DATE_TRUNC(date, WEEK), product_id, retailer_id
            """
            
            self._execute_with_retry(weekly_sales_view)
            self.logger.info("Created historical_weekly_sales view")
            
            # Monthly historical sales view
            monthly_sales_view = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset}.historical_monthly_sales`
            AS
            SELECT 
                DATE_TRUNC(date, MONTH) as month,
                product_id,
                retailer_id,
                SUM(quantity) as monthly_quantity,
                AVG(unit_price) as avg_unit_price,
                SUM(total_amount) as monthly_revenue,
                COUNT(DISTINCT date) as active_days,
                COUNT(*) as monthly_transactions
            FROM `{self.project_id}.{self.dataset}.fact_sales`
            GROUP BY DATE_TRUNC(date, MONTH), product_id, retailer_id
            """
            
            self._execute_with_retry(monthly_sales_view)
            self.logger.info("Created historical_monthly_sales view")
            
            # Daily historical inventory view
            daily_inventory_view = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset}.historical_daily_inventory`
            AS
            SELECT 
                date,
                product_id,
                location_id,
                SUM(opening_stock) as daily_opening_stock,
                SUM(closing_stock) as daily_closing_stock,
                SUM(stock_received) as daily_received,
                SUM(stock_sold) as daily_sold,
                SUM(COALESCE(stock_lost, 0)) as daily_lost,
                AVG(unit_cost) as avg_unit_cost,
                SUM(total_value) as daily_value
            FROM `{self.project_id}.{self.dataset}.fact_inventory`
            GROUP BY date, product_id, location_id
            """
            
            self._execute_with_retry(daily_inventory_view)
            self.logger.info("Created historical_daily_inventory view")
            
            # Monthly historical inventory view
            monthly_inventory_view = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset}.historical_monthly_inventory`
            AS
            SELECT 
                DATE_TRUNC(date, MONTH) as month,
                product_id,
                location_id,
                SUM(opening_stock) as monthly_opening_stock,
                SUM(closing_stock) as monthly_closing_stock,
                SUM(stock_received) as monthly_received,
                SUM(stock_sold) as monthly_sold,
                SUM(COALESCE(stock_lost, 0)) as monthly_lost,
                AVG(unit_cost) as avg_unit_cost,
                SUM(total_value) as monthly_value
            FROM `{self.project_id}.{self.dataset}.fact_inventory`
            GROUP BY DATE_TRUNC(date, MONTH), product_id, location_id
            """
            
            self._execute_with_retry(monthly_inventory_view)
            self.logger.info("Created historical_monthly_inventory view")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create historical views: {str(e)}")
            return False
    
    def run_historical_synchronization(self) -> Dict:
        """Run synchronization on complete historical dataset"""
        
        self.logger.info("Running historical synchronization on full dataset...")
        
        try:
            # Load full historical data in batches
            sales_data = self.load_full_historical_data('fact_sales')
            inventory_data = self.load_full_historical_data('fact_inventory')
            
            if len(sales_data) == 0 or len(inventory_data) == 0:
                return {
                    'status': 'FAILED',
                    'reason': 'No historical data loaded',
                    'sales_rows': len(sales_data),
                    'inventory_rows': len(inventory_data)
                }
            
            # Load product data
            product_query = """
            SELECT product_id, sku, product_name, category_id, brand_id
            FROM `dim_products`
            """
            
            product_data = self._execute_with_retry(product_query)
            
            # Perform synchronization analysis
            sync_results = self._analyze_historical_synchronization(
                sales_data, inventory_data, product_data
            )
            
            return {
                'status': 'SUCCESS',
                'sync_analysis': sync_results,
                'dataset_info': {
                    'sales_rows': len(sales_data),
                    'inventory_rows': len(inventory_data),
                    'product_rows': len(product_data) if product_data is not None else 0
                }
            }
            
        except Exception as e:
            self.logger.error(f"Historical synchronization failed: {str(e)}")
            return {
                'status': 'FAILED',
                'reason': str(e)
            }
    
    def _analyze_historical_synchronization(self, sales_data: pd.DataFrame, 
                                         inventory_data: pd.DataFrame,
                                         product_data: pd.DataFrame) -> Dict:
        """Analyze synchronization for historical datasets"""
        
        self.logger.info("Analyzing historical synchronization...")
        
        # Aggregate sales by product and date
        sales_agg = sales_data.groupby(['product_id', 'date']).agg({
            'quantity': 'sum',
            'total_amount': 'sum'
        }).reset_index()
        
        # Aggregate inventory by product and date
        inventory_agg = inventory_data.groupby(['product_id', 'date']).agg({
            'stock_sold': 'sum',
            'opening_stock': 'sum',
            'closing_stock': 'sum'
        }).reset_index()
        
        # Merge for comparison
        sync_comparison = sales_agg.merge(
            inventory_agg,
            on=['product_id', 'date'],
            how='outer',
            suffixes=('_sales', '_inventory')
        )
        
        # Fill missing values
        sync_comparison['quantity'] = sync_comparison['quantity'].fillna(0)
        sync_comparison['stock_sold'] = sync_comparison['stock_sold'].fillna(0)
        
        # Calculate variance
        sync_comparison['variance'] = abs(sync_comparison['quantity'] - sync_comparison['stock_sold'])
        sync_comparison['variance_percentage'] = np.where(
            sync_comparison['stock_sold'] > 0,
            (sync_comparison['variance'] / sync_comparison['stock_sold']) * 100,
            np.where(sync_comparison['quantity'] > 0, 100.0, 0.0)
        )
        
        # Add product information
        if product_data is not None:
            sync_comparison = sync_comparison.merge(product_data, on='product_id', how='left')
        
        # Classify variances
        sync_comparison['variance_level'] = np.where(
            sync_comparison['variance_percentage'] >= 15,
            'CRITICAL',
            np.where(
                sync_comparison['variance_percentage'] >= 5,
                'WARNING',
                'ACCEPTABLE'
            )
        )
        
        # Generate summary statistics
        summary = {
            'total_comparisons': len(sync_comparison),
            'critical_variances': len(sync_comparison[sync_comparison['variance_level'] == 'CRITICAL']),
            'warning_variances': len(sync_comparison[sync_comparison['variance_level'] == 'WARNING']),
            'acceptable_variances': len(sync_comparison[sync_comparison['variance_level'] == 'ACCEPTABLE']),
            'average_variance_percentage': sync_comparison['variance_percentage'].mean(),
            'max_variance_percentage': sync_comparison['variance_percentage'].max(),
            'total_sales_quantity': sync_comparison['quantity'].sum(),
            'total_inventory_sold': sync_comparison['stock_sold'].sum(),
            'overall_variance_amount': sync_comparison['variance'].sum()
        }
        
        return {
            'summary': summary,
            'detailed_comparison': sync_comparison,
            'top_variances': sync_comparison.nlargest(20, 'variance_percentage').to_dict('records')
        }
    
    def _calculate_date_span(self, table_name: str) -> Dict:
        """Calculate date span for a table"""
        
        try:
            query = f"""
            SELECT 
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                DATEDIFF(MAX(date), MIN(date)) as day_span
            FROM `{self.project_id}.{self.dataset}.{table_name}`
            """
            
            result = self._execute_with_retry(query)
            if result is not None and len(result) > 0:
                return result.iloc[0].to_dict()
        except Exception as e:
            self.logger.error(f"Failed to calculate date span for {table_name}: {str(e)}")
        
        return {}
    
    def _get_overall_date_range(self) -> Dict:
        """Get overall date range across all tables"""
        
        try:
            query = f"""
            SELECT 
                MIN(date) as overall_earliest,
                MAX(date) as overall_latest,
                DATEDIFF(MAX(date), MIN(date)) as overall_span_days
            FROM (
                SELECT date FROM `{self.project_id}.{self.dataset}.fact_sales`
                UNION ALL
                SELECT date FROM `{self.project_id}.{self.dataset}.fact_inventory`
            ) combined_dates
            """
            
            result = self._execute_with_retry(query)
            if result is not None and len(result) > 0:
                return result.iloc[0].to_dict()
        except Exception as e:
            self.logger.error(f"Failed to get overall date range: {str(e)}")
        
        return {}
    
    def _assess_data_quality(self) -> Dict:
        """Assess data quality metrics"""
        
        quality_metrics = {
            'completeness': {},
            'consistency': {},
            'accuracy': {}
        }
        
        try:
            # Check for null values in key fields
            null_checks = [
                ("fact_sales", "sale_id", "Sales ID"),
                ("fact_sales", "date", "Sales Date"),
                ("fact_sales", "quantity", "Sales Quantity"),
                ("fact_inventory", "inventory_id", "Inventory ID"),
                ("fact_inventory", "date", "Inventory Date"),
                ("fact_inventory", "stock_sold", "Stock Sold")
            ]
            
            for table, column, description in null_checks:
                null_query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT({column}) as non_null_rows,
                    (COUNT(*) - COUNT({column})) as null_rows,
                    (COUNT(*) - COUNT({column})) * 100.0 / COUNT(*) as null_percentage
                FROM `{self.project_id}.{self.dataset}.{table}`
                """
                
                result = self._execute_with_retry(null_query)
                if result is not None and len(result) > 0:
                    quality_metrics['completeness'][description] = result.iloc[0].to_dict()
            
        except Exception as e:
            self.logger.error(f"Failed to assess data quality: {str(e)}")
            quality_metrics['error'] = str(e)
        
        return quality_metrics


def run_full_historical_analysis(project_id: str, dataset: str) -> Dict:
    """Run complete historical dataset analysis"""
    
    manager = FullHistoricalDataManager(project_id, dataset)
    
    print("\n" + "="*80)
    print("📊 FULL HISTORICAL DATA ANALYSIS")
    print("="*80)
    print(f"🗄️  Dataset Scale: {manager.total_sales_rows:,} sales, {manager.total_inventory_rows:,} inventory")
    print(f"💾 Estimated Storage: {manager.total_estimated_storage_gb:.1f} GB")
    
    # Get dataset information
    dataset_info = manager.get_full_dataset_info()
    
    print(f"\n📋 DATASET OVERVIEW:")
    scale = dataset_info['scale_summary']
    print(f"   Total Records: {scale['total_records']:,}")
    print(f"   Sales Records: {scale['total_sales_rows']:,}")
    print(f"   Inventory Records: {scale['total_inventory_rows']:,}")
    
    if 'date_ranges' in dataset_info and dataset_info['date_ranges']:
        ranges = dataset_info['date_ranges']
        if 'overall_span' in ranges and ranges['overall_span']:
            span = ranges['overall_span']
            print(f"   Date Range: {span.get('overall_earliest', 'N/A')} to {span.get('overall_latest', 'N/A')}")
            print(f"   Data Span: {span.get('overall_span_days', 'N/A')} days")
    
    print(f"\n📈 TABLE DETAILS:")
    for table_name, details in dataset_info.get('table_details', {}).items():
        print(f"\n🔹 {table_name.upper()}:")
        
        if 'basic_stats' in details:
            stats = details['basic_stats']
            print(f"   Total Rows: {stats.get('total_rows', 'N/A'):,}")
            print(f"   Date Range: {stats.get('earliest_date', 'N/A')} to {stats.get('latest_date', 'N/A')}")
            print(f"   Unique Dates: {stats.get('unique_dates', 'N/A')}")
        
        if 'cardinality' in details:
            cardinality = details['cardinality']
            if 'unique_products' in cardinality:
                print(f"   Unique Products: {cardinality['unique_products']:,}")
            if 'unique_retailers' in cardinality:
                print(f"   Unique Retailers: {cardinality['unique_retailers']:,}")
            if 'unique_locations' in cardinality:
                print(f"   Unique Locations: {cardinality['unique_locations']:,}")
    
    return dataset_info
