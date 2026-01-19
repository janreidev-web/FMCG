"""
Large-Scale Data Management for FMCG Platform
Optimized for 471K sales records and 2M inventory records
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


class LargeScaleDataManager:
    """Manages large datasets (471K sales, 2M inventory) efficiently"""
    
    def __init__(self, project_id: str, dataset: str):
        self.project_id = project_id
        self.dataset = dataset
        self.client = bigquery.Client(project=project_id)
        self.logger = default_logger
        
        # Large dataset configurations
        self.sales_rows = 471854
        self.inventory_rows = 2000000
        self.estimated_storage_gb = 15.2  # Based on your current usage
        
        # Processing configurations
        self.batch_size = 50000  # Process 50K rows at a time
        self.archive_threshold_days = 180  # Archive 6+ months old data
        self.recent_days_limit = 90  # Keep last 90 days in main tables
    
    def analyze_large_dataset_storage(self) -> Dict:
        """Analyze storage for large datasets"""
        
        self.logger.info("Analyzing large dataset storage...")
        
        # Get detailed table information
        tables_info = {}
        
        # Analyze fact_sales (471K rows)
        sales_query = """
        SELECT 
            COUNT(*) as total_rows,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            COUNT(DISTINCT product_id) as unique_products,
            COUNT(DISTINCT retailer_id) as unique_retailers,
            COUNT(DISTINCT DATE(date)) as unique_dates
        FROM `fact_sales`
        """
        
        try:
            sales_result = self.client.query(sales_query).to_dataframe()
            tables_info['fact_sales'] = {
                'rows': int(sales_result['total_rows'].iloc[0]),
                'earliest_date': sales_result['earliest_date'].iloc[0],
                'latest_date': sales_result['latest_date'].iloc[0],
                'unique_products': int(sales_result['unique_products'].iloc[0]),
                'unique_retailers': int(sales_result['unique_retailers'].iloc[0]),
                'date_range_days': int(sales_result['unique_dates'].iloc[0])
            }
        except Exception as e:
            self.logger.warning(f"Could not analyze fact_sales: {str(e)}")
            tables_info['fact_sales'] = {'rows': self.sales_rows, 'status': 'estimated'}
        
        # Analyze fact_inventory (2M rows)
        inventory_query = """
        SELECT 
            COUNT(*) as total_rows,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            COUNT(DISTINCT product_id) as unique_products,
            COUNT(DISTINCT location_id) as unique_locations,
            COUNT(DISTINCT DATE(date)) as unique_dates
        FROM `fact_inventory`
        """
        
        try:
            inventory_result = self.client.query(inventory_query).to_dataframe()
            tables_info['fact_inventory'] = {
                'rows': int(inventory_result['total_rows'].iloc[0]),
                'earliest_date': inventory_result['earliest_date'].iloc[0],
                'latest_date': inventory_result['latest_date'].iloc[0],
                'unique_products': int(inventory_result['unique_products'].iloc[0]),
                'unique_locations': int(inventory_result['unique_locations'].iloc[0]),
                'date_range_days': int(inventory_result['unique_dates'].iloc[0])
            }
        except Exception as e:
            self.logger.warning(f"Could not analyze fact_inventory: {str(e)}")
            tables_info['fact_inventory'] = {'rows': self.inventory_rows, 'status': 'estimated'}
        
        return tables_info
    
    def create_large_scale_archiving_strategy(self) -> Dict:
        """Create archiving strategy for large datasets"""
        
        self.logger.info("Creating large-scale archiving strategy...")
        
        cutoff_date = datetime.now() - timedelta(days=self.archive_threshold_days)
        recent_cutoff = datetime.now() - timedelta(days=self.recent_days_limit)
        
        strategy = {
            'immediate_actions': [],
            'archiving_plan': {},
            'storage_savings': {},
            'retention_policy': {}
        }
        
        # Analyze data distribution for archiving
        tables_info = self.analyze_large_dataset_storage()
        
        # Strategy for fact_sales (471K rows)
        if 'fact_sales' in tables_info:
            sales_info = tables_info['fact_sales']
            
            # Count old records
            old_sales_query = f"""
            SELECT COUNT(*) as old_rows
            FROM `fact_sales`
            WHERE date < '{cutoff_date.date()}'
            """
            
            try:
                old_sales_result = self.client.query(old_sales_query).to_dataframe()
                old_sales_rows = int(old_sales_result['old_rows'].iloc[0])
                
                if old_sales_rows > 0:
                    strategy['archiving_plan']['fact_sales'] = {
                        'old_records': old_sales_rows,
                        'percentage': (old_sales_rows / sales_info['rows']) * 100,
                        'cutoff_date': cutoff_date.date(),
                        'estimated_savings_gb': old_sales_rows * 0.000032  # ~32KB per row
                    }
                    strategy['immediate_actions'].append(f"Archive {old_sales_rows:,} old sales records")
            except Exception as e:
                self.logger.warning(f"Could not calculate old sales records: {str(e)}")
        
        # Strategy for fact_inventory (2M rows)
        if 'fact_inventory' in tables_info:
            inventory_info = tables_info['fact_inventory']
            
            # Count old records
            old_inventory_query = f"""
            SELECT COUNT(*) as old_rows
            FROM `fact_inventory`
            WHERE date < '{cutoff_date.date()}'
            """
            
            try:
                old_inventory_result = self.client.query(old_inventory_query).to_dataframe()
                old_inventory_rows = int(old_inventory_result['old_rows'].iloc[0])
                
                if old_inventory_rows > 0:
                    strategy['archiving_plan']['fact_inventory'] = {
                        'old_records': old_inventory_rows,
                        'percentage': (old_inventory_rows / inventory_info['rows']) * 100,
                        'cutoff_date': cutoff_date.date(),
                        'estimated_savings_gb': old_inventory_rows * 0.000028  # ~28KB per row
                    }
                    strategy['immediate_actions'].append(f"Archive {old_inventory_rows:,} old inventory records")
            except Exception as e:
                self.logger.warning(f"Could not calculate old inventory records: {str(e)}")
        
        # Calculate total savings
        total_savings = sum(
            plan['estimated_savings_gb'] for plan in strategy['archiving_plan'].values()
        )
        strategy['storage_savings']['total_gb'] = total_savings
        strategy['storage_savings']['new_estimated_gb'] = self.estimated_storage_gb - total_savings
        
        # Retention policy
        strategy['retention_policy'] = {
            'active_data_days': self.recent_days_limit,
            'archive_threshold_days': self.archive_threshold_days,
            'permanent_archive_days': 1095,  # 3 years
            'recommended_cleanup_frequency': 'monthly'
        }
        
        return strategy
    
    def execute_large_scale_archiving(self, strategy: Dict, interactive: bool = True) -> bool:
        """Execute archiving for large datasets"""
        
        self.logger.info("Executing large-scale archiving...")
        
        success_count = 0
        total_archived = 0
        
        for table_name, plan in strategy['archiving_plan'].items():
            if interactive:
                print(f"\n📦 Archiving {table_name}:")
                print(f"   Records to archive: {plan['old_records']:,}")
                print(f"   Percentage of total: {plan['percentage']:.1f}%")
                print(f"   Estimated savings: {plan['estimated_savings_gb']:.2f} GB")
                
                response = input("Proceed with archiving? (y/n): ").lower().strip()
                if response != 'y':
                    print("Skipping...")
                    continue
            
            try:
                # Create archive table
                archive_table = f"{table_name}_archive"
                
                # Move data in batches to avoid timeouts
                cutoff_date = plan['cutoff_date']
                batch_query = f"""
                INSERT INTO `{self.project_id}.{self.dataset}.{archive_table}`
                SELECT *
                FROM `{self.project_id}.{self.dataset}.{table_name}`
                WHERE date < '{cutoff_date}'
                LIMIT {self.batch_size}
                """
                
                # Process in batches
                archived_count = 0
                while True:
                    try:
                        batch_job = self.client.query(batch_query)
                        batch_result = batch_job.result()
                        
                        if batch_result.num_dml_affected_rows == 0:
                            break
                        
                        archived_count += batch_result.num_dml_affected_rows
                        self.logger.info(f"Archived {archived_count:,} records from {table_name}")
                        
                        # Delete archived records
                        delete_query = f"""
                        DELETE FROM `{self.project_id}.{self.dataset}.{table_name}`
                        WHERE date < '{cutoff_date}'
                        LIMIT {self.batch_size}
                        """
                        
                        delete_job = self.client.query(delete_query)
                        delete_job.result()
                        
                    except Exception as e:
                        self.logger.error(f"Batch failed for {table_name}: {str(e)}")
                        break
                
                if archived_count > 0:
                    self.logger.info(f"Successfully archived {archived_count:,} records from {table_name}")
                    success_count += 1
                    total_archived += archived_count
                else:
                    self.logger.warning(f"No records archived from {table_name}")
                    
            except Exception as e:
                self.logger.error(f"Failed to archive {table_name}: {str(e)}")
        
        self.logger.info(f"Archiving completed: {success_count} tables, {total_archived:,} total records")
        return success_count > 0
    
    def create_large_scale_aggregated_views(self) -> bool:
        """Create optimized aggregated views for large datasets"""
        
        self.logger.info("Creating large-scale aggregated views...")
        
        try:
            # Daily aggregated sales (for recent data)
            daily_sales_view = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset}.daily_sales_aggregated`
            AS
            SELECT 
                date,
                product_id,
                retailer_id,
                SUM(quantity) as total_quantity,
                AVG(unit_price) as avg_unit_price,
                SUM(total_amount) as total_revenue,
                COUNT(*) as transaction_count,
                SUM(discount_amount) as total_discount
            FROM `{self.project_id}.{self.dataset}.fact_sales`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
            GROUP BY date, product_id, retailer_id
            """
            
            self.client.query(daily_sales_view).result()
            self.logger.info("Created daily_sales_aggregated view")
            
            # Weekly aggregated sales (for medium-term analysis)
            weekly_sales_view = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset}.weekly_sales_aggregated`
            AS
            SELECT 
                DATE_TRUNC(date, WEEK) as week,
                product_id,
                retailer_id,
                SUM(quantity) as total_quantity,
                AVG(unit_price) as avg_unit_price,
                SUM(total_amount) as total_revenue,
                COUNT(DISTINCT date) as active_days,
                COUNT(*) as transaction_count
            FROM `{self.project_id}.{self.dataset}.fact_sales`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
            GROUP BY DATE_TRUNC(date, WEEK), product_id, retailer_id
            """
            
            self.client.query(weekly_sales_view).result()
            self.logger.info("Created weekly_sales_aggregated view")
            
            # Monthly aggregated inventory
            monthly_inventory_view = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset}.monthly_inventory_aggregated`
            AS
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
                SUM(total_value) as total_inventory_value
            FROM `{self.project_id}.{self.dataset}.fact_inventory`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
            GROUP BY DATE_TRUNC(date, MONTH), product_id, location_id
            """
            
            self.client.query(monthly_inventory_view).result()
            self.logger.info("Created monthly_inventory_aggregated view")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create aggregated views: {str(e)}")
            return False
    
    def optimize_large_scale_queries(self) -> Dict:
        """Provide query optimization recommendations"""
        
        recommendations = {
            'query_optimizations': [],
            'index_suggestions': [],
            'partition_recommendations': [],
            'performance_tips': []
        }
        
        # Query optimizations for large datasets
        recommendations['query_optimizations'] = [
            "Use DATE filters in WHERE clauses to limit data range",
            "SELECT only necessary columns instead of SELECT *",
            "Use aggregated views for historical analysis",
            "Add LIMIT clauses to prevent accidental full table scans",
            "Use table aliases for better query readability"
        ]
        
        # Partition recommendations
        recommendations['partition_recommendations'] = [
            "Partition fact_sales by date for efficient time-based queries",
            "Cluster fact_sales by product_id for product-specific analysis",
            "Partition fact_inventory by date and cluster by location_id",
            "Consider materialized views for frequently accessed aggregations"
        ]
        
        # Performance tips
        recommendations['performance_tips'] = [
            f"Process data in batches of {self.batch_size:,} rows",
            "Use cached results for repeated queries",
            "Schedule heavy queries during off-peak hours",
            "Monitor query execution times and costs",
            "Use query parameters instead of string concatenation"
        ]
        
        return recommendations


def run_large_scale_optimization(project_id: str, dataset: str) -> Dict:
    """Run complete large-scale optimization"""
    
    manager = LargeScaleDataManager(project_id, dataset)
    
    print("\n" + "="*80)
    print("🗄️  LARGE-SCALE DATA OPTIMIZATION")
    print("="*80)
    print(f"📊 Dataset Scale: {manager.sales_rows:,} sales, {manager.inventory_rows:,} inventory")
    print(f"💾 Estimated Storage: {manager.estimated_storage_gb:.1f} GB")
    
    # Analyze current state
    tables_info = manager.analyze_large_dataset_storage()
    
    print(f"\n📋 Current Data Analysis:")
    for table, info in tables_info.items():
        print(f"   {table}:")
        print(f"     Rows: {info.get('rows', 'N/A'):,}")
        if 'earliest_date' in info:
            print(f"     Date Range: {info['earliest_date']} to {info['latest_date']}")
        if 'unique_products' in info:
            print(f"     Unique Products: {info['unique_products']:,}")
    
    # Create archiving strategy
    strategy = manager.create_large_scale_archiving_strategy()
    
    print(f"\n🎯 Archiving Strategy:")
    if strategy['immediate_actions']:
        for action in strategy['immediate_actions']:
            print(f"   • {action}")
    else:
        print("   No immediate archiving needed")
    
    print(f"\n💰 Storage Savings Potential: {strategy['storage_savings']['total_gb']:.2f} GB")
    print(f"   After archiving: {strategy['storage_savings']['new_estimated_gb']:.1f} GB")
    
    # Show optimization recommendations
    recommendations = manager.optimize_large_scale_queries()
    
    print(f"\n💡 Optimization Recommendations:")
    for category, tips in recommendations.items():
        print(f"   {category.replace('_', ' ').title()}:")
        for tip in tips[:3]:  # Show top 3 tips
            print(f"     • {tip}")
    
    return {
        'tables_info': tables_info,
        'archiving_strategy': strategy,
        'optimization_recommendations': recommendations
    }
