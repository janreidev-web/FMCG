"""
Inventory-Sales Synchronization Module for FMCG Data Analytics Platform
Ensures consistency between fact_sales and fact_inventory tables
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

# Simple logger fallback for testing
try:
    from src.utils.logger import default_logger
    from src.utils.bigquery_client import BigQueryManager
except ImportError:
    import logging
    default_logger = logging.getLogger(__name__)
    
    class MockBigQueryManager:
        def execute_query(self, query):
            return pd.DataFrame()
    
    BigQueryManager = MockBigQueryManager


class InventorySalesSynchronizer:
    """
    Synchronizes fact_sales and fact_inventory tables to ensure
    consistency between stock movements and sales quantities
    """
    
    def __init__(self, bigquery_client: BigQueryManager):
        self.bq_client = bigquery_client
        self.logger = default_logger
        
        # Synchronization thresholds
        self.max_acceptable_variance = 0.05  # 5% variance tolerance
        self.critical_variance = 0.15       # 15% critical variance
        
        # Cache for data during synchronization
        self.sales_data = None
        self.inventory_data = None
        self.product_data = None
    
    def load_data(self, start_date: str = None, end_date: str = None, 
                  storage_aware: bool = False, max_days: int = 365, 
                  batch_size: int = 100000, historical_mode: bool = False) -> None:
        """Load sales and inventory data for synchronization analysis"""
        
        # Historical mode for full dataset (471K sales, 2M inventory)
        if historical_mode and not start_date:
            # For historical analysis, load complete dataset
            self.logger.info(f"Loading complete historical dataset (471K sales, 2M inventory)")
            # Don't limit date range for historical mode
            if not end_date:
                end_date = datetime.now().date()
            if not start_date:
                # Go back to 2015 based on ETL pipeline start date
                start_date = datetime(2015, 1, 1).date()
        elif storage_aware and not start_date:
            # Storage-aware mode for limited processing
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=max_days)
            self.logger.info(f"Using storage-aware date range: {start_date} to {end_date}")
        elif not end_date:
            end_date = datetime.now().date()
        elif not start_date:
            start_date = end_date - timedelta(days=90)  # Default 90 days
        
        mode_desc = "historical" if historical_mode else "date range"
        self.logger.info(f"Loading {mode_desc} data from {start_date} to {end_date}")
        
        try:
            # Load sales data with appropriate batch size
            sales_query = f"""
            SELECT 
                sale_id,
                date as sale_date,
                product_id,
                retailer_id,
                quantity as sales_quantity,
                unit_price,
                total_amount,
                delivery_status
            FROM `fact_sales`
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND delivery_status != 'Cancelled'
            ORDER BY date, product_id, retailer_id
            """
            
            # Add LIMIT only for non-historical mode
            if not historical_mode:
                sales_query += f" LIMIT {batch_size}"
            
            self.sales_data = self.bq_client.execute_query(sales_query)
            mode_info = f" ({'historical' if historical_mode else f'batch size: {batch_size}'})"
            self.logger.info(f"Loaded {len(self.sales_data)} sales records{mode_info}")
            
            # Load inventory data with appropriate batch size
            inventory_query = f"""
            SELECT 
                inventory_id,
                date as inventory_date,
                product_id,
                location_id,
                opening_stock,
                closing_stock,
                stock_received,
                stock_sold,
                stock_lost,
                unit_cost,
                total_value
            FROM `fact_inventory`
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY date, product_id, location_id
            """
            
            # Add LIMIT only for non-historical mode
            if not historical_mode:
                inventory_query += f" LIMIT {batch_size}"
            
            self.inventory_data = self.bq_client.execute_query(inventory_query)
            self.logger.info(f"Loaded {len(self.inventory_data)} inventory records{mode_info}")
            
            # Load product data
            product_query = """
            SELECT product_id, sku, product_name, category_id, brand_id
            FROM `dim_products`
            LIMIT 10000  # Reasonable limit for product catalog
            """
            
            self.product_data = self.bq_client.execute_query(product_query)
            self.logger.info(f"Loaded {len(self.product_data)} product records")
            
            # Log dataset statistics
            if len(self.sales_data) > 0:
                self.logger.info(f"Sales data range: {self.sales_data['sale_date'].min()} to {self.sales_data['sale_date'].max()}")
                if historical_mode:
                    self.logger.info(f"Historical sales dataset: {len(self.sales_data):,} records loaded")
            if len(self.inventory_data) > 0:
                self.logger.info(f"Inventory data range: {self.inventory_data['inventory_date'].min()} to {self.inventory_data['inventory_date'].max()}")
                if historical_mode:
                    self.logger.info(f"Historical inventory dataset: {len(self.inventory_data):,} records loaded")
            
        except Exception as e:
            error_desc = "historical" if historical_mode else "dataset"
            self.logger.error(f"Failed to load {error_desc} data: {str(e)}")
            # Fallback to empty dataframes
            self.sales_data = pd.DataFrame()
            self.inventory_data = pd.DataFrame()
            self.product_data = pd.DataFrame()
            raise
    
    def analyze_synchronization_gaps(self) -> pd.DataFrame:
        """Analyze gaps between sales quantities and inventory stock movements"""
        
        if self.sales_data is None or self.inventory_data is None:
            raise ValueError("Data not loaded. Call load_data() first.")
        
        self.logger.info("Analyzing synchronization gaps...")
        
        # Aggregate sales by product and date
        sales_agg = self.sales_data.groupby(['product_id', 'sale_date']).agg({
            'sales_quantity': 'sum',
            'sale_id': 'count'
        }).reset_index()
        sales_agg.rename(columns={'sale_id': 'transaction_count'}, inplace=True)
        
        # Aggregate inventory by product and date
        inventory_agg = self.inventory_data.groupby(['product_id', 'inventory_date']).agg({
            'stock_sold': 'sum',
            'opening_stock': 'sum',
            'closing_stock': 'sum',
            'stock_received': 'sum',
            'stock_lost': 'sum'
        }).reset_index()
        
        # Merge sales and inventory data
        sync_analysis = sales_agg.merge(
            inventory_agg,
            left_on=['product_id', 'sale_date'],
            right_on=['product_id', 'inventory_date'],
            how='outer'
        )
        
        # Fill missing values
        sync_analysis['sales_quantity'] = sync_analysis['sales_quantity'].fillna(0)
        sync_analysis['stock_sold'] = sync_analysis['stock_sold'].fillna(0)
        sync_analysis['transaction_count'] = sync_analysis['transaction_count'].fillna(0)
        
        # Calculate variance metrics
        sync_analysis['quantity_variance'] = abs(sync_analysis['sales_quantity'] - sync_analysis['stock_sold'])
        sync_analysis['variance_percentage'] = np.where(
            sync_analysis['stock_sold'] > 0,
            (sync_analysis['quantity_variance'] / sync_analysis['stock_sold']) * 100,
            np.where(
                sync_analysis['sales_quantity'] > 0,
                100.0,  # If no inventory movement but sales exist
                0.0
            )
        )
        
        # Add product information
        sync_analysis = sync_analysis.merge(self.product_data, on='product_id', how='left')
        
        # Classify variance levels
        sync_analysis['variance_level'] = np.where(
            sync_analysis['variance_percentage'] >= self.critical_variance * 100,
            'CRITICAL',
            np.where(
                sync_analysis['variance_percentage'] >= self.max_acceptable_variance * 100,
                'WARNING',
                'ACCEPTABLE'
            )
        )
        
        # Sort by variance percentage (highest first)
        sync_analysis = sync_analysis.sort_values('variance_percentage', ascending=False)
        
        return sync_analysis
    
    def generate_synchronization_report(self, sync_analysis: pd.DataFrame) -> Dict:
        """Generate comprehensive synchronization report"""
        
        report = {
            'summary': {},
            'critical_issues': [],
            'recommendations': [],
            'statistics': {}
        }
        
        # Summary statistics
        total_records = len(sync_analysis)
        critical_count = len(sync_analysis[sync_analysis['variance_level'] == 'CRITICAL'])
        warning_count = len(sync_analysis[sync_analysis['variance_level'] == 'WARNING'])
        acceptable_count = len(sync_analysis[sync_analysis['variance_level'] == 'ACCEPTABLE'])
        
        report['summary'] = {
            'total_records_analyzed': total_records,
            'critical_variance_records': critical_count,
            'warning_variance_records': warning_count,
            'acceptable_variance_records': acceptable_count,
            'critical_percentage': (critical_count / total_records * 100) if total_records > 0 else 0,
            'average_variance_percentage': sync_analysis['variance_percentage'].mean(),
            'maximum_variance_percentage': sync_analysis['variance_percentage'].max()
        }
        
        # Critical issues (top 10)
        critical_issues = sync_analysis[sync_analysis['variance_level'] == 'CRITICAL'].head(10)
        for _, issue in critical_issues.iterrows():
            report['critical_issues'].append({
                'product_id': issue['product_id'],
                'sku': issue.get('sku', 'N/A'),
                'date': issue['sale_date'],
                'sales_quantity': int(issue['sales_quantity']),
                'inventory_stock_sold': int(issue['stock_sold']),
                'variance_percentage': round(issue['variance_percentage'], 2),
                'variance_amount': int(issue['quantity_variance'])
            })
        
        # Recommendations based on analysis
        if critical_count > 0:
            report['recommendations'].append("URGENT: Review critical variance records immediately")
        
        if warning_count > total_records * 0.1:  # More than 10% warnings
            report['recommendations'].append("Implement automated reconciliation processes")
        
        if sync_analysis['variance_percentage'].mean() > 10:
            report['recommendations'].append("Review inventory counting procedures and sales recording accuracy")
        
        # Additional statistics
        report['statistics'] = {
            'total_sales_quantity': int(sync_analysis['sales_quantity'].sum()),
            'total_inventory_stock_sold': int(sync_analysis['stock_sold'].sum()),
            'overall_variance_amount': int(sync_analysis['quantity_variance'].sum()),
            'affected_skus': sync_analysis['product_id'].nunique(),
            'affected_dates': sync_analysis['sale_date'].nunique()
        }
        
        return report
    
    def create_synchronization_adjustments(self, sync_analysis: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Create adjustment records to synchronize sales and inventory data
        Returns tuple of (inventory_adjustments, sales_adjustments)
        """
        
        self.logger.info("Creating synchronization adjustments...")
        
        # Focus on records with significant variance
        significant_variance = sync_analysis[
            (sync_analysis['variance_level'] == 'CRITICAL') |
            (sync_analysis['variance_level'] == 'WARNING')
        ].copy()
        
        inventory_adjustments = []
        sales_adjustments = []
        
        for _, record in significant_variance.iterrows():
            product_id = record['product_id']
            date = record['sale_date']
            sales_qty = record['sales_quantity']
            inventory_qty = record['stock_sold']
            variance = record['quantity_variance']
            
            # Determine adjustment direction
            if sales_qty > inventory_qty:
                # Inventory understated - need to increase stock_sold
                adjustment_amount = sales_qty - inventory_qty
                inventory_adjustments.append({
                    'adjustment_id': f"INV_ADJ_{product_id}_{date.strftime('%Y%m%d')}",
                    'product_id': product_id,
                    'date': date,
                    'adjustment_type': 'STOCK_SOLD_INCREASE',
                    'adjustment_quantity': adjustment_amount,
                    'reason': 'Sales-Inventory Synchronization',
                    'original_stock_sold': inventory_qty,
                    'adjusted_stock_sold': sales_qty,
                    'variance_percentage': record['variance_percentage'],
                    'created_at': datetime.now()
                })
            
            elif inventory_qty > sales_qty:
                # Inventory overstated or sales understated
                # For this implementation, we'll adjust inventory down
                adjustment_amount = inventory_qty - sales_qty
                inventory_adjustments.append({
                    'adjustment_id': f"INV_ADJ_{product_id}_{date.strftime('%Y%m%d')}",
                    'product_id': product_id,
                    'date': date,
                    'adjustment_type': 'STOCK_SOLD_DECREASE',
                    'adjustment_quantity': adjustment_amount,
                    'reason': 'Sales-Inventory Synchronization',
                    'original_stock_sold': inventory_qty,
                    'adjusted_stock_sold': sales_qty,
                    'variance_percentage': record['variance_percentage'],
                    'created_at': datetime.now()
                })
        
        return pd.DataFrame(inventory_adjustments), pd.DataFrame(sales_adjustments)
    
    def apply_synchronization_adjustments(self, inventory_adjustments: pd.DataFrame) -> bool:
        """Apply inventory adjustments to fact_inventory table"""
        
        if len(inventory_adjustments) == 0:
            self.logger.info("No inventory adjustments to apply")
            return True
        
        self.logger.info(f"Applying {len(inventory_adjustments)} inventory adjustments...")
        
        try:
            # For each adjustment, update the corresponding inventory records
            for _, adjustment in inventory_adjustments.iterrows():
                update_query = f"""
                UPDATE `fact_inventory`
                SET 
                    stock_sold = stock_sold + {adjustment['adjustment_quantity']},
                    closing_stock = closing_stock - {adjustment['adjustment_quantity']},
                    updated_at = CURRENT_TIMESTAMP()
                WHERE product_id = '{adjustment['product_id']}'
                AND date = '{adjustment['date']}'
                """
                
                self.bq_client.execute_query(update_query)
            
            self.logger.info("Successfully applied inventory adjustments")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to apply inventory adjustments: {str(e)}")
            return False
    
    def validate_synchronization(self, start_date: str = None, end_date: str = None) -> Dict:
        """
        Complete synchronization validation process
        Returns comprehensive validation results
        """
        
        self.logger.info("Starting complete synchronization validation...")
        
        # Load data
        self.load_data(start_date, end_date)
        
        # Analyze gaps
        sync_analysis = self.analyze_synchronization_gaps()
        
        # Generate report
        report = self.generate_synchronization_report(sync_analysis)
        
        # Create adjustments
        inventory_adjustments, sales_adjustments = self.create_synchronization_adjustments(sync_analysis)
        
        # Add adjustment info to report
        report['adjustments'] = {
            'inventory_adjustments_count': len(inventory_adjustments),
            'sales_adjustments_count': len(sales_adjustments),
            'total_adjustment_quantity': inventory_adjustments['adjustment_quantity'].sum() if len(inventory_adjustments) > 0 else 0
        }
        
        return {
            'report': report,
            'sync_analysis': sync_analysis,
            'inventory_adjustments': inventory_adjustments,
            'sales_adjustments': sales_adjustments
        }
    
    def get_sku_level_summary(self) -> pd.DataFrame:
        """Get SKU-level synchronization summary"""
        
        if self.sales_data is None or self.inventory_data is None:
            raise ValueError("Data not loaded. Call load_data() first.")
        
        # Aggregate by SKU
        sku_summary = self.sales_data.merge(
            self.product_data, on='product_id', how='left'
        ).groupby(['sku', 'product_name']).agg({
            'sales_quantity': 'sum',
            'total_amount': 'sum'
        }).reset_index()
        
        # Add inventory data
        inventory_by_sku = self.inventory_data.merge(
            self.product_data, on='product_id', how='left'
        ).groupby(['sku', 'product_name']).agg({
            'stock_sold': 'sum',
            'opening_stock': 'sum',
            'closing_stock': 'sum'
        }).reset_index()
        
        sku_summary = sku_summary.merge(
            inventory_by_sku, on=['sku', 'product_name'], how='outer'
        ).fillna(0)
        
        # Calculate SKU-level metrics
        sku_summary['variance'] = abs(sku_summary['sales_quantity'] - sku_summary['stock_sold'])
        sku_summary['variance_percentage'] = np.where(
            sku_summary['stock_sold'] > 0,
            (sku_summary['variance'] / sku_summary['stock_sold']) * 100,
            np.where(sku_summary['sales_quantity'] > 0, 100.0, 0.0)
        )
        
        sku_summary = sku_summary.sort_values('variance_percentage', ascending=False)
        
        return sku_summary


def run_synchronization_validation(bigquery_client: BigQueryManager, 
                                  start_date: str = None, 
                                  end_date: str = None) -> Dict:
    """
    Convenience function to run complete synchronization validation
    """
    
    synchronizer = InventorySalesSynchronizer(bigquery_client)
    return synchronizer.validate_synchronization(start_date, end_date)
