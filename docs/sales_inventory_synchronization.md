# Sales-Inventory Synchronization Documentation

## Overview

The Sales-Inventory Synchronization module ensures data consistency between `fact_sales` and `fact_inventory` tables in the FMCG Data Analytics Platform. It identifies discrepancies between sales quantities and inventory stock movements, providing automated reconciliation capabilities.

## Key Features

### 🔍 **Variance Analysis**
- Compares sales quantities with inventory stock_sold values
- Calculates variance percentages with configurable thresholds
- Classifies variances as ACCEPTABLE, WARNING, or CRITICAL
- Provides detailed SKU-level analysis

### 📊 **Comprehensive Reporting**
- Summary statistics with variance distribution
- Critical issue identification with top problem records
- Actionable recommendations based on analysis results
- Volume statistics and affected SKU/date tracking

### 🔧 **Automated Adjustments**
- Generates inventory adjustment records for significant variances
- Supports both stock_sold increases and decreases
- Maintains audit trail with adjustment reasons and timestamps
- Batch processing for efficient reconciliation

### 📦 **SKU-Level Insights**
- Aggregated variance analysis by SKU
- Product name and category context
- Trend identification across product lines
- Priority-based issue resolution

## Architecture

### Core Components

1. **InventorySalesSynchronizer**: Main synchronization engine
2. **Data Loading**: Efficient BigQuery data retrieval with date filtering
3. **Variance Analysis**: Statistical comparison and classification
4. **Adjustment Engine**: Automated correction generation
5. **Reporting**: Comprehensive analysis output

### Data Flow

```
BigQuery Tables → Data Loading → Variance Analysis → Report Generation → Adjustment Creation → Application
```

## Usage

### Basic Synchronization Check

```python
from src.utils.inventory_sales_sync import run_synchronization_validation
from src.utils.bigquery_client import BigQueryManager

# Initialize BigQuery client
bq_client = BigQueryManager()

# Run validation for last 30 days
results = run_synchronization_validation(bq_client)

# View results
print(f"Critical issues: {results['report']['summary']['critical_variance_records']}")
print(f"Average variance: {results['report']['summary']['average_variance_percentage']:.2f}%")
```

### Advanced Usage with Custom Date Range

```python
synchronizer = InventorySalesSynchronizer(bq_client)

# Load specific date range
synchronizer.load_data('2024-01-01', '2024-01-31')

# Analyze gaps
sync_analysis = synchronizer.analyze_synchronization_gaps()

# Generate report
report = synchronizer.generate_synchronization_report(sync_analysis)

# Create and apply adjustments
inventory_adj, sales_adj = synchronizer.create_synchronization_adjustments(sync_analysis)
synchronizer.apply_synchronization_adjustments(inventory_adj)
```

### Command Line Interface

```bash
# Basic validation
python scripts/synchronize_sales_inventory.py

# Custom date range
python scripts/synchronize_sales_inventory.py --start-date 2024-01-01 --end-date 2024-01-31

# Apply adjustments interactively
python scripts/synchronize_sales_inventory.py --apply-adjustments --interactive

# Show detailed SKU summary
python scripts/synchronize_sales_inventory.py --sku-summary
```

## Configuration

### Variance Thresholds

```python
# In InventorySalesSynchronizer class
self.max_acceptable_variance = 0.05  # 5% variance tolerance
self.critical_variance = 0.15       # 15% critical variance
```

### Adjustment Types

- **STOCK_SOLD_INCREASE**: When sales quantity > inventory stock_sold
- **STOCK_SOLD_DECREASE**: When inventory stock_sold > sales quantity

## Reports and Metrics

### Summary Statistics

- Total records analyzed
- Critical/Warning/Acceptable variance counts
- Average and maximum variance percentages
- Total adjustment quantities required

### Critical Issues

Top 10 records with highest variance, including:
- Product SKU and name
- Date of discrepancy
- Sales vs inventory quantities
- Variance percentage and amount

### Recommendations

Automated suggestions based on:
- Critical variance count
- Warning variance percentage
- Overall variance trends

## Testing

### Unit Tests

```bash
python tests/test_synchronization.py
```

### Integration Tests

The test suite includes:
- Data loading validation
- Variance calculation accuracy
- Report generation verification
- Adjustment creation testing
- SKU-level summary validation

## Performance Considerations

### Data Volume Optimization

- Date range filtering for large datasets
- Efficient BigQuery query patterns
- Batch processing for adjustments
- Memory-conscious data handling

### Scalability Features

- Configurable batch sizes
- Parallel processing capabilities
- Progress logging for long operations
- Error handling and recovery

## Error Handling

### Common Issues

1. **Missing Data**: Handles null values in sales/inventory
2. **Date Mismatches**: Aligns sales and inventory dates
3. **Product Gaps**: Reports missing product mappings
4. **Calculation Errors**: Graceful handling of division by zero

### Logging

Comprehensive logging at multiple levels:
- Data loading progress
- Variance analysis results
- Adjustment application status
- Error details and recovery actions

## Best Practices

### Regular Synchronization

1. **Daily Validation**: Check for recent discrepancies
2. **Weekly Analysis**: Review trends and patterns
3. **Monthly Reconciliation**: Apply systematic adjustments
4. **Quarterly Review**: Update thresholds and processes

### Data Quality

1. **Consistent Recording**: Ensure accurate sales and inventory entry
2. **Timely Updates**: Process transactions promptly
3. **Audit Trails**: Maintain adjustment history
4. **Validation Checks**: Pre-emptive data quality rules

### Monitoring

1. **Variance Trends**: Track improvement over time
2. **SKU Performance**: Identify problematic products
3. **Process Efficiency**: Monitor reconciliation speed
4. **System Health**: Regular synchronization validation

## Integration

### ETL Pipeline Integration

```python
# Add to existing ETL pipeline
from src.utils.inventory_sales_sync import InventorySalesSynchronizer

# After fact table generation
synchronizer = InventorySalesSynchronizer(bigquery_client)
sync_results = synchronizer.validate_synchronization()

# Log results
pipeline_logger.info(f"Synchronization: {sync_results['report']['summary']}")
```

### Scheduled Automation

```bash
# Cron job for daily synchronization
0 2 * * * cd /path/to/project && python scripts/synchronize_sales_inventory.py --apply-adjustments
```

## Troubleshooting

### High Variance Issues

1. **Check Data Entry**: Verify sales and inventory recording accuracy
2. **Review Processes**: Identify timing discrepancies in data capture
3. **System Integration**: Ensure proper data flow between systems
4. **Training**: Address staff understanding of recording requirements

### Performance Issues

1. **Date Range**: Use smaller date ranges for large datasets
2. **Query Optimization**: Review BigQuery query performance
3. **Memory Usage**: Monitor system resources during processing
4. **Batch Size**: Adjust processing batch sizes as needed

## Future Enhancements

### Planned Features

1. **Machine Learning**: Predictive variance detection
2. **Real-time Sync**: Continuous monitoring capabilities
3. **Advanced Analytics**: Trend analysis and forecasting
4. **Multi-location Support**: Warehouse-level synchronization
5. **API Integration**: External system connectivity

### Scalability Improvements

1. **Distributed Processing**: Multi-node synchronization
2. **Cloud-native**: Enhanced BigQuery integration
3. **Stream Processing**: Real-time data validation
4. **Automated Resolution**: AI-powered adjustment recommendations
