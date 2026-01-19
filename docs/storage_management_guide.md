# Storage Management and Optimization Guide

## Overview

This guide addresses BigQuery storage quota issues and provides comprehensive solutions for managing data storage in your FMCG Data Analytics Platform.

## Problem Analysis

### Current Issue
```
Quota exceeded: Your project exceeded quota for free storage for projects.
```

### Free Tier Limitations
- **Storage Limit**: 10 GB free storage
- **Warning Threshold**: 8 GB (80% usage)
- **Critical Threshold**: 9.5 GB (95% usage)

## Solutions Implemented

### 🔧 **Storage Management Tools**

#### 1. BigQueryStorageManager (`src/utils/storage_manager.py`)
- **Storage Analysis**: Real-time usage monitoring
- **Archiving Strategy**: Automated old data management
- **Optimization Features**: Table partitioning and clustering
- **Usage Reporting**: Comprehensive storage insights

#### 2. Storage-Optimized Synchronizer
- **Storage-Aware Processing**: Limited date ranges to avoid quota issues
- **Batch Processing**: Efficient data handling
- **Fallback Mechanisms**: Graceful error handling

#### 3. Command Line Tools (`scripts/manage_storage.py`)
- **Interactive Management**: User-friendly storage operations
- **Automated Cleanup**: One-click archiving solutions
- **Real-time Monitoring**: Live storage usage tracking

### 📊 **Storage Optimization Strategies**

#### Data Archiving
```bash
# Archive data older than 1 year
python scripts/manage_storage.py --archive

# Custom retention period
python scripts/manage_storage.py --archive --days-to-keep 180
```

#### Storage Analysis
```bash
# Analyze current storage usage
python scripts/manage_storage.py --analyze

# Check specific project/dataset
python scripts/manage_storage.py --project-id your-project --dataset your-dataset
```

#### Storage Optimization
```bash
# Create aggregated views and optimize tables
python scripts/manage_storage.py --optimize
```

## Immediate Actions Required

### 🚨 **Critical Steps**

1. **Run Storage Analysis**
   ```bash
   python scripts/manage_storage.py --analyze
   ```

2. **Archive Old Data** (if needed)
   ```bash
   python scripts/manage_storage.py --archive
   ```

3. **Use Storage-Aware Synchronization**
   ```bash
   python scripts/synchronize_sales_inventory.py --start-date 2024-12-01
   ```

### 📋 **Storage Management Workflow**

#### Step 1: Assess Current Usage
```python
from src.utils.storage_manager import BigQueryStorageManager

storage_manager = BigQueryStorageManager('your-project', 'your-dataset')
report = storage_manager.generate_storage_report()

print(f"Storage Status: {report['storage_analysis']['status']}")
print(f"Usage: {report['storage_analysis']['total_storage_gb']:.2f} GB")
```

#### Step 2: Archive Historical Data
```python
# Archive data older than 1 year
archiving_candidates = storage_manager.identify_archiving_candidates(days_to_keep=365)

for table, info in archiving_candidates.items():
    cutoff_date = info['cutoff_date']
    storage_manager.archive_old_data(table, str(cutoff_date))
```

#### Step 3: Optimize Storage Structure
```python
# Create aggregated views
storage_manager.create_aggregated_views()

# Optimize table partitioning
storage_manager.optimize_table_storage('fact_sales')
storage_manager.optimize_table_storage('fact_inventory')
```

## Long-term Storage Strategy

### 🗄️ **Data Retention Policy**

#### Active Data (Last 30 Days)
- **Location**: Main fact tables
- **Access**: Real-time queries
- **Synchronization**: Daily updates

#### Recent Data (30-365 Days)
- **Location**: Main fact tables (with optimization)
- **Access**: Monthly aggregated views
- **Synchronization**: Weekly validation

#### Historical Data (>1 Year)
- **Location**: Archive tables
- **Access**: On-demand queries
- **Synchronization**: Monthly validation

### 📈 **Storage Optimization Techniques**

#### 1. Table Partitioning
```sql
-- Partition by date for efficient querying
CREATE TABLE fact_sales_partitioned
PARTITION BY date
CLUSTER BY product_id
AS SELECT * FROM fact_sales
```

#### 2. Aggregated Views
```sql
-- Monthly summary for historical analysis
CREATE VIEW monthly_sales_summary AS
SELECT 
    DATE_TRUNC(date, MONTH) as month,
    product_id,
    SUM(quantity) as total_quantity,
    SUM(total_amount) as total_amount
FROM fact_sales
GROUP BY month, product_id
```

#### 3. Data Compression
- Use appropriate data types
- Remove unnecessary columns
- Implement efficient schemas

## Monitoring and Maintenance

### 📊 **Daily Monitoring**
```python
# Check storage status daily
storage_manager = BigQueryStorageManager(project_id, dataset)
usage = storage_manager.get_storage_usage()

if usage['status'] == 'CRITICAL':
    # Trigger immediate cleanup
    storage_manager.archive_old_data('fact_sales', '2023-01-01')
```

### 🔄 **Weekly Maintenance**
- Review storage trends
- Archive data approaching limits
- Optimize table structures
- Update retention policies

### 📅 **Monthly Review**
- Analyze storage growth patterns
- Adjust archiving thresholds
- Review query performance
- Update optimization strategies

## Integration with Existing Workflows

### 🔄 **Updated Synchronization Script**
The synchronization script now includes storage-aware features:

```python
# Storage-aware synchronization (default 30 days)
python scripts/synchronize_sales_inventory.py

# Custom date range with storage constraints
python scripts/synchronize_sales_inventory.py --start-date 2024-01-01 --end-date 2024-01-31
```

### 🚀 **ETL Pipeline Integration**
```python
# Add storage check before data generation
from src.utils.storage_manager import StorageOptimizedSynchronizer

sync = StorageOptimizedSynchronizer(bigquery_client, storage_manager)
if sync.check_storage_before_sync():
    # Proceed with synchronization
    results = sync.run_storage_aware_sync()
```

## Cost Management

### 💰 **Free Tier Optimization**
- **Target Usage**: Keep under 8 GB (80% of limit)
- **Buffer Zone**: Maintain 2 GB buffer
- **Monitoring**: Daily usage checks

### 💳 **Paid Tier Considerations**
- **Break-even Point**: When storage needs exceed 10 GB
- **Cost Analysis**: $20/GB/month for on-demand pricing
- **Upgrade Strategy**: Gradual scale-up based on usage

## Troubleshooting

### 🚨 **Common Issues**

#### 1. Quota Exceeded Error
```bash
# Immediate action
python scripts/manage_storage.py --archive

# Check results
python scripts/manage_storage.py --analyze
```

#### 2. Slow Query Performance
```bash
# Optimize tables
python scripts/manage_storage.py --optimize

# Use aggregated views
SELECT * FROM monthly_sales_summary WHERE month >= '2024-01-01'
```

#### 3. Synchronization Failures
```bash
# Use storage-aware sync
python scripts/synchronize_sales_inventory.py --start-date 2024-12-01

# Check storage status first
python scripts/manage_storage.py --analyze
```

### 📞 **Support and Recovery**

#### Data Recovery
- Archive tables maintain historical data
- Point-in-time recovery available
- Automated backup recommendations

#### Performance Tuning
- Query optimization guidelines
- Index and clustering strategies
- Caching recommendations

## Best Practices

### ✅ **Do's**
- Monitor storage usage daily
- Archive data regularly
- Use aggregated views for historical analysis
- Implement storage-aware processing
- Set up automated alerts

### ❌ **Don'ts**
- Exceed free tier limits without planning
- Store unnecessary historical data
- Ignore storage warnings
- Run large queries without limits
- Skip regular maintenance

## Future Enhancements

### 🚀 **Planned Features**
1. **Automated Archiving**: Scheduled data cleanup
2. **Smart Compression**: AI-driven optimization
3. **Multi-region Storage**: Geographic distribution
4. **Real-time Monitoring**: Dashboard integration
5. **Predictive Analytics**: Storage forecasting

### 📊 **Performance Improvements**
1. **Query Optimization**: Advanced caching
2. **Data Streaming**: Real-time processing
3. **Parallel Processing**: Multi-threaded operations
4. **Memory Management**: Efficient resource usage

## Emergency Procedures

### 🆘 **Storage Crisis Response**
1. **Immediate Assessment**: Run storage analysis
2. **Emergency Archiving**: Archive oldest data first
3. **Service Continuation**: Use storage-aware mode
4. **Recovery Planning**: Implement long-term solution

### 📞 **Escalation Contacts**
- **Technical Support**: Storage management team
- **Business Impact**: Data analytics stakeholders
- **Cost Approval**: Finance department for upgrades

By implementing these storage management strategies, you can maintain efficient operations within BigQuery's free tier limits while ensuring data integrity and system performance.
