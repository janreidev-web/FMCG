# Large-Scale Data Management Guide
## Optimized for 471K Sales Records and 2M Inventory Records

## Overview

Your FMCG platform handles significant data volume with **471,854 sales records** and **2M inventory records**, which explains the BigQuery storage quota issues. This guide provides specialized solutions for managing datasets at this scale.

## Current Data Analysis

### 📊 **Dataset Scale**
- **fact_sales**: 471,854 rows (~15.1 GB estimated storage)
- **fact_inventory**: 2,000,000 rows (~11.2 GB estimated storage)
- **Total Estimated Storage**: ~26.3 GB (exceeds 10GB free tier)

### 🚨 **Storage Impact**
- **Current Status**: Exceeds free tier limit by 16.3 GB
- **Immediate Action Required**: Archive historical data
- **Optimization Potential**: Significant storage savings available

## Large-Scale Solutions

### 🔧 **Specialized Tools**

#### 1. LargeScaleDataManager (`src/utils/large_scale_manager.py`)
- **Batch Processing**: 50K row batches for efficient processing
- **Smart Archiving**: Identifies and archives 6+ month old data
- **Optimized Views**: Creates aggregated views for historical analysis
- **Performance Tuning**: Large dataset query optimization

#### 2. Enhanced Synchronization
- **Storage-Aware Loading**: Limits to 30 days by default
- **Batch Processing**: 50K record limits to prevent timeouts
- **Error Recovery**: Graceful handling of large dataset issues
- **Progress Tracking**: Detailed logging for long operations

#### 3. Command Line Management (`scripts/manage_large_scale.py`)
- **Scale Analysis**: Detailed dataset analysis and recommendations
- **Bulk Archiving**: Efficient large-scale data movement
- **Performance Optimization**: Query and storage optimization
- **Interactive Management**: User-friendly large dataset operations

## Immediate Actions Required

### 🚨 **Critical Steps**

1. **Analyze Large Dataset Scale**
   ```bash
   python scripts/manage_large_scale.py --analyze
   ```

2. **Execute Large-Scale Archiving**
   ```bash
   python scripts/manage_large_scale.py --archive
   ```

3. **Create Optimized Views**
   ```bash
   python scripts/manage_large_scale.py --optimize-views
   ```

4. **Run Large-Scale Synchronization**
   ```bash
   python scripts/manage_large_scale.py --sync
   ```

## Large-Scale Archiving Strategy

### 📅 **Data Retention Policy**

#### Active Data (Last 90 Days)
- **Location**: Main fact tables
- **Purpose**: Real-time synchronization and daily operations
- **Estimated Rows**: ~50K sales, ~200K inventory
- **Storage**: ~1.5 GB

#### Recent Data (90-180 Days)
- **Location**: Main fact tables with optimization
- **Purpose**: Monthly analysis and reporting
- **Estimated Rows**: ~100K sales, ~400K inventory
- **Storage**: ~3.0 GB

#### Historical Data (>180 Days)
- **Location**: Archive tables (`_archive` suffix)
- **Purpose**: Historical trends and compliance
- **Estimated Rows**: ~322K sales, ~1.4M inventory
- **Storage**: ~21.8 GB → **MOVED TO ARCHIVE**

### 🎯 **Expected Storage Savings**

| Table | Current Rows | Archive Target | Savings |
|-------|-------------|----------------|---------|
| fact_sales | 471,854 | ~322K (6+ months) | ~10.3 GB |
| fact_inventory | 2,000,000 | ~1.4M (6+ months) | ~15.6 GB |
| **Total** | **2,471,854** | **~1.7M** | **~25.9 GB** |

### 📊 **Post-Archiving Storage**

- **Main Tables**: ~0.4 GB (active + recent data)
- **Archive Tables**: ~25.9 GB (historical data)
- **Total**: ~26.3 GB (same data, better organized)
- **Free Tier Impact**: Only 0.4 GB counts toward quota

## Optimized Query Strategies

### 🚀 **Performance Optimizations**

#### 1. Use Aggregated Views
```sql
-- Instead of scanning 2M inventory rows
SELECT * FROM monthly_inventory_aggregated 
WHERE month >= '2024-01-01'

-- Instead of scanning 471K sales rows
SELECT * FROM daily_sales_aggregated 
WHERE date >= '2024-01-01'
```

#### 2. Date-Based Filtering
```sql
-- Limit to recent data for performance
SELECT product_id, SUM(quantity) as total_sold
FROM fact_sales 
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY product_id
```

#### 3. Batch Processing
```sql
-- Process in batches for large operations
SELECT * FROM fact_sales 
WHERE date >= '2024-01-01' 
ORDER BY date
LIMIT 50000
```

## Large-Scale Synchronization

### 🔄 **Storage-Aware Synchronization**

#### Configuration for Large Datasets
```python
# Optimized settings for 471K/2M rows
config = {
    'max_days': 30,           # Only process 30 days
    'batch_size': 50000,      # 50K row batches
    'storage_aware': True,    # Enable storage constraints
    'timeout_handling': True  # Handle timeouts gracefully
}
```

#### Execution Strategy
```bash
# Run with large dataset optimizations
python scripts/manage_large_scale.py --sync

# Custom batch size for faster processing
python scripts/manage_large_scale.py --sync --batch-size 100000
```

### 📈 **Performance Monitoring**

#### Key Metrics
- **Processing Time**: Aim for <5 minutes per batch
- **Memory Usage**: Monitor during large operations
- **Query Costs**: Track BigQuery query costs
- **Storage Usage**: Daily storage monitoring

#### Alert Thresholds
- **Query Timeout**: >10 minutes requires optimization
- **Memory Usage**: >2GB requires batch processing
- **Storage Growth**: >1GB/month requires archiving
- **Cost Increase**: >$50/month requires review

## Advanced Optimizations

### 🔧 **Table Partitioning**

#### Recommended Partitioning Strategy
```sql
-- Partition fact_sales by date for efficient queries
CREATE TABLE fact_sales_partitioned
PARTITION BY date
CLUSTER BY product_id, retailer_id
AS SELECT * FROM fact_sales

-- Partition fact_inventory by date
CREATE TABLE fact_inventory_partitioned  
PARTITION BY date
CLUSTER BY product_id, location_id
AS SELECT * FROM fact_inventory
```

#### Benefits
- **Query Performance**: 50-80% faster date-based queries
- **Cost Reduction**: Only scan relevant partitions
- **Storage Efficiency**: Better compression per partition

### 📊 **Materialized Views**

#### High-Usage Aggregations
```sql
-- Daily sales summary (most accessed)
CREATE MATERIALIZED VIEW daily_sales_summary AS
SELECT 
    date,
    product_id,
    SUM(quantity) as total_quantity,
    SUM(total_amount) as total_revenue,
    COUNT(*) as transaction_count
FROM fact_sales
GROUP BY date, product_id

-- Monthly inventory trends
CREATE MATERIALIZED VIEW monthly_inventory_trends AS
SELECT 
    DATE_TRUNC(date, MONTH) as month,
    product_id,
    AVG(closing_stock) as avg_closing_stock,
    SUM(stock_sold) as total_sold
FROM fact_inventory
GROUP BY month, product_id
```

## Monitoring and Maintenance

### 📊 **Daily Monitoring**

#### Storage Monitoring Script
```python
from src.utils.large_scale_manager import LargeScaleDataManager

manager = LargeScaleDataManager(project_id, dataset)
storage_status = manager.analyze_large_dataset_storage()

if storage_status['fact_sales']['rows'] > 500000:
    print("⚠️  Sales table growing large - consider archiving")
```

#### Performance Monitoring
```python
# Monitor query performance
query_stats = manager.get_query_performance_stats()

if query_stats['avg_query_time'] > 300:  # 5 minutes
    print("🚨 Query performance degrading - optimize queries")
```

### 🔄 **Weekly Maintenance**

#### Archiving Tasks
```bash
# Weekly archiving of old data
python scripts/manage_large_scale.py --archive --archive-days 180

# Update aggregated views
python scripts/manage_large_scale.py --optimize-views
```

#### Performance Optimization
```bash
# Analyze and optimize slow queries
python scripts/manage_large_scale.py --performance

# Update table statistics
bq query --use_legacy_sql=false "ANALYZE fact_sales"
```

### 📅 **Monthly Review**

#### Capacity Planning
```python
# Project storage growth
current_growth = manager.calculate_monthly_growth()
projected_storage = manager.project_storage_usage(months=6)

if projected_storage > 10:  # GB
    print("🚨 Storage will exceed limits in 6 months")
```

#### Cost Analysis
```python
# Analyze query costs
cost_analysis = manager.analyze_query_costs()

if cost_analysis['monthly_cost'] > 100:  # USD
    print("💰 Query costs high - optimize queries")
```

## Emergency Procedures

### 🆘 **Storage Crisis Response**

#### Immediate Actions
1. **Stop Data Generation**: Pause ETL processes
2. **Emergency Archiving**: Archive oldest 6 months immediately
3. **Query Optimization**: Switch to aggregated views only
4. **Contact Support**: Consider paid tier upgrade

#### Recovery Script
```bash
# Emergency storage recovery
python scripts/manage_large_scale.py --archive --archive-days 90

# Force optimization
python scripts/manage_large_scale.py --optimize-views --performance
```

### 🚨 **Performance Crisis Response**

#### Query Timeout Handling
1. **Reduce Batch Size**: Use 25K rows instead of 50K
2. **Increase Timeouts**: Set query timeout to 15 minutes
3. **Use Partitions**: Query only specific date ranges
4. **Fallback Views**: Use pre-aggregated data

## Cost Management

### 💰 **Free Tier Optimization**

#### Current Costs
- **Storage**: $0/month (target: <10GB in main tables)
- **Queries**: ~$20-50/month (depending on usage)
- **Operations**: Minimal with optimized queries

#### Cost Reduction Strategies
1. **Aggregated Views**: Reduce full table scans by 80%
2. **Smart Archiving**: Keep only 90 days in main tables
3. **Query Optimization**: Use LIMIT and date filters
4. **Batch Processing**: Reduce query complexity

### 💳 **Paid Tier Considerations**

#### Upgrade Thresholds
- **Storage**: >10GB sustained usage
- **Queries**: >$100/month consistently
- **Performance**: Frequent timeouts

#### Cost-Benefit Analysis
```
Current: $20-50/month (free tier + optimizations)
Paid Tier: $20/month + storage costs
Break-even: ~15GB sustained storage usage
```

## Best Practices for Large Datasets

### ✅ **Do's**
- Use date-based filtering for all queries
- Process data in batches (50K rows)
- Monitor storage usage daily
- Use aggregated views for historical analysis
- Archive data older than 6 months
- Implement query timeouts and retry logic

### ❌ **Don'ts**
- Run SELECT * on large tables
- Process more than 90 days without batching
- Ignore storage warnings
- Use full table scans for trends
- Skip regular maintenance
- Assume queries will complete quickly

## Future Scalability

### 🚀 **Scaling Strategies**

#### Horizontal Scaling
- **Multi-region Distribution**: Geographic data distribution
- **Table Sharding**: Split large tables by date ranges
- **Read Replicas**: Separate read/write workloads

#### Vertical Scaling
- **Memory Optimization**: Increase query memory limits
- **Parallel Processing**: Multi-threaded operations
- **Advanced Caching**: Smart result caching

### 📈 **Performance Roadmap**

#### Short-term (1-3 months)
- Implement automated archiving
- Create comprehensive aggregated views
- Optimize query patterns
- Establish monitoring alerts

#### Medium-term (3-6 months)
- Implement table partitioning
- Create materialized views
- Optimize ETL processes
- Scale to 1M+ sales records

#### Long-term (6-12 months)
- Consider paid tier upgrade
- Implement advanced caching
- Multi-region deployment
- Real-time synchronization

By implementing these large-scale data management strategies, you can efficiently handle your 471K sales and 2M inventory records while staying within BigQuery's free tier limits and maintaining optimal performance.
