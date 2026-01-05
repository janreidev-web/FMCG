# User Guide

## Getting Started

This guide will help you set up and use the FMCG Data Analytics Platform.

## Prerequisites

### Required Software

- **Python 3.8 or higher**
- **Google Cloud Account** with BigQuery enabled
- **Git** (for cloning the repository)

### Google Cloud Setup

1. **Create a Google Cloud Project**
   ```bash
   gcloud projects create YOUR_PROJECT_ID
   ```

2. **Enable BigQuery API**
   ```bash
   gcloud services enable bigquery.googleapis.com --project=YOUR_PROJECT_ID
   ```

3. **Create Service Account**
   ```bash
   gcloud iam service-accounts create fmcg-analytics \
     --display-name="FMCG Analytics Service Account" \
     --project=YOUR_PROJECT_ID
   ```

4. **Grant Permissions**
   ```bash
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
     --member="serviceAccount:fmcg-analytics@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataEditor"
   
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
     --member="serviceAccount:fmcg-analytics@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/bigquery.jobUser"
   ```

5. **Download Service Account Key**
   ```bash
   gcloud iam service-accounts keys create ~/fmcg-analytics-key.json \
     --iam-account=fmcg-analytics@YOUR_PROJECT_ID.iam.gserviceaccount.com
   ```

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd FMCG-Data-Analytics
```

### 2. Create Virtual Environment

```bash
# Using venv
python -m venv venv

# Activate on Windows
venv\Scripts\activate

# Activate on macOS/Linux
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set Up Configuration

#### Option A: Environment Variables

```bash
export GCP_PROJECT_ID="YOUR_PROJECT_ID"
export GCP_DATASET="fmcg_warehouse"
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
```

#### Option B: .env File

1. Copy the example file:
```bash
cp .env.example .env
```

2. Edit `.env` with your configuration:
```bash
GCP_PROJECT_ID=YOUR_PROJECT_ID
GCP_DATASET=fmcg_warehouse
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service-account-key.json
```

### 5. Run Setup Script

```bash
python scripts/setup.py
```

This will:
- Verify your configuration
- Test BigQuery connection
- Create the dataset and tables
- Generate a `.env` file if needed

## Usage

### Initial Data Generation

To generate the complete dataset for the first time:

```bash
python src/main.py
```

This will:
- Generate all dimension tables (employees, products, retailers, etc.)
- Generate historical fact tables (10 years of data)
- Load everything into BigQuery
- Display a summary of the generated data

### Incremental Updates

For daily updates (generates new sales data):

```bash
python src/main.py --incremental
```

### Configuration Options

You can customize the data generation by modifying environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `INITIAL_EMPLOYEES` | 350 | Number of employees to generate |
| `INITIAL_PRODUCTS` | 150 | Number of products to generate |
| `INITIAL_RETAILERS` | 500 | Number of retailers to generate |
| `INITIAL_SALES_AMOUNT` | 8000000000 | Total historical sales amount (PHP) |
| `DAILY_SALES_AMOUNT` | 2000000 | Daily sales target (PHP) |
| `BATCH_SIZE` | 1000 | Batch size for data loading |
| `LOG_LEVEL` | INFO | Logging level (DEBUG, INFO, WARNING, ERROR) |

## Data Schema

### Dimension Tables

#### Employees (`dim_employees`)
Contains employee master data with demographics, employment details, and compensation.

**Key Fields:**
- `employee_id`: Unique identifier
- `first_name`, `last_name`: Employee name
- `department_id`: Department reference
- `job_id`: Position reference
- `salary`: Monthly salary
- `hire_date`, `termination_date`: Employment dates

#### Products (`dim_products`)
Product catalog with pricing and categorization.

**Key Fields:**
- `product_id`: Unique identifier
- `product_name`: Product description
- `category_id`, `subcategory_id`: Category references
- `brand_id`: Brand reference
- `unit_price`, `cost`: Pricing information
- `status`: Product status (Active, Discontinued, etc.)

#### Retailers (`dim_retailers`)
Retail network information with geographic distribution.

**Key Fields:**
- `retailer_id`: Unique identifier
- `retailer_name`: Business name
- `retailer_type`: Store type (Sari-Sari, Supermarket, etc.)
- `location_id`: Geographic reference
- `credit_limit`, `payment_terms`: Credit information

#### Locations (`dim_locations`)
Philippines geographic data with regional hierarchy.

**Key Fields:**
- `location_id`: Unique identifier
- `street_address`, `city`, `province`, `region`: Address components
- `latitude`, `longitude`: Coordinates

### Fact Tables

#### Sales (`fact_sales`)
Sales transactions with order and delivery tracking.

**Key Fields:**
- `sale_id`: Unique identifier
- `date`: Transaction date
- `product_id`, `retailer_id`, `employee_id`: References
- `quantity`, `total_amount`: Transaction details
- `delivery_status`: Order fulfillment status

#### Inventory (`fact_inventory`)
Inventory movements and valuations.

**Key Fields:**
- `inventory_id`: Unique identifier
- `date`: Snapshot date
- `product_id`, `location_id`: References
- `opening_stock`, `closing_stock`: Stock levels
- `total_value`: Inventory valuation

#### Operating Costs (`fact_operating_costs`)
Business expenses by category and department.

**Key Fields:**
- `cost_id`: Unique identifier
- `date`: Expense date
- `cost_category`: Expense type
- `department_id`: Department reference
- `amount`: Expense amount

## Query Examples

### Basic Sales Analysis

```sql
-- Total sales by month
SELECT 
  DATE_TRUNC(date, MONTH) as month,
  SUM(total_amount) as total_sales,
  COUNT(*) as transaction_count
FROM `YOUR_PROJECT.fmcg_warehouse.fact_sales`
GROUP BY month
ORDER BY month DESC;
```

### Top Products

```sql
-- Top 10 products by revenue
SELECT 
  p.product_name,
  p.brand_name,
  SUM(s.total_amount) as total_revenue,
  SUM(s.quantity) as total_quantity
FROM `YOUR_PROJECT.fmcg_warehouse.fact_sales` s
JOIN `YOUR_PROJECT.fmcg_warehouse.dim_products` p ON s.product_id = p.product_id
GROUP BY p.product_name, p.brand_name
ORDER BY total_revenue DESC
LIMIT 10;
```

### Regional Performance

```sql
-- Sales by region
SELECT 
  l.region,
  SUM(s.total_amount) as total_sales,
  COUNT(DISTINCT s.retailer_id) as active_retailers
FROM `YOUR_PROJECT.fmcg_warehouse.fact_sales` s
JOIN `YOUR_PROJECT.fmcg_warehouse.dim_retailers` r ON s.retailer_id = r.retailer_id
JOIN `YOUR_PROJECT.fmcg_warehouse.dim_locations` l ON r.location_id = l.location_id
GROUP BY l.region
ORDER BY total_sales DESC;
```

### Employee Performance

```sql
-- Top performing employees
SELECT 
  CONCAT(e.first_name, ' ', e.last_name) as employee_name,
  d.department_name,
  SUM(s.total_amount) as total_sales,
  SUM(s.commission_rate * s.total_amount) as total_commission
FROM `YOUR_PROJECT.fmcg_warehouse.fact_sales` s
JOIN `YOUR_PROJECT.fmcg_warehouse.dim_employees` e ON s.employee_id = e.employee_id
JOIN `YOUR_PROJECT.fmcg_warehouse.dim_departments` d ON e.department_id = d.department_id
WHERE s.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY employee_name, d.department_name
ORDER BY total_sales DESC
LIMIT 10;
```

## Troubleshooting

### Common Issues

#### BigQuery Connection Errors

**Problem**: `Permission denied` or `Authentication failed`

**Solution**:
1. Verify service account permissions
2. Check service account key path
3. Ensure project ID is correct

```bash
# Test connection
gcloud auth activate-service-account --key-file=path/to/key.json
gcloud bigquery tables list --project=YOUR_PROJECT_ID
```

#### Memory Issues

**Problem**: `MemoryError` during data generation

**Solution**:
1. Reduce batch size in configuration
2. Use incremental mode for large datasets
3. Close other applications to free memory

#### Dataset Not Found

**Problem**: `Dataset not found` error

**Solution**:
1. Run setup script: `python scripts/setup.py`
2. Verify dataset name in configuration
3. Check if dataset exists in BigQuery console

### Performance Optimization

#### For Large Datasets

1. **Use Incremental Mode**: Generate only new data
2. **Adjust Batch Size**: Find optimal batch size for your system
3. **Monitor Resources**: Watch memory and CPU usage
4. **Schedule Updates**: Run updates during off-peak hours

#### For Query Performance

1. **Use Partitioning**: Partition fact tables by date
2. **Create Clusters**: Cluster tables by frequently queried columns
3. **Optimize Queries**: Use appropriate WHERE clauses
4. **Cache Results**: Use query result caching

## Advanced Usage

### Custom Data Generation

You can extend the data generators by modifying the `src/core/generators.py` file:

```python
class CustomProductGenerator(ProductGenerator):
    def generate_custom_products(self, count: int) -> pd.DataFrame:
        # Your custom logic here
        pass
```

### Automated Scheduling

Set up automated updates using cron or cloud scheduler:

```bash
# Daily at 2 AM
0 2 * * * cd /path/to/FMCG-Data-Analytics && python src/main.py --incremental
```

### Integration with Other Tools

The generated data can be used with:
- **Power BI**: Connect directly to BigQuery
- **Tableau**: Use BigQuery connector
- **Looker Studio**: Google's BI tool
- **Custom Applications**: Use BigQuery client libraries

## Support

### Getting Help

1. **Check Documentation**: Review this guide and architecture docs
2. **Review Logs**: Check application logs for error details
3. **Test Configuration**: Run setup script to verify configuration
4. **Community**: Post questions to the project repository

### Contributing

1. **Fork Repository**: Create your own copy
2. **Create Branch**: Work on your changes
3. **Test Changes**: Ensure everything works
4. **Submit Pull Request**: Share your improvements

### Best Practices

1. **Regular Updates**: Run incremental updates regularly
2. **Monitor Usage**: Track data generation and query performance
3. **Backup Data**: Export important data periodically
4. **Security**: Rotate service account keys regularly
5. **Documentation**: Keep documentation up to date
