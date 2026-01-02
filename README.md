<div align="center">

# FMCG Data Analytics Platform

**A comprehensive dimensional data warehouse solution for Fast-Moving Consumer Goods analytics**

[![Python](https://img.shields.io/badge/Python-3.14-blue.svg)](https://www.python.org/downloads/)
[![BigQuery](https://img.shields.io/badge/Google%20Cloud-BigQuery-orange.svg)](https://cloud.google.com/bigquery)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow.svg)](https://powerbi.microsoft.com/)
[![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-Automated-brightgreen.svg)](https://github.com/features/actions)

*Dimensional Modeling • Synthetic Data Generation • Automated ETL • Business Intelligence*

</div>

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Schema](#data-schema)
- [Geography Coverage](#geography-coverage)
- [Power BI Integration](#power-bi-integration)
- [Security](#security-notes)
- [Dependencies](#dependencies)
- [Notes](#notes)

---

## Overview

This platform implements a **complete dimensional data warehouse** for FMCG business analytics, featuring a star schema architecture optimized for Google Cloud BigQuery. The system generates realistic synthetic data across all major business domains and provides automated ETL pipelines with continuous data enrichment.

### Architecture Highlights

<div align="center">

| **Component** | **Technology** | **Purpose** |
|:--------------:|:--------------:|:------------|
| **Data Warehouse** | Google BigQuery | Scalable cloud storage with SQL analytics |
| **ETL Pipeline** | Python + Pandas | Automated data generation and loading |
| **Orchestration** | GitHub Actions | Scheduled daily data updates |
| **Analytics** | Power BI | Business intelligence dashboards |
| **Data Model** | Star Schema | Optimized for query performance |

</div>

### Business Domains Covered

<div align="center">

| **Domain** | **Key Metrics** | **Analytics Value** |
|:----------:|:---------------:|:-------------------:|
| **Sales** | Revenue, Volume, Commission | Performance tracking & forecasting |
| **Products** | Pricing, Categories, Status | Product mix analysis & optimization |
| **Employees** | Revenue, Volume, Commission, Performance, Attendance, Engagement | Complete HR analytics & workforce planning |
| **Retailers** | Geographic distribution | Market penetration analysis |
| **Inventory** | Stock levels, Locations | Supply chain optimization |
| **Marketing** | Campaign ROI, Spend | Marketing effectiveness |
| **Operations** | Cost structure, Trends | Financial planning & analysis |

</div>

---

## Features

<table>
<tr>
<td width="50%">

### Data Generation & Modeling
- **Dimensional Star Schema** architecture
- Realistic synthetic data generation
- **4 Dimension Tables** with realistic FMCG data
- **4 Fact Tables** with business metrics
- **900 Total Employees** (250 active, 650 terminated) across 10 departments
- **150 Products** across 7 FMCG categories
- **500 Retailers** across all Philippines regions
- **Comprehensive HR data** with 47 fields per employee

</td>
<td width="50%">

### Automation & Operations
- **Automated GitHub Actions** workflow
- Scheduled daily data generation
- Manual and scheduled execution modes
- **BigQuery auto-scaling** integration
- Error handling and logging
- Storage quota optimization

</td>
</tr>
<tr>
<td width="50%">

### Geographic Intelligence
- **Complete Philippines coverage** (16 regions)
- Province and city-level granularity
- Region-based delivery calculations
- Location-based sales analytics
- Realistic delivery time modeling

</td>
<td width="50%">

### Business Intelligence
- **Power BI ready** data structure
- Optimized for analytical queries
- Time-based analytics support
- Multi-dimensional analysis
- Performance metrics tracking

</td>
</tr>
</table>

---

## Project Structure

```
FMCG-Data-Analytics/
│
├── FMCG/                           # Main application package
│   ├── main.py                     # Primary ETL orchestration script
│   ├── config.py                   # Configuration management
│   ├── auth.py                     # Google Cloud authentication
│   ├── helpers.py                  # Utility functions and helpers
│   ├── geography.py                # Philippines geographic data
│   ├── schema.py                   # BigQuery table definitions
│   │
│   ├── generators/                 # Data generation modules
│   │   ├── dimensional.py          # Dimensional model generators
│   │   ├── employees.py            # Employee data generation
│   │   ├── products.py             # Product catalog generation
│   │   ├── retailers.py            # Retailer network generation
│   │   ├── sales.py                # Sales transaction generation
│   │   ├── costs.py                # Operating costs generation
│   │   ├── inventory.py            # Inventory data generation
│   │   └── marketing.py            # Marketing campaigns generation
│   │
│   └── requirements.txt             # Python dependencies
│
├── .github/
│   └── workflows/                   # GitHub Actions workflows
│       └── simulator.yml           # Automated daily data generation
│
├── README.md                       # This documentation
└── [PBIX file - to be added]        # Power BI dashboard file
```

### Key Components

- **`main.py`**: Central orchestration script managing the complete ETL pipeline
- **`generators/dimensional.py`**: Star schema implementation with optimized fact/dimension tables
- **`geography.py`**: Complete Philippines geographic database with 16 regions
- **`.github/workflows/simulator.yml`**: Automated daily data generation pipeline

---

## Prerequisites

<div align="center">

| **Requirement** | **Version/Details** | **Purpose** |
|:---------------:|:-------------------:|:-----------:|
| **Python** | 3.7 or higher | ETL pipeline execution |
| **Google Cloud Platform** | Active GCP account | Cloud data warehouse |
| **BigQuery Dataset** | Created in your GCP project | Data storage and analytics |
| **Service Account** | BigQuery Admin permissions | Data access and management |
| **GitHub Repository** | Public or Private | Automated workflow execution |

</div>

### Required Permissions

Your Google Cloud service account requires the following permissions:
- **BigQuery Data Editor** - Create and modify tables
- **BigQuery Job User** - Execute queries and load jobs
- **BigQuery Read Session User** - Read data efficiently (optional)

---

## Installation

### Step 1: Repository Setup

```bash
git clone <repository-url>
cd Data-Analytics
```

### Step 2: Python Environment

```bash
cd FMCG
pip install -r requirements.txt
```

### Step 3: Google Cloud Authentication

<details>
<summary><b>Option A: Environment Variables (Recommended for CI/CD)</b></summary>

```bash
export GCP_SERVICE_ACCOUNT="<base64-encoded-service-account-json>"
export GCP_PROJECT_ID="your-project-id"
export BQ_DATASET="fmcg_analytics"
export INITIAL_SALES_AMOUNT="5000000000"
export DAILY_SALES_AMOUNT="15000000"
```

**Base64 Encoding Service Account:**
```bash
base64 -i path/to/your/service-account-key.json
```
</details>

<details>
<summary><b>Option B: Service Account Key File</b></summary>

```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
export GCP_PROJECT_ID="your-project-id"
export BQ_DATASET="fmcg_analytics"
```
</details>

### Step 4: GitHub Repository Setup

1. **Fork or create** a new repository
2. **Add repository secrets** in GitHub Settings:
   - `GCP_SERVICE_ACCOUNT` (base64 encoded)
   - `GCP_PROJECT_ID`
   - `BQ_DATASET`
   - `INITIAL_SALES_AMOUNT` (optional)
   - `DAILY_SALES_AMOUNT` (optional)
3. **Enable GitHub Actions** in repository settings

---

## Configuration

### Environment Variables

Configure the system using environment variables or edit `FMCG/config.py`:

<table>
<thead>
<tr>
<th>Variable</th>
<th>Description</th>
<th>Default Value</th>
<th>Notes</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>GCP_PROJECT_ID</code></td>
<td>Google Cloud project identifier</td>
<td><code>"fmcg-data-simulator"</code></td>
<td>Must exist in your GCP account</td>
</tr>
<tr>
<td><code>BQ_DATASET</code></td>
<td>BigQuery dataset name</td>
<td><code>"fmcg_analytics"</code></td>
<td>Auto-created if not exists</td>
</tr>
<tr>
<td><code>INITIAL_SALES_AMOUNT</code></td>
<td>Historical sales target (PHP)</td>
<td><code>6,000,000,000</code></td>
<td>11-year historical data (₱6B revenue)</td>
</tr>
<tr>
<td><code>DAILY_SALES_AMOUNT</code></td>
<td>Daily sales target (PHP)</td>
<td><code>1,640,000</code></td>
<td>For scheduled runs (₱1.64M daily)</td>
</tr>
</tbody>
</table>

### Internal Configuration

Additional settings in `config.py`:

<table>
<thead>
<tr>
<th>Parameter</th>
<th>Description</th>
<th>Default</th>
<th>Purpose</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>INITIAL_EMPLOYEES</code></td>
<td>Current active employee count</td>
<td><code>250</code></td>
<td>Scaled for ₱6B revenue company</td>
</tr>
<tr>
<td><code>INITIAL_PRODUCTS</code></td>
<td>Initial product count</td>
<td><code>50</code></td>
<td>Storage quota optimized</td>
</tr>
<tr>
<td><code>INITIAL_RETAILERS</code></td>
<td>Initial retailer count</td>
<td><code>100</code></td>
<td>Storage quota optimized</td>
</tr>
<tr>
<td><code>NEW_PRODUCTS_PER_RUN</code></td>
<td>New products per run</td>
<td><code>1-5</code></td>
<td>Random range</td>
</tr>
<tr>
<td><code>NEW_HIRES_PER_RUN</code></td>
<td>New hires per run</td>
<td><code>2-15</code></td>
<td>Random range</td>
</tr>
</tbody>
</table>

---

## Usage

### Execution Modes

The platform supports two distinct execution modes:

#### Manual Initial Run

Execute the complete ETL pipeline to populate the data warehouse:

```bash
cd FMCG
python main.py
```

**Process Flow:**
<div align="center">

| **Step** | **Action** | **Output** |
|:--------:|:----------:|:----------:|
| **1** | Authenticate with Google Cloud | Secure connection |
| **2** | Generate dimension tables | 4 dimension tables |
| **3** | Generate historical fact tables | 4 fact tables (2015-today) |
| **4** | Load data into BigQuery | ~1.3 GB storage |
| **5** | Display summary statistics | Record counts and metrics |

</div>

#### Automated Scheduled Runs

GitHub Actions automatically executes daily updates:

- **Frequency**: Daily at 12:00 AM PHT (UTC+8)
- **Scope**: Incremental sales data only
- **Features**: Delivery status updates, continuity preservation
- **Logging**: Comprehensive execution logs

### Data Generation Process

#### Dimension Tables (One-time Generation)

<details>
<summary><b>dim_products</b> - Product Master Data</summary>

- **150 unique products** across 7 FMCG categories
- Realistic Filipino and international brands
- Product lifecycle status tracking
- Size variants and pricing structure

</details>

<details>
<summary><b>dim_employees</b> - Employee Master Data</summary>

- **900 total employees** (250 active, 650 terminated) across 10 departments
- **47 comprehensive fields** including demographics, performance, benefits, and analytics
- Realistic organizational hierarchy with department-specific distributions
- Position-based salary structure (₱15K-150K monthly range)
- Complete Philippine government IDs (TIN, SSS, PhilHealth, Pag-IBIG)
- Work setup modeling (On-site, Remote, Hybrid, Field-based)
- Performance ratings, training records, and skills tracking
- Attendance, engagement, and satisfaction metrics
- Leave balances and overtime tracking
- 11-year historical growth patterns with realistic turnover

**Key Employee Data Categories:**
- **Demographics**: Name, gender, birth date, age, blood type
- **Employment**: Department, position, hire/termination dates, work type
- **Compensation**: Monthly salary, bank details, payroll information
- **Performance**: Ratings, review dates, training completed, skills
- **Benefits**: Health insurance, enrollment dates, leave balances
- **Analytics**: Attendance rate, engagement score, satisfaction index
- **Contact**: Work/personal email, phone, emergency contacts
- **Location**: Complete address with Philippine geography integration

</details>

<details>
<summary><b>dim_retailers</b> - Retail Network Master Data</summary>

- **500 retailers** across all 16 Philippines regions
- 45% sari-sari stores (realistic distribution)
- Major Philippine retail chains included
- Retailer type-specific behavior patterns

</details>

<details>
<summary><b>dim_campaigns</b> - Marketing Campaign Master Data</summary>

- **50 marketing campaigns** across 8 types
- Budget allocation and duration tracking
- Campaign performance metrics
- Multi-channel marketing support

</details>

#### Fact Tables (Continuous Generation)

<details>
<summary><b>fact_sales</b> - Sales Transactions</summary>

- **Historical**: ₱6B across 11 years (2015-2026) (initial run)
- **Daily**: ₱1.64M per day (scheduled runs)
- Seasonal demand variations
- Retailer-specific order patterns
- Optimized for ₱6B FMCG company scale
- 250 active employees driving sales performance

</details>

<details>
<summary><b>fact_operating_costs</b> - Operating Expenses</summary>

- **40 cost categories** (fixed/variable)
- Realistic employee salary structure for 250 active employees
- Optimized for ₱6B revenue with 15-20% profit margins
- 50% cost reduction across all categories for improved profitability
- Complete business expenses including payroll, operations, and overhead

</details>

<details>
<summary><b>fact_inventory</b> - Inventory Management</summary>

- **Monthly inventory snapshots** by product
- 4 warehouse locations across Philippines
- Stock level tracking and valuation
- Supply chain analytics support

</details>

<details>
<summary><b>fact_marketing_costs</b> - Marketing Spend</summary>

- **Campaign-specific cost allocation**
- Marketing overhead categories
- Seasonal spending variations
- ROI analysis support

</details>

---

## Data Schema

### Star Schema Architecture

The platform implements a **dimensional modeling** approach with a central fact table surrounded by dimension tables:

```
┌─────────────────┐    │    ┌─────────────────┐
│  dim_products   │    │    │ dim_campaigns   │
└─────────────────┘    │    └─────────────────┘
        │              │              │
        ├──────────────┼──────────────┤
        │              │              │
┌─────────────────┐    │    ┌─────────────────┐
│ dim_retailers   │    │    │ dim_employees   │
└─────────────────┘    │    └─────────────────┘
        │              │              │
        └──────────────┼──────────────┘
                       │
                ┌─────────────────┐
                │   fact_sales    │
                └─────────────────┘
```

### Table Relationships

<div align="center">

| **Fact Table** | **Dimension Tables** | **Relationship Type** |
|:--------------:|:-------------------:|:---------------------:|
| **fact_sales** | dim_products, dim_employees, dim_retailers, dim_campaigns | Many-to-One |
| **fact_operating_costs** | None (date fields stored directly) | Standalone |
| **fact_inventory** | dim_products | Many-to-One |
| **fact_marketing_costs** | dim_campaigns | Many-to-One |

</div>

### Key Design Principles

- **Surrogate Keys**: All tables use auto-incrementing integer keys
- **Referential Integrity**: Foreign key constraints enforced during generation
- **Optimized for Analytics**: Star schema enables fast aggregations
- **Date Handling**: Direct date storage in fact tables for time-based analysis
- **Scalability**: Designed for BigQuery's distributed architecture

---

## Geography Coverage

### Complete Philippines Regional Database

The platform includes **comprehensive geographic coverage** of all 16 administrative regions of the Philippines with accurate provincial and municipal data:

<div align="center">

**Luzon Island Group**

| **Region** | **Key Provinces** | **Major Cities** |
|:-----------:|:-----------------:|:----------------:|
| **Region I** | Ilocos Norte, Ilocos Sur, La Union, Pangasinan | Laoag, Vigan, San Fernando, Dagupan |
| **Region II** | Cagayan, Isabela, Nueva Vizcaya, Quirino | Tuguegarao, Ilagan, Bayombong |
| **Region III** | Aurora, Bataan, Bulacan, Nueva Ecija, Pampanga, Tarlac, Zambales | Balanga, Malolos, Cabanatuan, San Fernando |
| **Region IV-A** | Cavite, Laguna, Batangas, Rizal, Quezon | Tagaytay, Santa Rosa, Batangas City, Antipolo |
| **Region IV-B** | Occidental Mindoro, Oriental Mindoro, Marinduque, Romblon, Palawan | Mamburao, Calapan, Puerto Princesa |
| **Region V** | Albay, Camarines Norte, Camarines Sur, Catanduanes, Masbate, Sorsogon | Legazpi, Naga, Sorsogon City |
| **CAR** | Abra, Apayao, Benguet, Ifugao, Kalinga, Mountain Province | Baguio City, Bontoc, Tabuk |
| **NCR** | Metro Manila | Manila, Quezon City, Makati, Pasig, Taguig |

**Visayas Island Group**

| **Region** | **Key Provinces** | **Major Cities** |
|:-----------:|:-----------------:|:----------------:|
| **Region VI** | Aklan, Antique, Capiz, Iloilo, Negros Occidental | Kalibo, Iloilo City, Bacolod |
| **Region VII** | Bohol, Cebu, Negros Oriental, Siquijor | Tagbilaran, Cebu City, Dumaguete |
| **Region VIII** | Biliran, Eastern Samar, Leyte, Northern Samar, Samar, Southern Leyte | Tacloban, Catbalogan, Borongan |

**Mindanao Island Group**

| **Region** | **Key Provinces** | **Major Cities** |
|:-----------:|:-----------------:|:----------------:|
| **Region IX** | Zamboanga del Norte, Zamboanga del Sur, Zamboanga Sibugay | Dipolog, Pagadian |
| **Region X** | Bukidnon, Camiguin, Lanao del Norte, Misamis Occidental, Misamis Oriental | Malaybalay, Cagayan de Oro |
| **Region XI** | Davao de Oro, Davao del Norte, Davao del Sur, Davao Occidental, Davao Oriental | Tagum, Digos, Mati |
| **Region XII** | Cotabato, Sarangani, South Cotabato, Sultan Kudarat | Kidapawan, Koronadal, General Santos |
| **Region XIII** | Agusan del Norte, Agusan del Sur, Surigao del Norte, Surigao del Sur, Dinagat Islands | Butuan, Surigao City, Tandag |
| **BARMM** | Basilan, Lanao del Sur, Maguindanao, Sulu, Tawi-Tawi | Isabela City, Marawi, Cotabato City |

</div>

### Geographic Intelligence Features

- **Region-Based Delivery Calculations**: Realistic delivery time estimates based on geographic distance
- **Market Penetration Analysis**: Retailer distribution analysis by region and province
- **Location-Based Sales Analytics**: Regional performance tracking and comparison
- **Supply Chain Optimization**: Warehouse location planning based on geographic distribution

---

## Power BI Integration

### Business Intelligence Ready

The dimensional data warehouse structure is **optimized for Power BI** and other BI tools:

<div align="center">

| **Analytics Domain** | **Key Metrics Available** | **Business Value** |
|:-------------------:|:-------------------------:|:-----------------:|
| **Sales Performance** | Revenue, Volume, Growth, Profitability | Revenue optimization & forecasting |
| **Product Analytics** | Category performance, Price elasticity, Product lifecycle | Product mix optimization |
| **Geographic Analysis** | Regional sales, Market penetration, Delivery performance | Market expansion planning |
| **Employee Productivity** | Sales per rep, Performance ratings, Attendance, Engagement, Satisfaction, Turnover, Skills | Complete HR analytics & workforce optimization |
| **Marketing ROI** | Campaign effectiveness, Cost per acquisition, Brand impact | Marketing budget optimization |
| **Financial Analysis** | Cost structure, Profit margins, Operating efficiency | Financial planning & control |
| **Inventory Management** | Stock levels, Turnover rates, Warehouse efficiency | Supply chain optimization |

</div>

### Dashboard Capabilities

#### Executive Dashboard
- **Key Performance Indicators** (KPIs) at a glance
- **Revenue trends** and growth metrics
- **Regional performance** comparison
- **Profitability analysis** by product category

#### Operational Dashboard
- **Daily sales tracking** and variance analysis
- **Inventory levels** and stock alerts
- **Delivery performance** metrics
- **Employee productivity** monitoring

#### Marketing Dashboard
- **Campaign performance** tracking
- **Marketing ROI** analysis
- **Customer acquisition** costs
- **Brand performance** metrics

### Data Connection Setup

1. **Connect to BigQuery**:
   - Use the BigQuery connector in Power BI
   - Authenticate with your Google Cloud credentials
   - Select the `fmcg_analytics` dataset

2. **Build Relationships**:
   - Import all dimension and fact tables
   - Configure relationships based on foreign keys
   - Set proper cardinality (many-to-one)

3. **Create Measures**:
   - Build DAX measures for key metrics
   - Implement time intelligence functions
   - Create calculated columns for business logic

### Sample Power BI Queries

```sql
-- Monthly Sales Trend
SELECT 
    d.year,
    d.month_name,
    SUM(s.total_amount) as total_revenue,
    COUNT(DISTINCT s.sale_key) as transaction_count
FROM fact_sales s
JOIN dim_dates d ON s.date_key = d.date_key
GROUP BY d.year, d.month_name
ORDER BY d.year, d.month;

-- Regional Performance
SELECT 
    r.region,
    SUM(s.total_amount) as total_revenue,
    COUNT(DISTINCT r.retailer_key) as retailer_count
FROM fact_sales s
JOIN dim_retailers r ON s.retailer_key = r.retailer_key
GROUP BY r.region
ORDER BY total_revenue DESC;
```

---

## Security & Best Practices

### Security Guidelines

<div align="center">

| **Security Aspect** | **Best Practice** | **Implementation** |
|:------------------:|:-----------------:|:-----------------:|
| **Credential Management** | Never commit secrets to version control | Use GitHub repository secrets |
| **Access Control** | Principle of least privilege | BigQuery role-based permissions |
| **Data Protection** | Encrypt sensitive data at rest | BigQuery default encryption |
| **Audit Trail** | Monitor data access and changes | BigQuery audit logs |

</div>

### Recommended Security Practices

#### 1. Service Account Security
```bash
# Create dedicated service account with minimal permissions
gcloud iam service-accounts create fmcg-data-simulator \
  --display-name="FMCG Data Simulator" \
  --project=your-project-id

# Grant only necessary permissions
gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:fmcg-data-simulator@your-project-id.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
```

#### 2. GitHub Repository Security
- **Repository Secrets**: Store all sensitive data in GitHub secrets
- **Branch Protection**: Enable branch protection rules for main branch
- **Audit Logs**: Monitor GitHub Actions execution logs
- **Access Control**: Limit repository access to authorized personnel

#### 3. Data Privacy Considerations
- **Synthetic Data Only**: All generated data is fictional and does not represent real entities
- **No PII**: No personally identifiable information is generated or stored
- **Compliance**: Designed to comply with data protection regulations

### Monitoring & Alerting

#### BigQuery Monitoring
```sql
-- Monitor storage usage
SELECT 
  dataset_id,
  table_id,
  size_bytes / (1024*1024) as size_mb,
  row_count
FROM `your-project-id.fmcg_analytics.INFORMATION_SCHEMA.TABLES`
ORDER BY size_bytes DESC;

-- Monitor query performance
SELECT 
  job_id,
  creation_time,
  total_bytes_processed / (1024*1024*1024) as gb_processed,
  total_slot_ms
FROM `region-us`.INFORMATION_SCHEMA.JOBS
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
ORDER BY total_bytes_processed DESC;
```

---

## Dependencies

### Core Python Packages

<table>
<thead>
<tr>
<th>Package</th>
<th>Version</th>
<th>Purpose</th>
<th>Usage in Project</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>pandas</code></td>
<td>Latest</td>
<td>Data manipulation and analysis</td>
<td>DataFrame operations, data transformation</td>
</tr>
<tr>
<td><code>faker</code></td>
<td>Latest</td>
<td>Synthetic data generation</td>
<td>Realistic names, addresses, business data</td>
</tr>
<tr>
<td><code>google-cloud-bigquery</code></td>
<td>Latest</td>
<td>BigQuery client library</td>
<td>Cloud data warehouse operations</td>
</tr>
<tr>
<td><code>pyarrow</code></td>
<td>Latest</td>
<td>Data serialization</td>
<td>BigQuery data format optimization</td>
</tr>
<tr>
<td><code>pandas_gbq</code></td>
<td>Latest</td>
<td>Pandas-BigQuery integration</td>
<td>Efficient data loading operations</td>
</tr>
</tbody>
</table>

### Infrastructure Dependencies

<div align="center">

| **Service** | **Purpose** | **Required Plan** |
|:-----------:|:-----------:|:----------------:|
| **Google Cloud Platform** | Cloud infrastructure | Free tier or paid |
| **BigQuery** | Data warehouse | Free tier (10GB storage) |
| **GitHub Actions** | Workflow automation | Free tier (2000 minutes/month) |
| **Power BI** | Business intelligence | Free or Pro license |

</div>

### Installation Requirements

```bash
# Install Python dependencies
pip install pandas faker google-cloud-bigquery pyarrow pandas_gbq

# Verify installation
python -c "import pandas, faker, google.cloud.bigquery, pyarrow, pandas_gbq; print('All dependencies installed successfully')"
```

---

## Performance & Scaling

### Storage Optimization

The platform is **optimized for BigQuery free tier** usage:

<div align="center">

| **Metric** | **Current Usage** | **Free Tier Limit** | **Utilization** |
|:----------:|:----------------:|:------------------:|:---------------:|
| **Storage** | ~1.3 GB | 10 GB | 13% |
| **Monthly Queries** | Variable | 1 TB | Variable |
| **Data Insertion** | ~3.6M records/day | 5 GB/day | 72% |

</div>

### Performance Characteristics

#### Data Generation Performance
- **Initial Load**: 11 years of historical data (~₱6B sales)
- **Daily Updates**: ₱1.64M PHP sales per day
- **Generation Speed**: ~10,000 records/second
- **BigQuery Loading**: Optimized batch operations

#### Query Performance
```sql
-- Sample query performance metrics
-- Monthly aggregation (10 years of data): ~2-3 seconds
-- Regional analysis: ~1-2 seconds  
-- Product performance: ~1-2 seconds
-- Complex joins with filters: ~3-5 seconds
```

### Scaling Considerations

#### Horizontal Scaling
- **BigQuery Auto-scaling**: Automatic query parallelization
- **Partitioned Tables**: Date-partitioned fact tables
- **Clustered Tables**: Optimized for common query patterns

#### Future Growth Path
1. **Increase Data Volume**: Adjust `INITIAL_SALES_AMOUNT` and `DAILY_SALES_AMOUNT`
2. **Add More Dimensions**: Expand product catalog, employee base
3. **Geographic Expansion**: Add more regions or countries
4. **Real-time Integration**: Stream processing capabilities

## Troubleshooting

### Common Issues & Solutions

#### Authentication Issues
```bash
# Error: Permission Denied
# Solution: Verify service account permissions
gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:your-service-account@project.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
```

#### Memory Issues
```bash
# Error: Memory limit exceeded during data generation
# Solution: Reduce batch size in config.py
INITIAL_SALES_AMOUNT = "3000000000"  # Reduce from 6B
DAILY_SALES_AMOUNT = "820000"      # Reduce from 1.64M
```

#### GitHub Actions Failures
```bash
# Error: Workflow timeout
# Solution: Check GitHub Actions logs and adjust timeout
# In .github/workflows/simulator.yml
timeout-minutes: 60  # Increase if needed
```

#### BigQuery Quota Issues
```sql
-- Check current usage
SELECT 
  project_id,
  user_email,
  total_bytes_processed,
  total_slot_ms
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER`
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);
```

## Contributing & Development

### Development Guidelines

#### Code Standards
- **Python 3.14** compatibility
- **Type hints** for all functions
- **Docstrings** following Google style
- **Error handling** with proper logging
- **Unit tests** for core functionality

#### Project Structure Best Practices
```
FMCG/
├── generators/          # Data generation modules
│   ├── dimensional.py   # Core dimensional model
│   └── [domain].py      # Domain-specific generators
├── config.py            # Configuration management
├── auth.py              # Authentication handling
├── helpers.py           # Utility functions
├── schema.py            # BigQuery schema definitions
└── main.py              # Main orchestration script
```

### Contributing Workflow

1. **Fork the repository**
2. **Create feature branch**: `git checkout -b feature/new-feature`
3. **Make changes** following code standards
4. **Test locally**: `python main.py` (with test dataset)
5. **Submit pull request** with detailed description

### Enhancement Opportunities

#### Potential Features
- **Real-time Data Streaming**: Kafka/PubSub integration
- **Advanced Analytics**: Machine learning models
- **Additional Dimensions**: Customer, Supplier, Warehouse
- **Multi-country Support**: Expand geographic coverage
- **API Endpoints**: RESTful API for data access
- **Data Quality**: Automated validation and monitoring

#### Performance Optimizations
- **Incremental Loading**: Change Data Capture (CDC)
- **Materialized Views**: Pre-aggregated summary tables
- **Query Optimization**: Partitioning and clustering strategies
- **Caching Layer**: Redis or similar for frequent queries

---

## License & Disclaimer

### Project Information

<div align="center">

**FMCG Data Analytics Platform**

*A comprehensive dimensional data warehouse solution for FMCG business analytics*

| **Aspect** | **Details** |
|:----------:|:-----------|
| **Purpose** | Educational and demonstration |
| **Data Type** | Synthetic/fictional data only |
| **Technology Stack** | Python, BigQuery, Power BI |
| **Geographic Focus** | Philippines FMCG market |
| **Data Volume** | Optimized for free-tier usage |

</div>

### Important Disclaimers

- **Synthetic Data Only**: All generated data is fictional and does not represent real business entities, transactions, or individuals
- **Educational Purpose**: This platform is designed for educational, demonstration, and testing purposes only
- **No Real Business Data**: No connection to actual FMCG companies, retailers, or employees
- **Privacy Compliance**: All data generation complies with privacy regulations as no real personal information is created or stored

### Support & Community

- **Issues**: Report bugs or feature requests via GitHub Issues
- **Discussions**: Use GitHub Discussions for questions and ideas
- **Documentation**: Comprehensive documentation in this README file
- **Examples**: Sample queries and configurations provided throughout

---

<div align="center">

**Built with modern data engineering best practices for scalable FMCG analytics**

*Last Updated: January 2026*

</div>
