# FMCG Data Analytics Platform

**A comprehensive normalized dimensional data warehouse solution for Fast-Moving Consumer Goods analytics with automated ETL pipelines and realistic business simulation**

[![Python](https://img.shields.io/badge/Python-3.14-blue.svg)](https://www.python.org/downloads/)
[![BigQuery](https://img.shields.io/badge/Google%20Cloud-BigQuery-orange.svg)](https://cloud.google.com/bigquery)
[![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-Automated-brightgreen.svg)](https://github.com/features/actions)

*Normalized Dimensional Modeling • Synthetic Data Generation • Automated ETL • Geographic Intelligence • Chronological ID Sequencing*

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Data Model](#data-model)
- [Automated Workflows](#automated-workflows)
- [Project Structure](#project-structure)
- [Installation & Setup](#installation--setup)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Quality](#data-quality)
- [Performance & Optimization](#performance--optimization)
- [Security](#security)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

The FMCG Data Analytics Platform is a **complete data warehousing solution** designed specifically for Fast-Moving Consumer Goods businesses. It implements a **normalized star schema architecture** with realistic synthetic data generation, automated ETL pipelines, and comprehensive business intelligence capabilities.

### What This Platform Does

- **Generates realistic FMCG business data** across all major domains
- **Maintains chronological ID ordering** (Employee 1 = earliest hire, Product 1 = earliest launch)
- **Automates data updates** with separate daily, monthly, and quarterly workflows
- **Optimizes for BigQuery free tier** while maintaining enterprise-grade functionality
- **Provides complete geographic coverage** of the Philippines with regional intelligence

### Business Domains Covered

| **Domain** | **Key Metrics** | **Analytics Value** |
|:----------:|:---------------:|:-------------------:|
| **Sales** | Revenue, Volume, Commission, Order Status | Performance tracking & forecasting |
| **Products** | Pricing, Categories, Launch Dates | Product mix analysis & optimization |
| **Employees** | Hire Dates, Compensation, Tenure | Complete workforce planning |
| **Retailers** | Geographic distribution, Types | Market penetration analysis |
| **Inventory** | Stock levels, Locations, Valuation | Supply chain optimization |
| **Marketing** | Campaign ROI, Spend, Effectiveness | Marketing effectiveness |
| **Operations** | Cost structure, Department allocation | Financial planning & analysis |

---

## Key Features

### Data Generation Excellence

- **Chronological ID Sequencing**: All entities maintain proper date-based ordering
  - Employee IDs assigned by hire date (earliest hire = Employee 1)
  - Product IDs assigned by launch date (earliest launch = Product 1)
  - Campaign IDs assigned by start date (earliest campaign = Campaign 1)
  
- **Realistic Business Logic**: Data generation follows real-world patterns
  - Company founding date: 2015-01-01
  - Employee tenure spans from hire date to termination/current date
  - Order progression: Pending → Shipped → Delivered
  - Geographic distribution based on Philippines administrative boundaries

- **Philippine Economic Scenario Integration**: Revenue and cost fluctuations based on actual PSA data
  - **TRAIN Law Impact** (2018): 6.5% inflation spike with 2-8% excise tax effects
  - **COVID-19 Pandemic** (2020-2022): Volume and pricing volatility across ECQ, GCQ, and lockdown periods
  - **Inflation Cycles** (2015-2026): Actual Philippine inflation rates from 1.5% (2015) to 8% peak (2023)
  - **Seasonal Patterns**: Ber months demand surge, lean months dip (Jun-Aug)
  - **Independent Price/Cost Movements**: Realistic gross margin fluctuations

### Automated Workflows

- **Daily Sales Generation**: 99-148 transactions with order status updates
- **Monthly Business Updates**: 1-3 new employees, 1-2 new products, operating costs, inventory
- **Quarterly Campaign Management**: 1 new campaign per quarter with comprehensive marketing costs

### Data Quality & Integrity

- **Temporal Accuracy**: All dates respect business logic and chronological constraints
- **Referential Integrity**: Foreign key relationships maintained across all tables
- **Duplicate Prevention**: ID-based filtering prevents data duplication
- **Consistent Sequencing**: No gaps or duplicates in ID sequences

---

## Architecture

### Modern Design Principles

| **Component** | **Technology** | **Purpose** |
|:--------------:|:--------------:|:------------:|
| **Data Warehouse** | Google BigQuery | Scalable cloud storage with SQL analytics |
| **ETL Pipeline** | Python + Pandas | Automated data generation and loading |
| **Orchestration** | GitHub Actions | Scheduled workflows with proper separation |
| **Data Model** | **Normalized Star Schema** | Optimized for query performance & storage |

### Technology Stack

- **Data Warehouse**: Google BigQuery with optimized schema design
- **Processing Engine**: Python with pandas for efficient data manipulation
- **Workflow Automation**: GitHub Actions with scheduled executions
- **Data Generation**: Faker library with realistic business logic
- **Cloud Integration**: Native Google Cloud Platform integration

---

## Data Model

### Normalized Star Schema Architecture

The platform implements a **normalized dimensional modeling** approach with optimized relationships:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ dim_products    │    │ dim_campaigns   │    │ dim_retailers   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              │
                ┌─────────────────┐
                │   fact_sales    │
                │ fact_employees  │
                │ fact_inventory  │
                │ fact_operating_ │
                │ fact_marketing_ │
                └─────────────────┘
```

### Dimension Tables

| **Table** | **Purpose** | **Key Features** |
|:----------:|:-----------:|:----------------:|
| **dim_employees** | Workforce master data | Hire dates, compensation, work setup |
| **dim_products** | Product catalog | Launch dates, pricing, categories |
| **dim_campaigns** | Marketing campaigns | Budget, duration, effectiveness |
| **dim_retailers** | Distribution network | Geographic distribution, types |
| **dim_locations** | Geographic hierarchy | Philippines regions, provinces, cities |
| **dim_departments** | Organizational structure | Cost centers, hierarchies |
| **dim_jobs** | Position definitions | Salary ranges, career progression |

### Fact Tables

| **Table** | **Purpose** | **Update Frequency** |
|:----------:|:-----------:|:-------------------:|
| **fact_sales** | Sales transactions | Daily (99-148 transactions) |
| **fact_employees** | Employee compensation | Monthly (full tenure coverage) |
| **fact_inventory** | Stock levels | Monthly (snapshots) |
| **fact_operating_costs** | Business expenses | Monthly (per department) |
| **fact_marketing_costs** | Marketing spend | Quarterly (per campaign) |

---

## Automated Workflows

### Daily Operations (Every Day)

**Sales Generation with Order Status Progression**
- **Volume**: 99-148 transactions per day
- **Order Status**: Pending → Shipped → Delivered progression
- **Geographic Distribution**: Across all Philippines regions
- **Campaign Attribution**: Latest campaigns prioritized
- **ID Sequencing**: Continues from existing max sale_id

### Monthly Updates (Last Day of Month)

**Business Growth Simulation**
- **New Employees**: 1-3 hires per month with chronological IDs
- **New Products**: 1-2 launches per month with chronological IDs
- **Operating Costs**: Monthly expenses per department
- **Inventory Snapshots**: End-of-month stock levels

### Quarterly Campaigns (1st Day of Quarter)

**Marketing Strategy Execution**
- **New Campaigns**: 1 major campaign per quarter
- **Marketing Costs**: Comprehensive expense tracking
- **Performance Analytics**: ROI measurement and assessment

---

## Project Structure

```
FMCG-Data-Analytics/
├── src/                          # Core application source code
│   ├── core/                    # Business logic and data generators
│   │   ├── generators.py        # Data generation engines
│   │   └── location_data.py     # Geographic data models
│   ├── data/                    # Data models and schemas
│   │   └── schemas.py           # BigQuery table definitions
│   ├── etl/                     # ETL pipeline components
│   │   └── pipeline.py          # Main data processing engine
│   └── utils/                   # Utility functions and helpers
│       ├── bigquery_client.py   # BigQuery integration
│       ├── id_generation.py     # Unique ID management
│       └── logger.py            # Logging framework
├── scripts/                     # Operational scripts
│   ├── setup.py                 # Initial environment setup
│   ├── monthly_update.py        # Monthly data generation
│   └── quarterly_campaign_update.py  # Campaign management
├── .github/workflows/           # CI/CD pipeline definitions
│   ├── daily-sales.yml          # Daily sales automation
│   ├── monthly-update.yml       # Monthly business updates
│   └── quarterly-update.yml     # Quarterly campaign management
├── docs/                        # Comprehensive documentation
├── tests/                       # Quality assurance suite
└── requirements.txt             # Python dependencies
```

### Key Components

- **`src/etl/pipeline.py`**: Main ETL orchestration with all generation methods
- **`src/core/generators.py`**: Core data generation engines with business logic
- **`scripts/monthly_update.py`**: Monthly business growth automation
- **`scripts/quarterly_campaign_update.py`**: Quarterly campaign management
- **`.github/workflows/`**: Automated execution schedules

---

## Installation & Setup

### Prerequisites

| **Requirement** | **Version/Details** | **Purpose** |
|:---------------:|:-------------------:|:-----------:|
| **Python** | 3.14 or higher | ETL pipeline execution |
| **Google Cloud Platform** | Active GCP account | Cloud data warehouse |
| **BigQuery Dataset** | Created in your GCP project | Data storage and analytics |
| **Service Account** | BigQuery Admin permissions | Data access and management |

### Quick Start Guide

1. **Repository Setup**
   ```bash
   git clone https://github.com/your-org/FMCG-Data-Analytics.git
   cd FMCG-Data-Analytics
   ```

2. **Environment Configuration**
   ```bash
   pip install -r requirements.txt
   ```

3. **Google Cloud Authentication**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
   export GCP_PROJECT_ID="your-project-id"
   export BQ_DATASET="fmcg_warehouse"
   ```

4. **Initial Database Setup**
   ```bash
   python scripts/setup.py
   ```

5. **Full Data Generation**
   ```bash
   python src/main.py
   ```

---

## Configuration

### Environment Variables

| **Variable** | **Description** | **Default Value** |
|:------------:|:---------------:|:-----------------:|
| `GCP_PROJECT_ID` | Google Cloud Project ID | `fmcg-data-generator` |
| `BQ_DATASET` | BigQuery Dataset Name | `fmcg_warehouse` |
| `INITIAL_SALES_AMOUNT` | Initial sales volume target | `8000000000` |
| `DAILY_SALES_AMOUNT` | Daily sales generation target | `2000000` |

### Customization Options

The platform supports extensive customization:

- **Data Volume Control**: Adjust transaction volumes and generation frequencies
- **Geographic Scope**: Configure regional coverage and distribution patterns
- **Business Logic**: Customize pricing, cost structures, and employee parameters
- **Workflow Scheduling**: Modify automation schedules and update frequencies

---

## Usage

### Execution Modes

#### Manual Initial Run
```bash
python src/main.py
```
Generates complete historical dataset from 2015 to present.

#### Manual Workflow Execution

The platform supports manual execution through GitHub Actions for testing and on-demand data generation:

**Available Manual Runs:**

| **Run Type** | **Purpose** | **What It Does** | **When to Use** |
|:------------:|:-----------:|:---------------:|:--------------:|
| **Full** | Complete data generation | All dimensions + All fact tables (historical from 2015) | Initial setup or complete refresh |
| **Daily** | Sales data only | New sales transactions (99-148) with order status updates | Testing sales generation or manual sales refresh |
| **Monthly** | Business growth simulation | New employees (1-3), New products (1-2), Operating costs, Inventory | Testing monthly updates or manual business growth |
| **Quarterly** | Campaign management | New campaign (1) and Marketing costs | Testing quarterly campaigns or manual campaign refresh |

**How to Trigger Manual Runs:**

1. Go to your repository's **Actions** tab
2. Select **Manual Data Generation** workflow
3. Click **Run workflow**
4. Choose the **Run Type** you need
5. Adjust optional parameters (sales amounts, employee/product counts)
6. Click **Run workflow** to execute

**Manual Run Parameters:**
- **Initial Sales Amount**: Override default ₱8B target (for full runs)
- **Daily Sales Amount**: Override default ₱2M daily target
- **New Employees Count**: Override default 1-3 employees (for monthly runs)
- **New Products Count**: Override default 1-2 products (for monthly runs)

#### Automated Scheduled Runs
- **Daily**: GitHub Actions generates incremental sales data
- **Monthly**: Automated business growth simulation
- **Quarterly**: Strategic campaign creation and cost tracking

### Data Generation Process

1. **Dimension Tables**: Generate master data (employees, products, retailers, etc.)
2. **Historical Facts**: Create 10+ years of historical data
3. **Incremental Updates**: Daily, monthly, and quarterly automated updates
4. **Quality Assurance**: ID sequencing and data integrity validation

---

## Economic Modeling

### Philippine Economic Scenario (2015-2026)

The platform incorporates **actual Philippine economic data** from the Philippine Statistics Authority (PSA) to create realistic revenue and cost fluctuations.

#### Price Inflation (Sales Revenue)

| **Period** | **Annual Rate** | **Key Events** |
|:----------:|:---------------:|:--------------:|
| 2015-2016 | 1.5% | Low inflation period |
| 2017 | 3.0% | Pre-TRAIN Law baseline |
| **2018** | **6.5%** | **TRAIN Law implementation spike** |
| 2019 | 3.0% | Post-TRAIN stabilization |
| 2020 | 2.5% | Pandemic-induced deflation |
| 2021 | 4.0% | Economic recovery |
| 2022 | 6.0% | Global inflation surge |
| **2023** | **8.0%** | **Peak inflation** |
| 2024 | 4.0% | Moderating inflation |
| 2025+ | 2.5% | Stabilization phase |

#### Cost Inflation (Inventory/COGS)

Cost inflation runs slightly lower than retail prices (0.5-1% differential), creating realistic gross margin compression and expansion cycles.

#### TRAIN Law Impact (January 2018)

- **Direct Price Impact**: Additional 2-8% increase from excise taxes on sweetened beverages, fuel, and other FMCG products
- **Input Cost Impact**: 5.5% cost inflation on raw materials and supplies
- **Margin Effect**: Temporary margin expansion as retail prices rose faster than costs

#### COVID-19 Pandemic Effects

**Volume Impact:**
- **Pre-pandemic** (Jan 2015 - Feb 2020): Normal transaction volumes (1.0x)
- **ECQ Period** (Mar-May 2020): Severe drop to 50-60% of normal volumes
- **GCQ/MGCQ** (Jun 2020 - Mar 2021): Moderate impact at 70-80% volumes
- **Various Lockdowns** (Apr 2021 - Feb 2022): Recovery phase at 90-105% volumes
- **Alert Levels/Endemic** (Mar 2022+): Return to normal with growth (100-110%)

**Pricing Impact:**
- **ECQ Period**: +8% to +18% price surge due to supply chain disruptions
- **GCQ/MGCQ**: +3% to +10% elevated pricing
- **Lockdown Period**: +2% to +6% moderate premium
- **Endemic Phase**: Normalizing to -1% to +3%

#### Seasonal Patterns

- **Ber Months** (Oct-Dec): +2% to +6% demand increase (Christmas season)
- **Lean Months** (Jun-Aug): -6% to -2% demand decrease (back-to-school, summer)
- **Regular Months**: ±4% normal variation

#### Gross Margin Dynamics

The independent movement of prices and costs creates realistic margin fluctuations:
- **Margin Expansion**: 2018 (TRAIN Law), 2020 (pandemic pricing power)
- **Margin Compression**: 2023 (costs rose 7.5% vs prices 8%)
- **Competitive Pressure**: ±8% random variation simulating market competition
- **Forex Impact**: ±5% variation from PHP peso fluctuations on imported goods

---

## Data Quality

### Chronological ID Sequencing

All entities maintain strict chronological ID ordering:

- **Employee IDs**: Assigned by hire date (earliest hire = Employee 1)
- **Product IDs**: Assigned by launch date (earliest launch = Product 1)
- **Campaign IDs**: Assigned by start date (earliest campaign = Campaign 1)
- **Sales IDs**: Sequential assignment with no gaps or duplicates

### Data Consistency

- **Temporal Accuracy**: All dates respect business logic and chronological constraints
- **Referential Integrity**: Foreign key relationships maintained across all tables
- **Business Rules**: Pricing, costs, and quantities follow realistic business patterns
- **Geographic Logic**: Location hierarchies respect administrative boundaries

---

## Performance & Optimization

### Free Tier Optimization

The platform is designed to operate efficiently within BigQuery free tier limits:

- **Daily Operations**: Minimal data generation (99-148 transactions)
- **Monthly Updates**: Controlled expansion with cost-conscious design
- **Quarterly Campaigns**: Strategic marketing data generation
- **Efficient Queries**: Optimized for slot consumption and cost management

### Optimization Strategies

- **Batch Processing**: Efficient bulk operations for large datasets
- **Query Optimization**: BigQuery best practices for cost-effective queries
- **Incremental Updates**: Targeted data updates to minimize processing overhead
- **Caching Layer**: Intelligent caching for frequently accessed reference data

---

## Security

### Data Governance

- **Access Control**: Role-based permissions and data access policies
- **Data Encryption**: End-to-end encryption for data in transit and at rest
- **Audit Logging**: Comprehensive audit trails for all data operations
- **Privacy Protection**: Compliance with data protection regulations

### Best Practices

- **Secrets Management**: Secure handling of API keys and credentials
- **Network Security**: VPC configuration and firewall rules
- **Data Classification**: Structured data classification and handling procedures
- **Regular Audits**: Periodic security assessments and compliance checks

---

## Contributing

### Development Guidelines

- **Code Standards**: PEP 8 compliance with comprehensive documentation
- **Testing Requirements**: Unit tests for all core functionality
- **Code Review**: Peer review process for all changes
- **Documentation**: Updated documentation for all new features

### Contribution Process

1. Fork the repository
2. Create feature branch with descriptive name
3. Implement changes with appropriate tests
4. Update documentation as needed
5. Submit pull request with detailed description

---

## License

This project is licensed under the MIT License - see the LICENSE file for complete details.

---

*For technical support or inquiries, please refer to the documentation or create an issue in the GitHub repository.*

**Last Updated**: January 16, 2026  
**Version**: 2.1 - Philippine Economic Scenario Integration  
**Status**: Production Ready
