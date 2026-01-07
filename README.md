# FMCG Data Analytics Platform

A sophisticated, enterprise-grade data analytics platform designed specifically for Fast-Moving Consumer Goods (FMCG) businesses seeking comprehensive insights into their operations, sales performance, and market dynamics.

---

## Overview

The FMCG Data Analytics Platform delivers a complete data warehousing and analytics solution that transforms raw business data into actionable intelligence. Built with modern architecture principles and scalable cloud infrastructure, this platform enables FMCG companies to make data-driven decisions with confidence and precision.

### Core Capabilities

- **Sales Performance Analytics**: Comprehensive tracking of sales transactions, revenue trends, and performance metrics across all channels
- **Inventory Management Intelligence**: Real-time monitoring of stock levels, turnover rates, and supply chain optimization
- **Employee Workforce Analytics**: Detailed insights into workforce productivity, compensation patterns, and organizational dynamics
- **Marketing Campaign Effectiveness**: End-to-end tracking of marketing initiatives, ROI analysis, and campaign performance metrics
- **Geographic Distribution Insights**: Philippines-wide coverage with granular regional, provincial, and municipal level analytics
- **Financial Cost Management**: Detailed tracking of operating costs, marketing expenses, and budget optimization

---

## Architecture Highlights

### Modern Design Principles

- **Modular Architecture**: Clean separation of concerns with loosely coupled components
- **Scalable Infrastructure**: Built on Google BigQuery for petabyte-scale data processing
- **Automated Workflows**: Scheduled ETL processes with intelligent data generation
- **Real-time Processing**: Streamlined data pipelines for near real-time analytics
- **Enterprise Security**: Robust data governance and security frameworks

### Technology Stack

- **Data Warehouse**: Google BigQuery with optimized schema design
- **Processing Engine**: Python with pandas for efficient data manipulation
- **Workflow Automation**: GitHub Actions with scheduled executions
- **Data Generation**: Faker library with realistic business logic
- **Cloud Integration**: Native Google Cloud Platform integration

---

## Data Model Architecture

### Dimension Tables

- **Products**: Comprehensive product catalog with pricing, categories, and lifecycle management
- **Employees**: Workforce data with compensation, tenure, and organizational structure
- **Retailers**: Distribution network with geographic and performance metrics
- **Campaigns**: Marketing initiatives with budget tracking and effectiveness metrics
- **Locations**: Geographic hierarchy from national to municipal level
- **Departments**: Organizational structure with cost center allocation
- **Jobs**: Position definitions with salary ranges and career progression

### Fact Tables

- **Sales Transactions**: Detailed sales data with order tracking and delivery status
- **Inventory Levels**: Stock monitoring with valuation and turnover metrics
- **Operating Costs**: Departmental expense tracking with categorization
- **Marketing Costs**: Campaign-specific expense allocation and ROI tracking
- **Employee Compensation**: Detailed payroll and benefits analytics

---

## Automated Data Updates

### Daily Operations

**Sales Generation**: 99-148 transactions per day with realistic order progression
- Order status tracking: Pending → Shipped → Delivered
- Geographic distribution across all regions
- Product and retailer relationship modeling
- Campaign influence attribution

### Monthly Updates

**Business Growth Simulation**: Controlled expansion of core business entities
- **New Employees**: 1-3 hires per month with proper sequencing
- **Product Launches**: 1-2 new products per month
- **Operating Costs**: Monthly expense generation per department
- **Inventory Snapshots**: End-of-month stock level reporting

### Quarterly Campaigns

**Marketing Strategy**: Strategic campaign planning and execution
- **New Campaigns**: 1 major campaign per quarter
- **Marketing Costs**: Comprehensive expense tracking across all channels
- **Performance Analytics**: ROI measurement and effectiveness assessment

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

---

## Installation & Setup

### Prerequisites

- Python 3.14 or higher
- Google Cloud Project with BigQuery API enabled
- Service account with appropriate permissions
- GitHub repository access (for automated workflows)

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

## Configuration Management

### Environment Variables

| Variable | Description | Default Value |
|----------|-------------|--------------|
| `GCP_PROJECT_ID` | Google Cloud Project ID | `fmcg-data-generator` |
| `BQ_DATASET` | BigQuery Dataset Name | `fmcg_warehouse` |
| `INITIAL_SALES_AMOUNT` | Initial sales volume target | `8000000000` |
| `DAILY_SALES_AMOUNT` | Daily sales generation target | `2000000` |

### Customization Options

The platform supports extensive customization through configuration parameters:

- **Data Volume Control**: Adjust transaction volumes and generation frequencies
- **Geographic Scope**: Configure regional coverage and distribution patterns
- **Business Logic**: Customize pricing, cost structures, and employee parameters
- **Workflow Scheduling**: Modify automation schedules and update frequencies

---

## Data Quality & Integrity

### ID Sequencing

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

## Performance & Scalability

### Optimization Strategies

- **Batch Processing**: Efficient bulk operations for large datasets
- **Query Optimization**: BigQuery best practices for cost-effective queries
- **Incremental Updates**: Targeted data updates to minimize processing overhead
- **Caching Layer**: Intelligent caching for frequently accessed reference data

### Free Tier Optimization

The platform is designed to operate efficiently within BigQuery free tier limits:

- **Daily Operations**: Minimal data generation (99-148 transactions)
- **Monthly Updates**: Controlled expansion with cost-conscious design
- **Quarterly Campaigns**: Strategic marketing data generation
- **Efficient Queries**: Optimized for slot consumption and cost management

---

## Monitoring & Maintenance

### Automated Workflows

- **Daily Sales**: Automatic sales data generation and order status updates
- **Monthly Updates**: Business growth simulation and data refresh
- **Quarterly Campaigns**: Marketing campaign creation and cost tracking
- **Data Validation**: Automated quality checks and integrity verification

### Logging & Debugging

Comprehensive logging framework provides detailed insights into:
- Data generation processes and volumes
- Error handling and recovery procedures
- Performance metrics and execution times
- Data quality validation results

---

## Business Intelligence & Analytics

### Key Performance Indicators

The platform enables tracking of critical FMCG metrics:

- **Sales Performance**: Revenue trends, product performance, regional analysis
- **Inventory Efficiency**: Turnover rates, stock levels, supply chain metrics
- **Employee Productivity**: Compensation analysis, tenure patterns, workforce metrics
- **Marketing ROI**: Campaign effectiveness, cost per acquisition, conversion rates
- **Geographic Insights**: Regional performance, market penetration, distribution analysis

### Analytics Capabilities

- **Time Series Analysis**: Trend identification and seasonal pattern detection
- **Comparative Analysis**: Period-over-period growth and performance benchmarking
- **Geographic Analytics**: Regional performance mapping and market analysis
- **Product Analytics**: Category performance, pricing optimization, lifecycle management

---

## Security & Compliance

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

## Contributing & Development

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

## Support & Documentation

### Documentation Resources

- **Architecture Guide**: Detailed system architecture and design decisions
- **API Reference**: Complete API documentation with examples
- **Deployment Guide**: Step-by-step deployment instructions
- **Troubleshooting**: Common issues and resolution procedures

### Community Support

- **GitHub Issues**: Bug reports and feature requests
- **Discussion Forums**: Community discussions and best practices
- **Wiki Pages**: Collaborative documentation and guides
- **Code Reviews**: Peer review and code quality discussions

---

## License

This project is licensed under the MIT License - see the LICENSE file for complete details.

---

## Acknowledgments

Built with modern data engineering principles and best practices for the FMCG industry. Special thanks to the open-source community for the tools and libraries that make this platform possible.

---

*For technical support or inquiries, please refer to the documentation or create an issue in the GitHub repository.*
