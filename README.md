# FMCG Data Analytics Platform

A modern, scalable data analytics platform for Fast-Moving Consumer Goods (FMCG) businesses.

## Overview

This platform provides comprehensive data warehousing and analytics capabilities for FMCG companies, including:
- Sales performance tracking
- Inventory management
- Employee analytics
- Marketing campaign analysis
- Geographic distribution insights

## Features

- **Modern Architecture**: Clean, modular design with separation of concerns
- **Scalable Data Warehouse**: Built for BigQuery with optimized schemas
- **Automated ETL**: Scheduled data generation and updates
- **Comprehensive Analytics**: 360-degree view of business operations
- **Geographic Intelligence**: Philippines-wide coverage with regional insights

## Project Structure

```
FMCG-Data-Analytics/
├── src/                    # Source code
│   ├── core/              # Core business logic
│   ├── data/              # Data models and schemas
│   ├── etl/               # ETL pipelines
│   └── utils/             # Utility functions
├── tests/                 # Test suite
├── docs/                  # Documentation
├── config/                # Configuration files
└── scripts/               # Deployment and utility scripts
```

## Quick Start

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Configure environment variables
4. Run initial setup: `python scripts/setup.py`
5. Start data generation: `python src/main.py`

## Documentation

See the `docs/` directory for detailed documentation on:
- Architecture overview
- Data schemas
- API reference
- Deployment guide

## License

MIT License - see LICENSE file for details.
