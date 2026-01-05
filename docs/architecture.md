# Architecture Documentation

## Overview

The FMCG Data Analytics Platform follows a modern, modular architecture designed for scalability, maintainability, and performance.

## Architecture Principles

1. **Separation of Concerns**: Each module has a single responsibility
2. **Modularity**: Components are loosely coupled and highly cohesive
3. **Scalability**: Designed to handle growing data volumes and user loads
4. **Testability**: All components are unit testable
5. **Configuration Management**: Environment-based configuration

## System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Application   │    │   Data Layer    │    │  Infrastructure │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │   Main.py   │ │    │ │   Schemas   │ │    │ │ BigQuery    │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │   Pipeline  │ │◄──►│ │ Generators  │ │    │ │ Cloud Auth  │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │   Setup     │ │    │ │   Models    │ │    │ │ Storage     │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Directory Structure

```
FMCG-Data-Analytics/
├── src/                          # Source code
│   ├── core/                     # Core business logic
│   │   └── generators.py         # Data generation engines
│   ├── data/                     # Data models and schemas
│   │   └── schemas.py            # BigQuery table definitions
│   ├── etl/                      # ETL pipelines
│   │   └── pipeline.py           # Main ETL orchestration
│   ├── utils/                    # Utility functions
│   │   ├── bigquery_client.py    # BigQuery operations
│   │   └── logger.py             # Logging utilities
│   └── main.py                   # Application entry point
├── config/                       # Configuration
│   └── settings.py               # Application settings
├── scripts/                      # Utility scripts
│   └── setup.py                  # Environment setup
├── tests/                        # Test suite
│   └── test_generators.py        # Generator tests
├── docs/                         # Documentation
└── pyproject.toml               # Project configuration
```

## Core Components

### 1. Data Layer (`src/data/`)

**Purpose**: Define data structures and schemas

**Key Components**:
- `schemas.py`: BigQuery table definitions with validation
- Table schema definitions for all dimension and fact tables
- Schema conversion utilities

### 2. Core Business Logic (`src/core/`)

**Purpose**: Implement data generation algorithms

**Key Components**:
- `generators.py`: All data generation classes
- Realistic data generation using Faker library
- Philippines-specific geographic data
- Business logic for relationships between entities

### 3. ETL Pipeline (`src/etl/`)

**Purpose**: Orchestrate data generation and loading

**Key Components**:
- `pipeline.py`: Main ETL orchestration
- Batch processing for large datasets
- Error handling and recovery
- Incremental updates support

### 4. Utilities (`src/utils/`)

**Purpose**: Provide common functionality

**Key Components**:
- `bigquery_client.py`: BigQuery connection and operations
- `logger.py`: Structured logging with Rich formatting

### 5. Configuration (`config/`)

**Purpose**: Manage application settings

**Key Components**:
- `settings.py`: Pydantic-based configuration
- Environment variable support
- Validation and type safety

## Data Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Config    │───►│ Generators  │───►│   Data      │
└─────────────┘    └─────────────┘    └─────────────┘
                           │                   │
                           ▼                   ▼
                    ┌─────────────┐    ┌─────────────┐
                    │   Pipeline  │◄───│ BigQuery    │
                    └─────────────┘    └─────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │   Logs      │
                    └─────────────┘
```

## Data Model Architecture

### Dimensional Modeling Approach

The platform uses a **normalized star schema** architecture:

```
Dimension Tables          Fact Tables
┌─────────────┐         ┌─────────────┐
│ dim_employees│         │ fact_sales  │
│ dim_products │         │ fact_inventory│
│ dim_retailers│         │ fact_operating_│
│ dim_campaigns│         │ fact_marketing_│
│ dim_locations│         └─────────────┘
│ dim_departments│
│ dim_jobs     │
│ dim_banks    │
│ dim_insurance│
│ dim_categories│
│ dim_brands   │
│ dim_subcategories│
└─────────────┘
```

### Normalization Benefits

1. **Reduced Data Redundancy**: Single source of truth for each entity
2. **Improved Data Quality**: Centralized validation and constraints
3. **Better Performance**: Optimized for analytical queries
4. **Easier Maintenance**: Changes only need to be made in one place
5. **Scalability**: Efficient storage and query performance

## Technology Stack

### Core Technologies

- **Python 3.8+**: Primary programming language
- **Google BigQuery**: Cloud data warehouse
- **Pandas**: Data manipulation and analysis
- **Faker**: Realistic synthetic data generation

### Infrastructure

- **Google Cloud Platform**: Cloud infrastructure
- **BigQuery Storage**: Scalable data storage
- **Cloud Authentication**: Secure access management

### Development Tools

- **Pydantic**: Configuration management and validation
- **Rich**: Enhanced terminal output and logging
- **Pytest**: Testing framework
- **Black**: Code formatting
- **MyPy**: Static type checking

## Security Architecture

### Authentication

1. **Service Account Authentication**: Primary method for production
2. **Environment Variables**: Secure credential management
3. **Base64 Encoding**: Safe storage in CI/CD systems

### Data Protection

1. **Synthetic Data**: No real PII in generated data
2. **Encryption**: BigQuery default encryption at rest
3. **Access Control**: Role-based permissions
4. **Audit Logging**: Complete audit trail

## Performance Considerations

### Data Generation

1. **Batch Processing**: Process data in configurable batches
2. **Memory Management**: Efficient memory usage patterns
3. **Parallel Processing**: Concurrent generation where possible
4. **Progress Tracking**: Real-time progress monitoring

### BigQuery Operations

1. **Bulk Loading**: Efficient bulk data loading
2. **Schema Validation**: Pre-upload validation
3. **Error Handling**: Comprehensive error recovery
4. **Retry Logic**: Automatic retry for transient failures

## Scalability Design

### Horizontal Scaling

1. **Stateless Design**: Components can be scaled independently
2. **Batch Processing**: Handle large datasets efficiently
3. **Cloud Native**: Leverage cloud scalability
4. **Resource Management**: Efficient resource utilization

### Vertical Scaling

1. **Configuration**: Adjustable batch sizes and timeouts
2. **Memory Optimization**: Efficient memory usage
3. **Performance Tuning**: Optimized algorithms and data structures

## Monitoring and Observability

### Logging

1. **Structured Logging**: Consistent log format
2. **Multiple Outputs**: Console and file logging
3. **Log Levels**: Configurable verbosity
4. **Rich Formatting**: Enhanced readability

### Metrics

1. **Data Volume**: Track data generation and loading
2. **Performance**: Monitor processing times
3. **Errors**: Track and analyze errors
4. **Resource Usage**: Monitor memory and CPU usage

## Deployment Architecture

### Development

1. **Local Development**: Full local development environment
2. **Virtual Environments**: Isolated Python environments
3. **Configuration Management**: Environment-specific settings

### Production

1. **Cloud Deployment**: Google Cloud Platform
2. **CI/CD Integration**: GitHub Actions or similar
3. **Environment Variables**: Secure configuration
4. **Automated Testing**: Pre-deployment validation

## Future Enhancements

### Planned Improvements

1. **Real-time Processing**: Stream processing capabilities
2. **Advanced Analytics**: Machine learning integration
3. **Data Visualization**: Built-in dashboards
4. **API Layer**: RESTful API for data access

### Extension Points

1. **Custom Generators**: Plugin architecture for custom data
2. **Multiple Destinations**: Support for other data warehouses
3. **Advanced Scheduling**: Complex scheduling rules
4. **Data Quality**: Advanced data quality checks
