"""
Data schemas for FMCG Data Analytics Platform
"""

from typing import List, Dict, Any
from google.cloud import bigquery
from dataclasses import dataclass
from enum import Enum


class TableType(Enum):
    """Table types for classification"""
    DIMENSION = "dimension"
    FACT = "fact"


@dataclass
class TableSchema:
    """Table schema definition"""
    name: str
    type: TableType
    fields: List[Dict[str, Any]]
    description: str = ""


# Dimension Table Schemas
DIM_EMPLOYEES = TableSchema(
    name="dim_employees",
    type=TableType.DIMENSION,
    fields=[
        {"name": "employee_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "first_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "last_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "REQUIRED"},
        {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
        {"name": "department_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "job_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "hire_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "termination_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "salary", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "location_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "bank_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "insurance_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ],
    description="Employee master data with demographics and employment details"
)

DIM_PRODUCTS = TableSchema(
    name="dim_products",
    type=TableType.DIMENSION,
    fields=[
        {"name": "product_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "product_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "sku", "type": "STRING", "mode": "REQUIRED"},
        {"name": "category_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "subcategory_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "brand_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "unit_price", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "cost", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "weight", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "volume", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "REQUIRED"},
        {"name": "launch_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "discontinued_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ],
    description="Product master data with pricing and categorization"
)

DIM_RETAILERS = TableSchema(
    name="dim_retailers",
    type=TableType.DIMENSION,
    fields=[
        {"name": "retailer_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "retailer_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "retailer_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "location_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "contact_person", "type": "STRING", "mode": "NULLABLE"},
        {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "credit_limit", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "payment_terms", "type": "STRING", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "REQUIRED"},
        {"name": "registration_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ],
    description="Retailer master data with geographic and business details"
)

DIM_LOCATIONS = TableSchema(
    name="dim_locations",
    type=TableType.DIMENSION,
    fields=[
        {"name": "location_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "street_address", "type": "STRING", "mode": "NULLABLE"},
        {"name": "city", "type": "STRING", "mode": "REQUIRED"},
        {"name": "province", "type": "STRING", "mode": "REQUIRED"},
        {"name": "region", "type": "STRING", "mode": "REQUIRED"},
        {"name": "postal_code", "type": "STRING", "mode": "NULLABLE"},
        {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ],
    description="Location data with Philippines geographic hierarchy"
)

DIM_DEPARTMENTS = TableSchema(
    name="dim_departments",
    type=TableType.DIMENSION,
    fields=[
        {"name": "department_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "department_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "parent_department_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "manager_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "budget", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "description", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ],
    description="Organizational department structure"
)

DIM_JOBS = TableSchema(
    name="dim_jobs",
    type=TableType.DIMENSION,
    fields=[
        {"name": "job_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "job_title", "type": "STRING", "mode": "REQUIRED"},
        {"name": "job_level", "type": "STRING", "mode": "REQUIRED"},
        {"name": "min_salary", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "max_salary", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "department_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "work_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "description", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ],
    description="Job positions with salary ranges and work arrangements"
)

DIM_CAMPAIGNS = TableSchema(
    name="dim_campaigns",
    type=TableType.DIMENSION,
    fields=[
        {"name": "campaign_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "campaign_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "campaign_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "start_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "end_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "budget", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "target_audience", "type": "STRING", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "REQUIRED"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ],
    description="Marketing campaign definitions and timelines"
)

# Fact Table Schemas
FACT_SALES = TableSchema(
    name="fact_sales",
    type=TableType.FACT,
    fields=[
        {"name": "sale_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "product_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "retailer_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "employee_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "campaign_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "quantity", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "unit_price", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "total_amount", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "discount_amount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "commission_rate", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "order_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "delivery_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "delivery_status", "type": "STRING", "mode": "REQUIRED"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ],
    description="Sales transactions with order and delivery tracking"
)

FACT_INVENTORY = TableSchema(
    name="fact_inventory",
    type=TableType.FACT,
    fields=[
        {"name": "inventory_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "product_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "location_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "opening_stock", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "closing_stock", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "stock_received", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "stock_sold", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "stock_lost", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "unit_cost", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "total_value", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ],
    description="Inventory movements and valuations"
)

FACT_OPERATING_COSTS = TableSchema(
    name="fact_operating_costs",
    type=TableType.FACT,
    fields=[
        {"name": "cost_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "cost_category", "type": "STRING", "mode": "REQUIRED"},
        {"name": "cost_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "department_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "amount", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "description", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ],
    description="Operating expenses by category and department"
)

FACT_MARKETING_COSTS = TableSchema(
    name="fact_marketing_costs",
    type=TableType.FACT,
    fields=[
        {"name": "marketing_cost_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "campaign_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "cost_category", "type": "STRING", "mode": "REQUIRED"},
        {"name": "amount", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "description", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ],
    description="Marketing campaign expenses by category"
)

# All schemas dictionary for easy access
ALL_SCHEMAS = {
    "dim_employees": DIM_EMPLOYEES,
    "dim_products": DIM_PRODUCTS,
    "dim_retailers": DIM_RETAILERS,
    "dim_locations": DIM_LOCATIONS,
    "dim_departments": DIM_DEPARTMENTS,
    "dim_jobs": DIM_JOBS,
    "dim_campaigns": DIM_CAMPAIGNS,
    "fact_sales": FACT_SALES,
    "fact_inventory": FACT_INVENTORY,
    "fact_operating_costs": FACT_OPERATING_COSTS,
    "fact_marketing_costs": FACT_MARKETING_COSTS,
}


def get_bigquery_schema(table_schema: TableSchema) -> List[bigquery.SchemaField]:
    """Convert TableSchema to BigQuery schema"""
    bigquery_fields = []
    
    for field in table_schema.fields:
        bigquery_field = bigquery.SchemaField(
            name=field["name"],
            field_type=field["type"],
            mode=field["mode"]
        )
        bigquery_fields.append(bigquery_field)
    
    return bigquery_fields
