"""
BigQuery client utilities for FMCG Data Analytics Platform
"""

import os
import base64
from typing import Optional, Dict, Any, List
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import Dataset, Table
from google.oauth2 import service_account
from google.auth import default
from .logger import default_logger


class BigQueryManager:
    """Manages BigQuery connections and operations"""
    
    def __init__(self, project_id: str, dataset: str, credentials_path: Optional[str] = None):
        self.project_id = project_id
        self.dataset = dataset
        self.client = self._create_client(credentials_path)
        self.logger = default_logger
        
    def _create_client(self, credentials_path: Optional[str]) -> bigquery.Client:
        """Create BigQuery client with proper authentication"""
        try:
            # Try service account from environment variable (base64 encoded)
            if "GCP_SERVICE_ACCOUNT" in os.environ:
                service_account_json = base64.b64decode(
                    os.environ["GCP_SERVICE_ACCOUNT"]
                ).decode("utf-8")
                credentials = service_account.Credentials.from_service_account_info(
                    eval(service_account_json)
                )
                return bigquery.Client(
                    project=self.project_id,
                    credentials=credentials
                )
            
            # Try credentials file
            elif credentials_path and os.path.exists(credentials_path):
                return bigquery.Client.from_service_account_json(
                    credentials_path,
                    project=self.project_id
                )
            
            # Try default credentials
            else:
                credentials, _ = default()
                return bigquery.Client(
                    project=self.project_id,
                    credentials=credentials
                )
                
        except Exception as e:
            self.logger.error(f"Failed to create BigQuery client: {e}")
            raise
    
    def ensure_dataset(self) -> Dataset:
        """Ensure dataset exists, create if necessary"""
        dataset_ref = self.client.dataset(self.dataset)
        
        try:
            dataset = self.client.get_dataset(dataset_ref)
            self.logger.info(f"Dataset {self.dataset} already exists")
            return dataset
        except Exception:
            self.logger.info(f"Creating dataset {self.dataset}")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            return self.client.create_dataset(dataset)
    
    def create_table(self, table_id: str, schema: List[bigquery.SchemaField]) -> Table:
        """Create a table with specified schema"""
        table_ref = self.client.dataset(self.dataset).table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        
        try:
            table = self.client.create_table(table)
            self.logger.info(f"Created table {table_id}")
            return table
        except Exception as e:
            if "Already Exists" in str(e):
                self.logger.info(f"Table {table_id} already exists")
                return self.client.get_table(table_ref)
            else:
                self.logger.error(f"Failed to create table {table_id}: {e}")
                raise
    
    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_id: str,
        write_disposition: str = "WRITE_APPEND"
    ) -> bigquery.job.LoadJob:
        """Load DataFrame into BigQuery table"""
        table_ref = self.client.dataset(self.dataset).table(table_id)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
            autodetect=True
        )
        
        job = self.client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        
        self.logger.info(f"Loading {len(df)} rows into {table_id}")
        job.result()  # Wait for completion
        
        if job.errors:
            self.logger.error(f"Errors loading data: {job.errors}")
            raise Exception(f"BigQuery load errors: {job.errors}")
        
        self.logger.info(f"Successfully loaded {len(df)} rows into {table_id}")
        return job
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        self.logger.info(f"Executing query: {query[:100]}...")
        
        try:
            df = self.client.query(query).to_dataframe()
            self.logger.info(f"Query returned {len(df)} rows")
            return df
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise
    
    def table_exists(self, table_id: str) -> bool:
        """Check if table exists"""
        try:
            self.client.get_table(self.client.dataset(self.dataset).table(table_id))
            return True
        except Exception:
            return False
    
    def get_table_row_count(self, table_id: str) -> int:
        """Get row count for a table"""
        query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset}.{table_id}`"
        result = self.execute_query(query)
        return result.iloc[0]['count'] if len(result) > 0 else 0
