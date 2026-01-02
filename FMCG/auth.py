import os
import base64
from google.cloud import bigquery

def setup_gcp_auth():
    """Setup GCP authentication from service account"""
    if "GCP_SERVICE_ACCOUNT" in os.environ:
        sa_json = base64.b64decode(os.environ["GCP_SERVICE_ACCOUNT"])
        with open("gcp-key.json", "wb") as f:
            f.write(sa_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"

def get_bigquery_client(project_id):
    """Get authenticated BigQuery client"""
    setup_gcp_auth()
    return bigquery.Client(project=project_id)
