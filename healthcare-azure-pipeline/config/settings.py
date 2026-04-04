"""
config/settings.py
──────────────────
Centralized configuration for the Healthcare Azure Pipeline.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# ── Azure ─────────────────────────────────────────────────────
AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID")
AZURE_RESOURCE_GROUP  = os.getenv("AZURE_RESOURCE_GROUP")
AZURE_TENANT_ID       = os.getenv("AZURE_TENANT_ID")

# ── ADLS Gen2 ─────────────────────────────────────────────────
ADLS_ACCOUNT_NAME     = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_CONTAINER        = os.getenv("ADLS_CONTAINER", "healthcare-data")
ADLS_RAW_PATH         = "raw"
ADLS_PROCESSED_PATH   = "processed"
ADLS_OUTPUT_PATH      = "databricks-output"

# Full ADLS URLs
ADLS_ACCOUNT_URL      = f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"
ADLS_RAW_URL          = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net/{ADLS_RAW_PATH}"
ADLS_PROCESSED_URL    = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net/{ADLS_PROCESSED_PATH}"

# ── ADF ───────────────────────────────────────────────────────
ADF_NAME              = os.getenv("ADF_NAME")
ADF_PIPELINE_NAME     = "healthcare-etl-pipeline"

# ── Databricks ────────────────────────────────────────────────
DATABRICKS_HOST       = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN      = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID")
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "5bd843704fe084fc")
DATABRICKS_NOTEBOOK_PATH = "/healthcare-pipeline/transform_cms_data"

# ── Pipeline ──────────────────────────────────────────────────
PIPELINE_ENV          = os.getenv("PIPELINE_ENV", "development")
CMS_DATASET_YEAR      = os.getenv("CMS_DATASET_YEAR", "2023")

# ── Local paths ───────────────────────────────────────────────
LOCAL_DATA_DIR        = BASE_DIR / "data"
LOCAL_RAW_DIR         = LOCAL_DATA_DIR / "raw"
LOCAL_PROCESSED_DIR   = LOCAL_DATA_DIR / "processed"

# Source data from Project 1
PROJECT1_DATA_DIR     = Path.home() / "Desktop/healthcare-etl-pipeline/data"
PROJECT1_RAW_FILE     = PROJECT1_DATA_DIR / "raw/cms_partd_2023.csv"


def validate_config():
    required = {
        "AZURE_SUBSCRIPTION_ID": AZURE_SUBSCRIPTION_ID,
        "ADLS_ACCOUNT_NAME":     ADLS_ACCOUNT_NAME,
        "DATABRICKS_HOST":       DATABRICKS_HOST,
        "DATABRICKS_TOKEN":      DATABRICKS_TOKEN,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            f"Copy .env.example to .env and fill in your values."
        )


def print_config():
    print(f"""
╔══════════════════════════════════════════════╗
║   Healthcare Azure Pipeline Config           ║
╠══════════════════════════════════════════════╣
║  Environment  : {PIPELINE_ENV:<28}║
║  ADLS Account : {ADLS_ACCOUNT_NAME or 'NOT SET':<28}║
║  ADF Name     : {ADF_NAME or 'NOT SET':<28}║
║  Databricks   : {(DATABRICKS_HOST or 'NOT SET')[-28:]:<28}║
║  Warehouse ID : {DATABRICKS_WAREHOUSE_ID:<28}║
╚══════════════════════════════════════════════╝
    """)
