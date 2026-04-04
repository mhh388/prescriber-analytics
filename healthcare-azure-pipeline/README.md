# 🏥 Healthcare Azure Pipeline — Databricks + ADF
> End-to-end data engineering on Azure: ADLS Gen2 → ADF → Databricks → Delta Lake

## 🗺️ Architecture
```
CMS Medicare Data (local / AWS S3)
        │
        ▼
┌─────────────────────┐
│  01_upload_adls.py  │  Upload raw CSV → Azure Data Lake Gen2
└────────┬────────────┘
         │ raw/cms_partd_2023.csv
         ▼
┌─────────────────────┐
│  Azure Data Lake    │  adls://healthcaredlmengqi/healthcare-data/
│  Gen2 (ADLS)        │  raw/ | processed/ | databricks-output/
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│  ADF Pipeline       │  Orchestrates the full workflow
│  (JSON definition)  │  Trigger → Copy → Databricks → Validate
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│  Databricks         │  PySpark transformation notebook
│  Notebook           │  Cleaning, enrichment, Delta Lake write
└────────┬────────────┘
         │ Delta format
         ▼
┌─────────────────────┐
│  ADLS processed/    │  Delta Lake tables, partitioned by state
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│  Databricks SQL     │  Analytics queries via SQL Warehouse
│  Warehouse          │  5 business insight queries
└─────────────────────┘
```

## 📁 Project Structure
```
healthcare-azure-pipeline/
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
├── config/
│   ├── __init__.py
│   └── settings.py
├── src/
│   ├── __init__.py
│   ├── 01_upload_adls.py      # Upload CMS data → ADLS Gen2
│   ├── 02_deploy_notebook.py  # Deploy Databricks notebook via API
│   ├── 03_run_pipeline.py     # Trigger ADF pipeline
│   └── 04_sql_analytics.py   # Run SQL queries on Databricks warehouse
├── notebooks/
│   └── transform_cms_data.py  # Databricks PySpark notebook
├── adf/
│   └── pipeline_definition.json  # ADF pipeline as code
├── tests/
│   ├── __init__.py
│   └── test_upload.py
└── .github/
    └── workflows/
        └── ci.yml
```

## 🚀 Setup Guide

### Prerequisites
```bash
python3 --version    # 3.9+
az --version         # Azure CLI
/usr/local/bin/databricks --version  # Databricks CLI 0.2x+
```

### Setup
```bash
# Clone repo
git clone https://github.com/YOUR_USERNAME/healthcare-azure-pipeline.git
cd healthcare-azure-pipeline

# Create venv
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
code .env  # Fill in your values
```

### Run the pipeline
```bash
# Step 1: Upload raw data to ADLS
python src/01_upload_adls.py

# Step 2: Deploy Databricks notebook
python src/02_deploy_notebook.py

# Step 3: Run ADF pipeline
python src/03_run_pipeline.py

# Step 4: SQL analytics
python src/04_sql_analytics.py
```

## 📊 Dataset
Same CMS Medicare Part D 2023 dataset from Project 1 (26.7M rows).

## 🔑 Key Differences vs Project 1 (AWS)
| Feature | Project 1 (AWS) | Project 2 (Azure) |
|---|---|---|
| Storage | S3 | ADLS Gen2 |
| Processing | Local PySpark | Databricks |
| Orchestration | Python scripts | Azure Data Factory |
| Format | Parquet | Delta Lake |
| Query engine | Athena | Databricks SQL |
| CI/CD | GitHub Actions | GitHub Actions |

## 📈 What This Demonstrates
- Azure Data Lake Gen2 with hierarchical namespace
- Azure Data Factory pipeline orchestration
- Databricks notebook deployment via REST API
- Delta Lake format (ACID transactions, time travel)
- Databricks SQL Warehouse analytics
- Infrastructure-as-code (ADF pipeline as JSON)
- Multi-cloud data engineering (AWS + Azure)
