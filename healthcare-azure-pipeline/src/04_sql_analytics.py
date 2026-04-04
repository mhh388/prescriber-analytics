"""
src/04_sql_analytics.py
────────────────────────
Step 4 of the Healthcare Azure Pipeline.

What this script does:
  1. Connects to Databricks SQL Warehouse
  2. Grants the warehouse access to ADLS using storage account key
  3. Runs 5 analytics queries directly on the raw CSV in ADLS
     using Databricks read_files() — no Delta table needed
  4. Saves results locally as CSV
  5. Prints key business insights

Run:
  python src/04_sql_analytics.py
  python src/04_sql_analytics.py --year 2023
"""

import os
import sys
import json
import time
import argparse
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from loguru import logger
from dotenv import load_dotenv

# Load .env
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

sys.path.insert(0, str(BASE_DIR))
from config.settings import (
    DATABRICKS_HOST, DATABRICKS_TOKEN,
    DATABRICKS_WAREHOUSE_ID, CMS_DATASET_YEAR,
    LOCAL_PROCESSED_DIR, validate_config
)

# ADLS credentials
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME", "healthcaredlmengqi")
ADLS_ACCOUNT_KEY  = os.getenv("ADLS_ACCOUNT_KEY", "")
ADLS_CONTAINER    = os.getenv("ADLS_CONTAINER", "healthcare-data")

# Full path to the sample file in ADLS (blob endpoint for SQL warehouse access)
# Use local file path for SQL queries via pandas
LOCAL_SAMPLE_FILE = str(BASE_DIR / "data/raw/cms_partd_2023_sample.csv")

# ── SQL Queries ───────────────────────────────────────────────────────────────
# All queries use read_files() to query raw CSV directly from ADLS
# No Delta table or pre-registered table needed

def get_queries(file_path: str) -> dict:
    return {

        "top_drugs_by_cost": {
            "description": "Top 20 drugs by total Medicare spend",
            "sql": f"""
                SELECT
                    Gnrc_Name                                   AS generic_name,
                    CASE WHEN Brnd_Name IS NOT NULL
                         AND Brnd_Name != ''
                         THEN 'brand' ELSE 'generic' END        AS drug_type,
                    COUNT(DISTINCT Prscrbr_NPI)                 AS prescriber_count,
                    ROUND(SUM(CAST(Tot_Clms AS DOUBLE)), 0)     AS total_claims,
                    ROUND(SUM(CAST(Tot_Drug_Cst AS DOUBLE)), 2) AS total_spend,
                    ROUND(
                        SUM(CAST(Tot_Drug_Cst AS DOUBLE)) /
                        NULLIF(SUM(CAST(Tot_Clms AS DOUBLE)), 0), 2
                    )                                           AS avg_cost_per_claim
                FROM read_files(
                    '{file_path}',
                    format => 'csv',
                    header => true
                )
                WHERE Tot_Drug_Cst IS NOT NULL
                  AND CAST(Tot_Clms AS DOUBLE) > 0
                GROUP BY Gnrc_Name, drug_type
                ORDER BY total_spend DESC
                LIMIT 20
            """,
        },

        "state_summary": {
            "description": "Drug spend summary by state",
            "sql": f"""
                SELECT
                    Prscrbr_State_Abrvtn                            AS prescriber_state,
                    COUNT(DISTINCT Prscrbr_NPI)                     AS unique_prescribers,
                    COUNT(DISTINCT Gnrc_Name)                       AS unique_drugs,
                    ROUND(SUM(CAST(Tot_Clms AS DOUBLE)), 0)         AS total_claims,
                    ROUND(SUM(CAST(Tot_Drug_Cst AS DOUBLE)), 2)     AS total_spend,
                    ROUND(
                        SUM(CAST(Tot_Drug_Cst AS DOUBLE)) /
                        NULLIF(SUM(CAST(Tot_Benes AS DOUBLE)), 0), 2
                    )                                               AS spend_per_patient
                FROM read_files(
                    '{file_path}',
                    format => 'csv',
                    header => true
                )
                WHERE Prscrbr_State_Abrvtn IS NOT NULL
                GROUP BY Prscrbr_State_Abrvtn
                ORDER BY total_spend DESC
            """,
        },

        "specialty_analysis": {
            "description": "Prescribing patterns by medical specialty",
            "sql": f"""
                SELECT
                    Prscrbr_Type                                    AS prescriber_specialty,
                    COUNT(DISTINCT Prscrbr_NPI)                     AS prescriber_count,
                    ROUND(SUM(CAST(Tot_Clms AS DOUBLE)), 0)         AS total_claims,
                    ROUND(SUM(CAST(Tot_Drug_Cst AS DOUBLE)), 2)     AS total_spend,
                    ROUND(
                        SUM(CAST(Tot_Drug_Cst AS DOUBLE)) /
                        NULLIF(SUM(CAST(Tot_Clms AS DOUBLE)), 0), 2
                    )                                               AS avg_cost_per_claim
                FROM read_files(
                    '{file_path}',
                    format => 'csv',
                    header => true
                )
                WHERE Prscrbr_Type IS NOT NULL
                  AND CAST(Tot_Clms AS DOUBLE) > 100
                GROUP BY Prscrbr_Type
                ORDER BY total_spend DESC
                LIMIT 20
            """,
        },

        "brand_vs_generic": {
            "description": "Brand vs generic utilization comparison",
            "sql": f"""
                SELECT
                    CASE WHEN Brnd_Name IS NOT NULL
                         AND Brnd_Name != ''
                         THEN 'brand' ELSE 'generic' END            AS drug_type,
                    COUNT(DISTINCT Gnrc_Name)                       AS unique_drugs,
                    ROUND(SUM(CAST(Tot_Clms AS DOUBLE)), 0)         AS total_claims,
                    ROUND(SUM(CAST(Tot_Drug_Cst AS DOUBLE)), 2)     AS total_spend,
                    ROUND(
                        SUM(CAST(Tot_Drug_Cst AS DOUBLE)) /
                        NULLIF(SUM(CAST(Tot_Clms AS DOUBLE)), 0), 2
                    )                                               AS avg_cost_per_claim
                FROM read_files(
                    '{file_path}',
                    format => 'csv',
                    header => true
                )
                GROUP BY drug_type
                ORDER BY total_spend DESC
            """,
        },

        "high_cost_outliers": {
            "description": "Ultra-high cost drug analysis (>$100k per claim)",
            "sql": f"""
                SELECT
                    Gnrc_Name                                       AS generic_name,
                    Brnd_Name                                       AS brand_name,
                    Prscrbr_Type                                    AS prescriber_specialty,
                    Prscrbr_State_Abrvtn                            AS state,
                    ROUND(SUM(CAST(Tot_Clms AS DOUBLE)), 0)         AS total_claims,
                    ROUND(SUM(CAST(Tot_Drug_Cst AS DOUBLE)), 2)     AS total_spend,
                    ROUND(
                        SUM(CAST(Tot_Drug_Cst AS DOUBLE)) /
                        NULLIF(SUM(CAST(Tot_Clms AS DOUBLE)), 0), 2
                    )                                               AS avg_cost_per_claim
                FROM read_files(
                    '{file_path}',
                    format => 'csv',
                    header => true
                )
                WHERE CAST(Tot_Drug_Cst AS DOUBLE) /
                    NULLIF(CAST(Tot_Clms AS DOUBLE), 0) > 100000
                GROUP BY Gnrc_Name, Brnd_Name, Prscrbr_Type, Prscrbr_State_Abrvtn
                ORDER BY avg_cost_per_claim DESC
            """,
        },
    }


# ── Databricks SQL API ────────────────────────────────────────────────────────

def run_sql_query(sql: str, warehouse_id: str) -> pd.DataFrame:
    """
    Execute SQL via Databricks SQL Statement API.
    Returns results as a pandas DataFrame.
    """
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type":  "application/json",
    }

    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/sql/statements",
        headers=headers,
        json={
            "statement":      sql,
            "warehouse_id":   warehouse_id,
            "wait_timeout":   "50s",
            "on_wait_timeout": "CONTINUE",
        },
        timeout=120,
    )
    response.raise_for_status()
    result = response.json()
    statement_id = result["statement_id"]

    # Poll until done
    start = time.time()
    while True:
        poll = requests.get(
            f"{DATABRICKS_HOST}/api/2.0/sql/statements/{statement_id}",
            headers=headers,
            timeout=30,
        ).json()

        status = poll["status"]["state"]

        if status == "SUCCEEDED":
            break
        elif status in ("FAILED", "CANCELED", "CLOSED"):
            error = poll["status"].get("error", {}).get("message", "unknown")
            raise RuntimeError(f"SQL query failed: {status} — {error}")
        elif time.time() - start > 300:
            raise TimeoutError("SQL query timed out after 300s")

        time.sleep(2)

    # Parse results
    manifest = poll.get("manifest", {})
    chunks   = poll.get("result", {}).get("data_array", [])
    columns  = [col["name"] for col in manifest.get("schema", {}).get("columns", [])]

    if not chunks:
        return pd.DataFrame(columns=columns)

    return pd.DataFrame(chunks, columns=columns)


def grant_storage_access(warehouse_id: str):
    """
    Grant the SQL Warehouse access to ADLS using the storage account key.
    This is run once before the analytics queries.
    """
    if not ADLS_ACCOUNT_KEY:
        logger.warning("ADLS_ACCOUNT_KEY not set in .env — storage access may fail")
        return

    setup_sql = (
        f"SET fs.azure.account.key.{ADLS_ACCOUNT_NAME}.blob.core.windows.net "
        f"= '{ADLS_ACCOUNT_KEY}'"
    )
    try:
        run_sql_query(setup_sql, warehouse_id)
        logger.success("Storage account access granted to SQL Warehouse")
    except Exception as e:
        logger.warning(f"Storage access setup warning (may still work): {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

import pandas as pd

def run_analytics_local(year: str = None):
    """Run analytics queries locally using pandas — same logic as Databricks SQL."""
    year = year or CMS_DATASET_YEAR

    logger.info("=" * 60)
    logger.info("  STEP 4 — SQL ANALYTICS (Local Execution)")
    logger.info("=" * 60)

    # Load data
    sample_file = BASE_DIR / "data/raw/cms_partd_2023_sample.csv"
    logger.info(f"Loading: {sample_file.name}")
    df = pd.read_csv(sample_file, low_memory=False)
    df["Tot_Clms"]     = pd.to_numeric(df["Tot_Clms"], errors="coerce")
    df["Tot_Drug_Cst"] = pd.to_numeric(df["Tot_Drug_Cst"], errors="coerce")
    df["Tot_Benes"]    = pd.to_numeric(df["Tot_Benes"], errors="coerce")
    df = df[df["Tot_Clms"] > 0].dropna(subset=["Tot_Clms", "Tot_Drug_Cst"])
    df["cost_per_claim"] = df["Tot_Drug_Cst"] / df["Tot_Clms"]
    df["drug_type"] = df["Brnd_Name"].apply(
        lambda x: "brand" if pd.notna(x) and str(x).strip() != "" else "generic"
    )
    logger.info(f"Loaded {len(df):,} rows")

    output_dir = LOCAL_PROCESSED_DIR / "query_results_azure"
    output_dir.mkdir(parents=True, exist_ok=True)
    results = {}

    # Query 1: Top drugs by cost
    q1 = (df.groupby(["Gnrc_Name", "drug_type"])
            .agg(prescriber_count=("Prscrbr_NPI", "nunique"),
                 total_claims=("Tot_Clms", "sum"),
                 total_spend=("Tot_Drug_Cst", "sum"),
                 avg_cost_per_claim=("cost_per_claim", "mean"))
            .reset_index()
            .sort_values("total_spend", ascending=False)
            .head(20)
            .round(2))
    q1.to_csv(output_dir / f"top_drugs_by_cost_{year}.csv", index=False)
    results["top_drugs_by_cost"] = {"status": "success", "rows": len(q1)}
    print(f"\n── Top 20 drugs by Medicare spend\n{q1.head(5).to_string(index=False)}")

    # Query 2: State summary
    q2 = (df.groupby("Prscrbr_State_Abrvtn")
            .agg(unique_prescribers=("Prscrbr_NPI", "nunique"),
                 unique_drugs=("Gnrc_Name", "nunique"),
                 total_claims=("Tot_Clms", "sum"),
                 total_spend=("Tot_Drug_Cst", "sum"))
            .reset_index()
            .rename(columns={"Prscrbr_State_Abrvtn": "state"})
            .sort_values("total_spend", ascending=False)
            .round(2))
    q2.to_csv(output_dir / f"state_summary_{year}.csv", index=False)
    results["state_summary"] = {"status": "success", "rows": len(q2)}
    print(f"\n── State-level drug spend\n{q2.head(5).to_string(index=False)}")

    # Query 3: Specialty analysis
    q3 = (df[df["Tot_Clms"] > 100]
            .groupby("Prscrbr_Type")
            .agg(prescriber_count=("Prscrbr_NPI", "nunique"),
                 total_claims=("Tot_Clms", "sum"),
                 total_spend=("Tot_Drug_Cst", "sum"),
                 avg_cost_per_claim=("cost_per_claim", "mean"))
            .reset_index()
            .rename(columns={"Prscrbr_Type": "specialty"})
            .sort_values("total_spend", ascending=False)
            .head(20)
            .round(2))
    q3.to_csv(output_dir / f"specialty_analysis_{year}.csv", index=False)
    results["specialty_analysis"] = {"status": "success", "rows": len(q3)}
    print(f"\n── Prescribing by specialty\n{q3.head(5).to_string(index=False)}")

    # Query 4: Brand vs generic
    q4 = (df.groupby("drug_type")
            .agg(unique_drugs=("Gnrc_Name", "nunique"),
                 total_claims=("Tot_Clms", "sum"),
                 total_spend=("Tot_Drug_Cst", "sum"),
                 avg_cost_per_claim=("cost_per_claim", "mean"))
            .reset_index()
            .round(2))
    q4.to_csv(output_dir / f"brand_vs_generic_{year}.csv", index=False)
    results["brand_vs_generic"] = {"status": "success", "rows": len(q4)}
    print(f"\n── Brand vs generic\n{q4.to_string(index=False)}")

    # Query 5: High cost outliers
    q5 = (df[df["cost_per_claim"] > 100000]
            .groupby(["Gnrc_Name", "Brnd_Name", "Prscrbr_Type", "Prscrbr_State_Abrvtn"])
            .agg(total_claims=("Tot_Clms", "sum"),
                 total_spend=("Tot_Drug_Cst", "sum"),
                 avg_cost_per_claim=("cost_per_claim", "mean"))
            .reset_index()
            .sort_values("avg_cost_per_claim", ascending=False)
            .round(2))
    q5.to_csv(output_dir / f"high_cost_outliers_{year}.csv", index=False)
    results["high_cost_outliers"] = {"status": "success", "rows": len(q5)}
    print(f"\n── High cost outliers\n{q5.to_string(index=False)}")

    # Summary
    succeeded = sum(1 for r in results.values() if r["status"] == "success")
    logger.info("=" * 60)
    logger.success("  STEP 4 COMPLETE ✓")
    logger.info(f"  Queries success : {succeeded}/5")
    logger.info(f"  Results saved   : {output_dir}")
    logger.info("=" * 60)
    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Healthcare Azure Pipeline — Step 4: Databricks SQL Analytics"
    )
    parser.add_argument("--year", default=CMS_DATASET_YEAR)
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)

    run_analytics_local(year=args.year)