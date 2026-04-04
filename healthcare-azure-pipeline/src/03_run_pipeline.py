"""
src/03_run_pipeline.py
──────────────────────
Step 3 of the Healthcare Azure Pipeline.

What this script does:
  1. Connects to Azure Data Factory via REST API
  2. Triggers the healthcare-etl-pipeline pipeline run
  3. Monitors the run until completion
  4. Prints a summary of each activity's status

In production, ADF pipelines are triggered by:
  - Schedules (e.g. daily at 2am)
  - Storage events (new file dropped in ADLS)
  - Manual triggers (this script)
  - Other pipelines (chaining)

Run:
  python src/03_run_pipeline.py
  python src/03_run_pipeline.py --year 2023 --sample 500000
"""

import sys
import json
import time
import argparse
import requests
from pathlib import Path
from datetime import datetime, timezone

from loguru import logger
from azure.identity import AzureCliCredential

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import (
    AZURE_SUBSCRIPTION_ID, AZURE_RESOURCE_GROUP,
    ADF_NAME, ADF_PIPELINE_NAME,
    CMS_DATASET_YEAR, LOCAL_PROCESSED_DIR,
    validate_config, print_config
)


# ── ADF REST API helpers ──────────────────────────────────────────────────────

def get_adf_token() -> str:
    """Get Azure access token for ADF REST API calls."""
    credential = AzureCliCredential()
    token = credential.get_token("https://management.azure.com/.default")
    return token.token


def adf_api(method: str, endpoint: str, payload: dict = None, token: str = None) -> dict:
    """Make an ADF REST API call."""
    base = (
        f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
        f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
        f"/providers/Microsoft.DataFactory/factories/{ADF_NAME}"
    )
    url     = f"{base}{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }
    response = requests.request(
        method, url, headers=headers,
        json=payload, params={"api-version": "2018-06-01"},
        timeout=60
    )
    response.raise_for_status()
    return response.json() if response.text else {}


# ── Pipeline Functions ────────────────────────────────────────────────────────

def trigger_pipeline(
    pipeline_name: str,
    parameters: dict,
    token: str,
) -> str:
    """
    Trigger an ADF pipeline run.
    Returns the run_id for monitoring.
    """
    logger.info(f"Triggering ADF pipeline: {pipeline_name}")
    logger.info(f"Parameters: {parameters}")

    result = adf_api(
        "POST",
        f"/pipelines/{pipeline_name}/createRun",
        payload={"parameters": parameters},
        token=token,
    )

    run_id = result["runId"]
    logger.success(f"Pipeline triggered — run_id: {run_id}")
    return run_id


def monitor_pipeline(run_id: str, token: str, timeout_seconds: int = 3600) -> dict:
    """
    Poll ADF pipeline run status until completion.
    Returns final run status dict.
    """
    logger.info(f"Monitoring pipeline run: {run_id}")
    start_time = time.time()

    terminal_states = {"Succeeded", "Failed", "Cancelled", "Canceling"}

    while True:
        status = adf_api(
            "GET",
            f"/pipelineruns/{run_id}",
            token=token,
        )

        run_status  = status.get("status", "Unknown")
        elapsed     = int(time.time() - start_time)
        duration_ms = status.get("durationInMs", 0)

        logger.info(f"  [{elapsed}s] Pipeline status: {run_status}")

        if run_status in terminal_states:
            return status

        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(
                f"Pipeline run timed out after {timeout_seconds}s. "
                f"Run ID: {run_id}"
            )

        time.sleep(15)


def get_activity_runs(run_id: str, token: str) -> list:
    """Get status of each activity in the pipeline run."""
    result = adf_api(
        "POST",
        f"/pipelineruns/{run_id}/queryActivityruns",
        payload={
            "lastUpdatedAfter":  "2024-01-01T00:00:00Z",
            "lastUpdatedBefore": datetime.now(timezone.utc).isoformat(),
        },
        token=token,
    )
    return result.get("value", [])


def check_pipeline_exists(pipeline_name: str, token: str) -> bool:
    """Check if the ADF pipeline exists before trying to trigger it."""
    try:
        adf_api("GET", f"/pipelines/{pipeline_name}", token=token)
        return True
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return False
        raise


def deploy_pipeline_from_json(pipeline_name: str, json_path: Path, token: str):
    """
    Deploy/update the ADF pipeline from the JSON definition file.
    This is infrastructure-as-code — the pipeline is defined in Git.
    """
    logger.info(f"Deploying pipeline from: {json_path}")

    with open(json_path) as f:
        pipeline_def = json.load(f)

    # ADF expects just the properties, not the full object
    payload = {"properties": pipeline_def["properties"]}

    adf_api(
        "PUT",
        f"/pipelines/{pipeline_name}",
        payload=payload,
        token=token,
    )
    logger.success(f"Pipeline deployed: {pipeline_name}")


# ── Main Pipeline Step ────────────────────────────────────────────────────────

def run_adf_pipeline(year: str, sample_rows: int = 0):
    """
    Deploy and trigger the ADF pipeline, then monitor until completion.
    """
    logger.info("=" * 60)
    logger.info("  STEP 3 — AZURE DATA FACTORY PIPELINE")
    logger.info("=" * 60)

    validate_config()
    print_config()

    # Get auth token
    token = get_adf_token()
    logger.info("Azure authentication successful")

    # Deploy pipeline from JSON definition (infrastructure-as-code)
    adf_json = Path(__file__).resolve().parent.parent / "adf" / "pipeline_definition.json"
    if adf_json.exists():
        try:
            deploy_pipeline_from_json(ADF_PIPELINE_NAME, adf_json, token)
        except Exception as e:
            logger.warning(f"Pipeline deploy skipped (may need Databricks linked service first): {e}")

    # Check pipeline exists
    if not check_pipeline_exists(ADF_PIPELINE_NAME, token):
        logger.warning(
            f"Pipeline '{ADF_PIPELINE_NAME}' not found in ADF.\n"
            f"The pipeline needs a Databricks Linked Service configured in the portal.\n"
            f"See README.md for setup instructions."
        )
        logger.info("Logging pipeline configuration for reference:")
        logger.info(f"  Pipeline name : {ADF_PIPELINE_NAME}")
        logger.info(f"  ADF name      : {ADF_NAME}")
        logger.info(f"  Resource group: {AZURE_RESOURCE_GROUP}")
        return {"status": "skipped", "reason": "pipeline_not_configured"}

    # Trigger pipeline
    parameters = {
        "year":        year,
        "sample_rows": str(sample_rows),
    }
    run_id = trigger_pipeline(ADF_PIPELINE_NAME, parameters, token)

    # Monitor
    final_status = monitor_pipeline(run_id, token)
    run_status   = final_status.get("status")

    # Get activity details
    activities = get_activity_runs(run_id, token)

    # Print activity summary
    logger.info("\n── Activity Results ───────────────────────────────────")
    for act in activities:
        icon   = "✅" if act.get("status") == "Succeeded" else "❌"
        name   = act.get("activityName", "unknown")
        status = act.get("status", "unknown")
        dur_ms = act.get("durationInMs", 0)
        logger.info(f"  {icon} {name:<35} {status} ({dur_ms/1000:.1f}s)")

    # Save run log
    run_log = {
        "run_id":         run_id,
        "pipeline":       ADF_PIPELINE_NAME,
        "status":         run_status,
        "parameters":     parameters,
        "activities":     activities,
        "triggered_at":   datetime.now(timezone.utc).isoformat(),
        "duration_ms":    final_status.get("durationInMs"),
    }

    log_dir = LOCAL_PROCESSED_DIR / "pipeline_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"adf_run_{run_id[:8]}.json"
    with open(log_path, "w") as f:
        json.dump(run_log, f, indent=2, default=str)

    logger.info("=" * 60)
    if run_status == "Succeeded":
        logger.success("  STEP 3 COMPLETE ✓")
    else:
        logger.error(f"  STEP 3 FAILED — status: {run_status}")
    logger.info(f"  Run ID    : {run_id}")
    logger.info(f"  Status    : {run_status}")
    logger.info(f"  Run log   : {log_path}")
    logger.info("=" * 60)

    return run_log


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Healthcare Azure Pipeline — Step 3: Trigger ADF Pipeline"
    )
    parser.add_argument("--year",        default=CMS_DATASET_YEAR)
    parser.add_argument("--sample",      type=int, default=500000,
                        help="Process N rows (0 = full dataset)")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)

    run_adf_pipeline(year=args.year, sample_rows=args.sample)
