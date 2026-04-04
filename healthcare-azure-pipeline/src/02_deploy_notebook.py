"""
src/02_deploy_notebook.py
─────────────────────────
Step 2 of the Healthcare Azure Pipeline.

What this script does:
  1. Reads the local PySpark notebook file
  2. Deploys it to Databricks workspace via REST API
  3. Verifies the deployment
  4. Optionally runs it immediately

This is how real data engineering teams deploy notebooks —
via CI/CD pipelines, not manual uploads through the UI.

Run:
  python src/02_deploy_notebook.py
  python src/02_deploy_notebook.py --run   (deploy and run immediately)
"""

import sys
import json
import time
import base64
import argparse
import requests
from pathlib import Path
from datetime import datetime, timezone

from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import (
    DATABRICKS_HOST, DATABRICKS_TOKEN,
    DATABRICKS_CLUSTER_ID, DATABRICKS_NOTEBOOK_PATH,
    CMS_DATASET_YEAR, validate_config
)


# ── Databricks API helpers ────────────────────────────────────────────────────

def db_api(method: str, endpoint: str, payload: dict = None) -> dict:
    """Make a Databricks REST API call."""
    url     = f"{DATABRICKS_HOST}/api/2.0{endpoint}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type":  "application/json",
    }
    response = requests.request(method, url, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    return response.json() if response.text else {}


# ── Deploy Functions ──────────────────────────────────────────────────────────

def deploy_notebook(local_path: Path, workspace_path: str) -> bool:
    """
    Deploy a Python notebook to Databricks workspace.
    Uses base64 encoding as required by the Workspace API.
    """
    logger.info(f"Deploying notebook: {local_path.name} → {workspace_path}")

    # Read and encode notebook content
    content      = local_path.read_text(encoding="utf-8")
    content_b64  = base64.b64encode(content.encode("utf-8")).decode("utf-8")

    # Create parent directory in workspace
    workspace_dir = str(Path(workspace_path).parent)
    try:
        db_api("POST", "/workspace/mkdirs", {"path": workspace_dir})
        logger.info(f"Workspace directory ensured: {workspace_dir}")
    except Exception as e:
        logger.warning(f"Directory may already exist: {e}")

    # Import notebook
    db_api("POST", "/workspace/import", {
        "path":      workspace_path,
        "format":    "SOURCE",
        "language":  "PYTHON",
        "content":   content_b64,
        "overwrite": True,
    })

    logger.success(f"Notebook deployed: {workspace_path}")
    return True


def verify_deployment(workspace_path: str) -> dict:
    """Verify the notebook exists in the workspace."""
    result = db_api("GET", "/workspace/get-status", {"path": workspace_path})
    logger.success(f"Verification passed ✓ — notebook exists at {workspace_path}")
    return result


def run_notebook(
    workspace_path: str,
    year: str,
    sample_rows: int = 500000,
    timeout_seconds: int = 3600,
) -> dict:
    """
    Submit the notebook as a one-time job run via Databricks Jobs API.
    This is how ADF triggers notebooks in production.
    """
    logger.info(f"Submitting notebook run: {workspace_path}")

    payload = {
        "run_name": f"healthcare-etl-{year}-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "existing_cluster_id": DATABRICKS_CLUSTER_ID,
        "notebook_task": {
            "notebook_path": workspace_path,
            "base_parameters": {
                "year":         year,
                "sample_rows":  str(sample_rows),
                "adls_account": "healthcaredlmengqi",
                "container":    "healthcare-data",
            },
        },
        "timeout_seconds": timeout_seconds,
    }

    run = db_api("POST", "/jobs/runs/submit", payload)
    run_id = run["run_id"]
    logger.info(f"Run submitted: run_id={run_id}")

    # Poll for completion
    logger.info("Waiting for run to complete...")
    start_time = time.time()
    while True:
        status = db_api("GET", f"/jobs/runs/get?run_id={run_id}")
        state  = status["state"]["life_cycle_state"]
        elapsed = int(time.time() - start_time)

        logger.info(f"  [{elapsed}s] State: {state}")

        if state == "TERMINATED":
            result_state = status["state"].get("result_state", "UNKNOWN")
            if result_state == "SUCCESS":
                logger.success(f"Run completed successfully in {elapsed}s")
                return {"run_id": run_id, "status": "success", "elapsed": elapsed}
            else:
                error = status["state"].get("state_message", "unknown error")
                raise RuntimeError(f"Run failed: {result_state} — {error}")

        elif state in ("INTERNAL_ERROR", "SKIPPED"):
            raise RuntimeError(f"Run failed with state: {state}")

        elif time.time() - start_time > timeout_seconds:
            raise TimeoutError(f"Run timed out after {timeout_seconds}s")

        time.sleep(15)


# ── Main ──────────────────────────────────────────────────────────────────────

def run_deploy(run_after: bool = False):
    """Deploy notebook to Databricks workspace."""
    logger.info("=" * 60)
    logger.info("  STEP 2 — DEPLOY DATABRICKS NOTEBOOK")
    logger.info("=" * 60)

    validate_config()

    # Local notebook path
    notebook_dir  = Path(__file__).resolve().parent.parent / "notebooks"
    notebook_file = notebook_dir / "transform_cms_data.py"

    if not notebook_file.exists():
        raise FileNotFoundError(f"Notebook not found: {notebook_file}")

    # Deploy
    deploy_notebook(notebook_file, DATABRICKS_NOTEBOOK_PATH)

    # Verify
    info = verify_deployment(DATABRICKS_NOTEBOOK_PATH)

    result = {
        "workspace_path": DATABRICKS_NOTEBOOK_PATH,
        "deployed_at":    datetime.now(timezone.utc).isoformat(),
        "object_type":    info.get("object_type"),
        "object_id":      info.get("object_id"),
    }

    # Optionally run
    if run_after:
        logger.info("--run flag set — submitting notebook run")
        run_result = run_notebook(
            workspace_path=DATABRICKS_NOTEBOOK_PATH,
            year=CMS_DATASET_YEAR,
            sample_rows=500_000,
        )
        result["run"] = run_result

    logger.info("=" * 60)
    logger.success("  STEP 2 COMPLETE ✓")
    logger.info(f"  Notebook    : {DATABRICKS_NOTEBOOK_PATH}")
    logger.info(f"  Deployed at : {result['deployed_at']}")
    logger.info("=" * 60)

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Healthcare Azure Pipeline — Step 2: Deploy Databricks Notebook"
    )
    parser.add_argument("--run", action="store_true",
                        help="Deploy and immediately run the notebook")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)

    run_deploy(run_after=args.run)
