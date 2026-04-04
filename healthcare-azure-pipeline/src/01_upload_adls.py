"""
src/01_upload_adls.py
─────────────────────
Step 1 of the Healthcare Azure Pipeline.

What this script does:
  1. Reads the CMS Medicare CSV from Project 1's data folder
  2. Uploads it to Azure Data Lake Storage Gen2
  3. Validates the upload
  4. Writes a manifest JSON

Run:
  python src/01_upload_adls.py
  python src/01_upload_adls.py --sample  (upload 500k row sample for testing)
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from loguru import logger
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import (
    ADLS_ACCOUNT_NAME, ADLS_ACCOUNT_URL, ADLS_CONTAINER,
    ADLS_RAW_PATH, PROJECT1_RAW_FILE, LOCAL_RAW_DIR,
    CMS_DATASET_YEAR, validate_config, print_config
)


# ── ADLS Client ───────────────────────────────────────────────────────────────

def get_adls_client() -> DataLakeServiceClient:
    """Get ADLS Gen2 client using Azure CLI credentials."""
    credential = AzureCliCredential()
    return DataLakeServiceClient(
        account_url=ADLS_ACCOUNT_URL,
        credential=credential
    )


# ── Upload Functions ──────────────────────────────────────────────────────────

def upload_file_to_adls(
    client: DataLakeServiceClient,
    local_path: Path,
    adls_path: str,
    overwrite: bool = True,
) -> str:
    """
    Upload a local file to ADLS Gen2.
    Returns the full ADLS URI of the uploaded file.
    """
    file_system = client.get_file_system_client(ADLS_CONTAINER)
    file_client  = file_system.get_file_client(adls_path)

    file_size_mb = local_path.stat().st_size / 1024 / 1024
    logger.info(f"Uploading {local_path.name} ({file_size_mb:.1f} MB) → adls://{adls_path}")

    with open(local_path, "rb") as f:
        file_client.upload_data(
            f,
            overwrite=overwrite,
            length=local_path.stat().st_size,
            chunk_size=4 * 1024 * 1024,  # 4MB chunks
            timeout=600,                  # 10 minute timeout
            max_concurrency=4,
        )

    adls_uri = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net/{adls_path}"
    logger.success(f"Upload complete: {adls_uri}")
    return adls_uri


def validate_adls_upload(
    client: DataLakeServiceClient,
    adls_path: str,
    expected_size_bytes: int,
) -> bool:
    """Verify the uploaded file exists and has the expected size."""
    file_system = client.get_file_system_client(ADLS_CONTAINER)
    file_client  = file_system.get_file_client(adls_path)

    try:
        props = file_client.get_file_properties()
        actual_size = props.size
        if actual_size == expected_size_bytes:
            logger.success(f"Validation passed ✓ — file size matches ({actual_size:,} bytes)")
            return True
        else:
            logger.error(
                f"Validation failed — expected {expected_size_bytes:,} bytes, "
                f"got {actual_size:,} bytes"
            )
            return False
    except Exception as e:
        logger.error(f"Validation failed — file not found: {e}")
        return False


def create_sample_file(source_path: Path, sample_rows: int = 500_000) -> Path:
    """Create a sample CSV file with N rows for faster testing."""
    sample_path = LOCAL_RAW_DIR / f"cms_partd_{CMS_DATASET_YEAR}_sample.csv"
    LOCAL_RAW_DIR.mkdir(parents=True, exist_ok=True)

    if sample_path.exists():
        logger.info(f"Sample file already exists: {sample_path}")
        return sample_path

    logger.info(f"Creating {sample_rows:,} row sample from {source_path.name}...")
    df = pd.read_csv(source_path, nrows=sample_rows)
    df.to_csv(sample_path, index=False)
    logger.success(f"Sample created: {sample_path} ({sample_path.stat().st_size/1024/1024:.1f} MB)")
    return sample_path


# ── Main Pipeline Step ────────────────────────────────────────────────────────

def run_upload(sample: bool = False):
    """Upload CMS data to ADLS Gen2."""
    logger.info("=" * 60)
    logger.info("  STEP 1 — UPLOAD TO AZURE DATA LAKE GEN2")
    logger.info("=" * 60)

    validate_config()
    print_config()

    # Determine source file
    if sample:
        logger.info("Sample mode — creating 500k row subset")
        source_file = create_sample_file(PROJECT1_RAW_FILE)
    else:
        source_file = PROJECT1_RAW_FILE

    if not source_file.exists():
        raise FileNotFoundError(
            f"Source file not found: {source_file}\n"
            f"Make sure Project 1 data is at: {PROJECT1_RAW_FILE}"
        )

    # ADLS destination path
    filename    = f"cms_partd_{CMS_DATASET_YEAR}{'_sample' if sample else ''}.csv"
    adls_path   = f"{ADLS_RAW_PATH}/source=cms/year={CMS_DATASET_YEAR}/{filename}"

    # Upload
    client   = get_adls_client()
    adls_uri = upload_file_to_adls(client, source_file, adls_path)

    # Validate
    valid = validate_adls_upload(client, adls_path, source_file.stat().st_size)
    if not valid:
        raise RuntimeError("Upload validation failed — file size mismatch")

    # Write manifest
    manifest = {
        "source_file":    str(source_file),
        "adls_uri":       adls_uri,
        "file_size_mb":   round(source_file.stat().st_size / 1024 / 1024, 2),
        "sample":         sample,
        "year":           CMS_DATASET_YEAR,
        "uploaded_at":    datetime.now(timezone.utc).isoformat(),
        "pipeline":       "healthcare-azure-pipeline",
        "step":           "01_upload_adls",
    }

    manifest_dir = LOCAL_RAW_DIR / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"adls_upload_{CMS_DATASET_YEAR}.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    logger.info("=" * 60)
    logger.success("  STEP 1 COMPLETE ✓")
    logger.info(f"  Source      : {source_file.name}")
    logger.info(f"  ADLS URI    : {adls_uri}")
    logger.info(f"  Validated   : ✓")
    logger.info(f"  Manifest    : {manifest_path}")
    logger.info("=" * 60)

    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Healthcare Azure Pipeline — Step 1: Upload to ADLS"
    )
    parser.add_argument("--sample", action="store_true",
                        help="Upload 500k row sample instead of full file")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)

    run_upload(sample=args.sample)
