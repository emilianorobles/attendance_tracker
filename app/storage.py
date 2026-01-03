"""Cloudflare R2 storage utilities for persisting files across dyno restarts.

Environment variables required:
- R2_BUCKET: Cloudflare R2 bucket name
- R2_ACCESS_KEY_ID: R2 API token access key
- R2_SECRET_ACCESS_KEY: R2 API token secret key
- R2_ENDPOINT_URL: R2 endpoint (e.g., https://<account_id>.r2.cloudflarestorage.com)

These are optional - if not set, R2 storage is disabled and local files are used.
"""
import os
from pathlib import Path
from typing import Optional

# R2 configuration from environment
R2_BUCKET = os.environ.get("R2_BUCKET")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.environ.get("R2_ENDPOINT_URL")

# Check if R2 is configured
R2_ENABLED = all([R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL])

_s3_client = None


def _get_s3_client():
    """Get or create a boto3 S3 client configured for Cloudflare R2."""
    global _s3_client
    if _s3_client is None:
        import boto3  # type: ignore
        _s3_client = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT_URL,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name="auto",  # R2 uses 'auto' for region
        )
    return _s3_client


def download_from_r2(remote_key: str, local_path: Path) -> bool:
    """Download a file from R2 to local path.
    
    Returns True if successful, False if file doesn't exist or R2 is disabled.
    """
    if not R2_ENABLED:
        return False
    
    try:
        client = _get_s3_client()
        client.download_file(R2_BUCKET, remote_key, str(local_path))
        print(f"[R2] Downloaded {remote_key} -> {local_path}")
        return True
    except Exception as e:
        # File might not exist yet, which is fine
        if "404" in str(e) or "NoSuchKey" in str(e):
            print(f"[R2] File {remote_key} not found in R2, using local if exists")
        else:
            print(f"[R2] Error downloading {remote_key}: {e}")
        return False


def upload_to_r2(local_path: Path, remote_key: str) -> bool:
    """Upload a file from local path to R2.
    
    Returns True if successful, False if R2 is disabled or upload failed.
    """
    if not R2_ENABLED:
        return False
    
    if not local_path.exists():
        print(f"[R2] Local file {local_path} does not exist, skipping upload")
        return False
    
    try:
        client = _get_s3_client()
        client.upload_file(str(local_path), R2_BUCKET, remote_key)
        print(f"[R2] Uploaded {local_path} -> {remote_key}")
        return True
    except Exception as e:
        print(f"[R2] Error uploading {local_path}: {e}")
        return False


def sync_from_r2():
    """Download actuals.csv and attendance.db from R2 on startup."""
    if not R2_ENABLED:
        print("[R2] R2 storage not configured, using local files only")
        return
    
    print("[R2] Syncing files from Cloudflare R2...")
    
    # Download actuals.csv
    actuals_path = Path("actuals.csv")
    download_from_r2("actuals.csv", actuals_path)
    
    # Download attendance.db
    db_path = Path("attendance.db")
    download_from_r2("attendance.db", db_path)


def sync_actuals_to_r2() -> bool:
    """Upload actuals.csv to R2 after it's updated."""
    return upload_to_r2(Path("actuals.csv"), "actuals.csv")


def sync_db_to_r2() -> bool:
    """Upload attendance.db to R2 after database changes."""
    return upload_to_r2(Path("attendance.db"), "attendance.db")


def is_r2_enabled() -> bool:
    """Check if R2 storage is enabled."""
    return R2_ENABLED
