#!/usr/bin/env python3
"""Backup attendance.db to S3 as a gzipped file.

Environment variables required:
- S3_BUCKET (bucket name)
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_REGION (optional, default us-east-1)

Usage:
    python3 scripts/backup_sqlite_to_s3.py
"""
import os
import sys
import gzip
import shutil
from datetime import datetime
from pathlib import Path

def main():
    bucket = os.environ.get("S3_BUCKET")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    region = os.environ.get("AWS_REGION", "us-east-1")

    if not bucket or not access_key or not secret:
        print("Missing S3_BUCKET or AWS credentials in environment. Aborting.")
        sys.exit(1)

    try:
        import boto3
    except Exception as e:
        print("boto3 is required. Install it via requirements.txt. Error:", e)
        sys.exit(1)

    db_path = Path("attendance.db")
    if not db_path.exists():
        print(f"Database file not found at {db_path}. Aborting.")
        sys.exit(1)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    local_tmp = Path("/tmp") / f"attendance-{timestamp}.db"
    gzip_path = Path("/tmp") / f"attendance-{timestamp}.db.gz"

    # Copy DB to /tmp to avoid locking issues
    shutil.copy2(db_path, local_tmp)

    # Gzip the copy
    with open(local_tmp, "rb") as f_in, gzip.open(gzip_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    key = f"backups/attendance-{timestamp}.db.gz"

    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret,
        region_name=region,
    )

    print(f"Uploading {gzip_path} to s3://{bucket}/{key} ...")
    s3.upload_file(str(gzip_path), bucket, key)

    print("Upload complete. Cleaning up local temp files.")
    try:
        local_tmp.unlink()
        gzip_path.unlink()
    except Exception:
        pass

    print("Backup finished: s3://{}/{}".format(bucket, key))


if __name__ == "__main__":
    main()
