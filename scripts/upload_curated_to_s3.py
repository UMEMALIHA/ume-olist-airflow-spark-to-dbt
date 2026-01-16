import os
import sys
import boto3
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.config import AWS_REGION, RAW_BUCKET, CURATED_S3_PREFIX, LOCAL_CURATED_DIR

s3 = boto3.client("s3", region_name=AWS_REGION)

def upload_directory(local_dir: str, bucket: str, prefix: str):
    base = Path(local_dir)
    for p in base.rglob("*"):
        if p.is_dir():
            continue
        rel = p.relative_to(base).as_posix()
        key = f"{prefix}{rel}"
        print(f"Uploading {p} -> s3://{bucket}/{key}")
        s3.upload_file(str(p), bucket, key)

def main():
    if not os.path.exists(LOCAL_CURATED_DIR):
        raise RuntimeError(f"Curated dir missing: {LOCAL_CURATED_DIR}")
    upload_directory(LOCAL_CURATED_DIR, RAW_BUCKET, CURATED_S3_PREFIX)
    print("[DONE] Upload complete")

if __name__ == "__main__":
    main()
