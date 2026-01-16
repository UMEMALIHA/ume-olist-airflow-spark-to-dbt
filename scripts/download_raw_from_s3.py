import os
import sys

# Ensure project root is on Python path (Windows-safe)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import boto3
from config.config import AWS_REGION, RAW_BUCKET, RAW_PREFIX, LOCAL_RAW_DIR

s3 = boto3.client("s3", region_name=AWS_REGION)

def main():
    os.makedirs(LOCAL_RAW_DIR, exist_ok=True)

    resp = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=RAW_PREFIX)
    if "Contents" not in resp:
        raise RuntimeError(f"No files found at s3://{RAW_BUCKET}/{RAW_PREFIX}")

    for obj in resp["Contents"]:
        key = obj["Key"]
        if not key.lower().endswith(".csv"):
            continue

        filename = os.path.basename(key)
        local_path = os.path.join(LOCAL_RAW_DIR, filename)

        print(f"Downloading s3://{RAW_BUCKET}/{key} -> {local_path}")
        s3.download_file(RAW_BUCKET, key, local_path)

    print("Download complete.")

if __name__ == "__main__":
    main()
