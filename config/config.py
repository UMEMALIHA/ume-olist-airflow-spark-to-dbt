import os

# Project root directory 
PROJECT_ROOT = r"C:\ume-olist-poc"

# Local data directories
LOCAL_RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
LOCAL_CURATED_DIR = os.path.join(PROJECT_ROOT, "data", "curated")

# AWS S3 settings 
AWS_REGION = "us-east-1"  
RAW_BUCKET = "ume-demo-olist-raw"  
CURATED_S3_PREFIX = "olist/curated/"

# Toggle S3 upload 
UPLOAD_CURATED_TO_S3 = True
