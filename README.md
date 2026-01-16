# Olist Data Pipeline: Migration from Airflow + Spark to dbt 

## Overview
This project demonstrates a modern ELT pipeline design where transformation logic is migrated from Apache Spark jobs and Airflow workflows into dbt models.

The implementation uses the Brazilian Olist e-commerce dataset (4 CSV files) stored in Amazon S3. The pipeline produces a curated Parquet layer and then builds analytics-ready tables using dbt, including data quality tests.

## Architecture (High Level)
1. Orchestration (Target): Apache Airflow (EC2) triggers the pipeline steps  
2. Transformation (Target): Apache Spark converts raw CSV → curated Parquet  
3. Analytics Layer: dbt Core builds models (staging + marts) and runs tests  
4. Storage: Amazon S3 holds raw and curated layers

### POC Execution Note
DuckDB is used as a lightweight local transformation engine to convert CSV → Parquet quickly. The curated Parquet layer and dbt logic remain Spark-compatible.

## Dataset
Brazilian Olist e-commerce dataset (Kaggle):
- customers
- orders
- order_items
- order_reviews

## Repository Structure
ume-olist-poc/
├── config/
│ └── config.py
├── scripts/
│ ├── download_raw_from_s3.py
│ ├── duckdb_csv_to_parquet.py
│ ├── upload_curated_to_s3.py
│ └── validate_curated.py
├── data/ # local only 
│ ├── raw/
│ └── curated/
├── dbt/
│ └── ume_olist/
│ ├── models/
│ │ ├── staging/
│ │ ├── marts/
│ │ └── schema.yml
│ └── dbt_project.yml
├── requirements.txt
└── README.md
