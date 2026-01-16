import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, to_timestamp
from config.config import LOCAL_RAW_DIR, LOCAL_CURATED_DIR

TABLES = {
    "customers": ["customers"],
    "orders": ["orders"],
    "order_items": ["order_items"],
    "order_reviews": ["order_reviews"],
}

def find_file(keyword_list):
    files = [f for f in os.listdir(LOCAL_RAW_DIR) if f.lower().endswith(".csv")]
    for f in files:
        lower = f.lower()
        for kw in keyword_list:
            if kw in lower:
                return os.path.join(LOCAL_RAW_DIR, f)
    return None

def main():
    os.makedirs(LOCAL_CURATED_DIR, exist_ok=True)

    spark = SparkSession.builder.appName("olist-local-csv-to-parquet").getOrCreate()

    for table, keywords in TABLES.items():
        in_file = find_file(keywords)
        if not in_file:
            raise RuntimeError(f"Could not find CSV for table '{table}' in {LOCAL_RAW_DIR}")

        out_dir = os.path.join(LOCAL_CURATED_DIR, table)

        df = spark.read.option("header", True).option("inferSchema", True).csv(in_file)

        # Trim all string columns
        for c in df.columns:
            df = df.withColumn(c, trim(col(c)))

        # Optional: parse timestamps if present
        if "order_purchase_timestamp" in df.columns:
            df = df.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))

        df.write.mode("overwrite").parquet(out_dir)
        print(f"Created Parquet for {table}: {out_dir}")

    spark.stop()
    print("All Parquet outputs created.")

if __name__ == "__main__":
    main()