import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, to_timestamp

def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # Spark 3.1 + Java 11 safe
        .getOrCreate()
    )

def find_csv(raw_dir: str, keywords):
    files = [f for f in os.listdir(raw_dir) if f.lower().endswith(".csv")]
    for f in files:
        lower = f.lower()
        for kw in keywords:
            if kw in lower:
                return os.path.join(raw_dir, f)
    return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw_dir", required=True)
    parser.add_argument("--curated_dir", required=True)
    args = parser.parse_args()

    raw_dir = args.raw_dir
    curated_dir = args.curated_dir
    os.makedirs(curated_dir, exist_ok=True)

    tables = {
        "customers": ["customer", "customers"],
        "orders": ["order", "orders"],
        "order_items": ["order_items", "items"],
        "order_reviews": ["order_reviews", "reviews"],
    }

    spark = build_spark("olist-csv-to-parquet-spark31")

    for table, keywords in tables.items():
        csv_path = find_csv(raw_dir, keywords)
        if not csv_path:
            raise RuntimeError(f"Could not find CSV for '{table}' in {raw_dir}")

        out_path = os.path.join(curated_dir, table)

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(csv_path)
        )

        for c in df.columns:
            df = df.withColumn(c, trim(col(c)))

        if "order_purchase_timestamp" in df.columns:
            df = df.withColumn(
                "order_purchase_timestamp",
                to_timestamp(col("order_purchase_timestamp"))
            )

        df.write.mode("overwrite").parquet(out_path)
        print(f"[OK] Wrote Parquet for {table} -> {out_path}")

    spark.stop()
    print("[DONE] All Parquet outputs created successfully.")

if __name__ == "__main__":
    main()
