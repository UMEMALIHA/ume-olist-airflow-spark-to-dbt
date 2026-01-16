import os
import sys
import duckdb

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.config import LOCAL_RAW_DIR, LOCAL_CURATED_DIR

def find_csv(keyword):
    for f in os.listdir(LOCAL_RAW_DIR):
        lf = f.lower()
        if lf.endswith(".csv") and keyword in lf:
            return os.path.join(LOCAL_RAW_DIR, f)
    return None

def main():
    os.makedirs(LOCAL_CURATED_DIR, exist_ok=True)
    con = duckdb.connect()

    mapping = {
        "customers": "customer",
        "orders": "orders",
        "order_items": "order_items",
        "order_reviews": "order_reviews",
    }

    for table, kw in mapping.items():
        csv_path = find_csv(kw)
        if not csv_path:
            raise RuntimeError(f"Could not find CSV for {table} in {LOCAL_RAW_DIR}")

        out_dir = os.path.join(LOCAL_CURATED_DIR, table)
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, f"{table}.parquet")

        print(f"[RUN] {csv_path} -> {out_file}")
        con.execute(f"""
            COPY (
                SELECT * FROM read_csv_auto('{csv_path}')
            )
            TO '{out_file}' (FORMAT PARQUET);
        """)

    print("[DONE] DuckDB CSV -> Parquet complete")

if __name__ == "__main__":
    main()
