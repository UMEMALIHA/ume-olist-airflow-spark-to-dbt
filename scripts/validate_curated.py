import os
import sys
import duckdb

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.config import LOCAL_CURATED_DIR

def main():
    con = duckdb.connect()

    # Works for both Spark output (folder of part files) and DuckDB output (single parquet)
    tables = ["customers", "orders", "order_items", "order_reviews"]

    for t in tables:
        path = os.path.join(LOCAL_CURATED_DIR, t)
        # DuckDB can read parquet files under a folder pattern
        parquet_glob = os.path.join(path, "*.parquet")

        # Spark writes many part files; DuckDB writes one file; glob covers both
        cnt = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_glob}')").fetchone()[0]
        print(f"{t}: {cnt} rows")

    print("[DONE] Validation complete")

if __name__ == "__main__":
    main()
