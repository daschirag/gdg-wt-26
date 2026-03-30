import sqlite3
import subprocess
import time
import re
import os

CSV_PATH = "/tmp/lsm_bench.csv"

def run_bench():
    print("Building Rust AQE release...")
    subprocess.run(["cargo", "build", "--release"], check=True)
    
    print("\n--- Running Rust AQE (and generating dataset) ---")
    rust_gen_process = subprocess.run(
        ["target/release/lsm", "--dump-csv", CSV_PATH],
        capture_output=True, text=True
    )
    
    print("Executing GROUP BY query on Rust AQE...")
    start_time_rust = time.perf_counter()
    rust_process = subprocess.run(
        ["target/release/lsm", "--query-only"],
        capture_output=True, text=True
    )
    rust_duration = (time.perf_counter() - start_time_rust) * 1000
    
    # Extract execution time and group results
    rust_output = rust_process.stdout
    print(rust_output)
    print(f"Rust AQE GROUP BY Execution Time (Python timer): {rust_duration:.3f}ms")

    print("\n--- Running SQLite on the same dataset ---")
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE users (
            user_id INTEGER,
            status INTEGER,
            country INTEGER,
            timestamp INTEGER
        )
    """)
    
    # Load CSV
    print("Loading CSV into SQLite...")
    with open(CSV_PATH, "r") as f:
        next(f) # skip header
        rows = [line.strip().split(",") for line in f]
        cursor.executemany("INSERT INTO users VALUES (?, ?, ?, ?)", rows)
    
    print("Executing GROUP BY query on SQLite...")
    start_time = time.perf_counter()
    cursor.execute("SELECT country, COUNT(*) FROM users GROUP BY country")
    results = cursor.fetchall()
    sqlite_duration = (time.perf_counter() - start_time) * 1000
    
    print(f"SQLite Execution Time: {sqlite_duration:.3f}ms")
    print("SQLite Results (Exact):")
    for row in results:
         print(f"  Country {row[0]}: {row[1]}")

if __name__ == "__main__":
    run_bench()
