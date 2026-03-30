import sqlite3
import subprocess
import time
import re
import os

CSV_PATH = "data/compare_bench.csv"

def run_query_bench(cursor, sql_rust, sql_sqlite, label, accuracy=0.7):
    print(f"\n--- Benchmarking: {label} (Target Accuracy: {accuracy}) ---")
    
    # 1. Update config accuracy
    with open("config.toml", "r") as f:
        content = f.read()
    new_content = re.sub(r'accuracy_target = [\d.]+', f'accuracy_target = {accuracy}', content)
    with open("config.toml", "w") as f:
        f.write(new_content)

    # 2. Rust AQE
    start_rust = time.perf_counter()
    rust_process = subprocess.run(
        ["target/release/lsm", "query", sql_rust],
        capture_output=True, text=True
    )
    rust_duration = (time.perf_counter() - start_rust) * 1000
    
    # Parse Rows Scanned
    rows_scanned = 0
    rows_match = re.search(r"Rows Scanned: (\d+)", rust_process.stdout)
    if rows_match:
        rows_scanned = int(rows_match.group(1))
    
    # Parse actual sampling rate reported by engine
    rate_match = re.search(r"Effective Sampling Rate: ([\d.]+)", rust_process.stdout)
    eff_rate = float(rate_match.group(1)) if rate_match else 1.0

    print(f"Rust AQE: {rust_duration:.2f}ms (Scanned: {rows_scanned:,}, Rate: {eff_rate:.4f})")

    # 3. SQLite
    start_sql = time.perf_counter()
    cursor.execute(sql_sqlite)
    res = cursor.fetchall()
    sqlite_duration = (time.perf_counter() - start_sql) * 1000
    print(f"SQLite:   {sqlite_duration:.2f}ms")
    
    return {
        "label": label,
        "acc": accuracy,
        "rust_ms": rust_duration,
        "sqlite_ms": sqlite_duration,
        "rows": rows_scanned,
        "rate": eff_rate
    }

def run_bench():
    print("Building Rust AQE release...")
    subprocess.run(["cargo", "build", "--release"], check=True)
    
    # 1. Generate Data with Python
    num_rows = 1000000
    print(f"\n--- Generating Dataset ({num_rows:,} rows) ---")
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    subprocess.run(["python3", "tools/gen_data.py", "--num", str(num_rows), "--output", CSV_PATH], check=True)
    
    # 2. Load into LSM
    print("Loading CSV into LSM storage...")
    subprocess.run(["target/release/lsm", "load", CSV_PATH], check=True, capture_output=True)
    
    print("Compacting storage for optimal scan performance...")
    subprocess.run(["target/release/lsm", "compact"], check=True, capture_output=True)
    
    # 3. Running SQLite
    print("\n--- Initializing SQLite on the same dataset ---")
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE users (
            user_id INTEGER,
            status INTEGER,
            country INTEGER,
            timestamp INTEGER,
            level INTEGER
        )
    """)
    
    # Load CSV
    print("Loading CSV into SQLite...")
    with open(CSV_PATH, "r") as f:
        next(f) # skip header
        rows = [line.strip().split(",") for line in f]
        cursor.executemany("INSERT INTO users VALUES (?, ?, ?, ?, ?)", rows)
    conn.commit()
    
    # 4. Run Benchmark Queries with Variable Accuracy
    benchmark_runs = []
    
    queries = [
        ("SELECT COUNT(*) FROM users GROUP BY country", "SELECT country, COUNT(*) FROM users GROUP BY country", "GROUP BY country"),
        ("SELECT COUNT(*) FROM users WHERE level > 500", "SELECT COUNT(*) FROM users WHERE level > 500", "Scan: level > 500"),
        ("SELECT SUM(level) FROM users WHERE status = 0", "SELECT SUM(level) FROM users WHERE status = 0", "SUM level WHERE status=0")
    ]
    
    accuracies = [0.7, 0.9, 0.99]
    
    for sql_r, sql_s, label in queries:
        for acc in accuracies:
            benchmark_runs.append(run_query_bench(cursor, sql_r, sql_s, label, acc))

    print("\n" + "="*85)
    print(f"{'Query':<25} | {'Acc':<5} | {'Rust (ms)':<10} | {'SQLite (ms)':<12} | {'Rows Scanned':<12} | {'Rate':<6}")
    print("-" * 85)
    for run in benchmark_runs:
        print(f"{run['label']:<25} | {run['acc']:<5.2f} | {run['rust_ms']:<10.2f} | {run['sqlite_ms']:<12.2f} | {run['rows']:<12,} | {run['rate']:<6.4f}")
    print("="*85)

if __name__ == "__main__":
    run_bench()
