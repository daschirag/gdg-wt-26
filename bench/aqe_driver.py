import subprocess
import time
import re
import os
import sqlite3

class AQEDriver:
    def __init__(self, binary_path="target/release/lsm", data_dir="data"):
        self.binary_path = binary_path
        self.data_dir = data_dir
        self._set_config(0.9)
        # Ensure project is built
        subprocess.run(["cargo", "build", "--release"], check=True)

    def _set_config(self, accuracy_target):
        # We rewrite config.toml before executing querying to change accuracy on the fly
        config_data = f"""
accuracy_target = {accuracy_target}
k = 2.0
low_confidence_threshold = 100
seed = 42
bloom_fpr = 0.01
min_group_rows = 5
memtable_row_limit = 1000000
memtable_size_limit = 104857600
"""
        with open("config.toml", "w") as f:
            f.write(config_data.strip())

    def insert(self, num_rows, dump_csv_path=None):
        cmd = [self.binary_path, "gen", str(num_rows)]
        if dump_csv_path:
            cmd.append(dump_csv_path)
            
        start_time = time.perf_counter()
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        py_duration = (time.perf_counter() - start_time) * 1000
        
        gen_time = re.search(r"GEN_TIME: (\d+\.?\d*)ms|GEN_TIME: (\d+\.?\d*)s", result.stdout)
        flush_time = re.search(r"FLUSH_TIME: (\d+\.?\d*)ms|FLUSH_TIME: (\d+\.?\d*)s", result.stdout)
        
        g_t = float(gen_time.group(1)) if gen_time.group(1) else float(gen_time.group(2))*1000 if gen_time else 0.0
        f_t = float(flush_time.group(1)) if flush_time.group(1) else float(flush_time.group(2))*1000 if flush_time else 0.0
        
        return {
            "gen_time_ms": g_t,
            "flush_time_ms": f_t,
            "total_process_time_ms": py_duration
        }

    def execute(self, sql, accuracy_target=None):
        if accuracy_target is not None:
            self._set_config(accuracy_target)

        start_time = time.perf_counter()
        result = subprocess.run([self.binary_path, "query", sql], capture_output=True, text=True, check=True)
        py_duration = (time.perf_counter() - start_time) * 1000
        
        # Parse output
        rust_time_match = re.search(r"Execution Time: (\d+\.?\d*)ms|Execution Time: (\d+\.?\d*)s|Execution Time: (\d+\.?\d*)µs", result.stdout)
        rust_internal_ms = 0.0
        if rust_time_match:
            if rust_time_match.group(1):
                rust_internal_ms = float(rust_time_match.group(1))
            elif rust_time_match.group(2):
                rust_internal_ms = float(rust_time_match.group(2)) * 1000.0
            elif rust_time_match.group(3):
                rust_internal_ms = float(rust_time_match.group(3)) / 1000.0
        
        groups = {}
        scalar = None
        for line in result.stdout.splitlines():
            if line.startswith("GROUP:"):
                parts = line.replace("GROUP: ", "").split("|")
                groups[parts[0].strip()] = {"value": float(parts[1]), "confidence": parts[2].strip()}
            elif line.startswith("SCALAR:"):
                parts = line.replace("SCALAR: ", "").split("|")
                scalar = {"value": float(parts[0]), "confidence": parts[1].strip()}
        
        return {
            "results": groups if groups else scalar,
            "rust_internal_time_ms": rust_internal_ms,
            "total_process_time_ms": py_duration
        }

def run_benchmarks():
    print("Initializing AQE Python Driver...")
    driver = AQEDriver()
    
    CSV_PATH = "/tmp/lsm_dynamic_bench.csv"
    NUM_ROWS = 1_000_000  # We test with 1M rows for clear distinctions!
    
    print(f"\\n--- [INSERT/GENERATE] 1M Rows ---")
    insert_stats = driver.insert(NUM_ROWS, CSV_PATH)
    print(f"Rust AQE Memory Gen Time: {insert_stats['gen_time_ms']:.2f} ms")
    print(f"Rust AQE Flush Time (10 SSTables): {insert_stats['flush_time_ms']:.2f} ms")
    print(f"Total Subprocess Overhead: {insert_stats['total_process_time_ms']:.2f} ms")
    
    print(f"\\n--- [INSERT/GENERATE] SQLite Baseline (1M Rows) ---")
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (user_id INTEGER, status INTEGER, country INTEGER, timestamp INTEGER)")
    
    # SQLite insert from CSV measure
    start_sqlite_insert = time.perf_counter()
    with open(CSV_PATH, "r") as f:
        next(f)
        rows = [line.strip().split(",") for line in f]
        cursor.executemany("INSERT INTO users VALUES (?, ?, ?, ?)", rows)
    sqlite_insert_time = (time.perf_counter() - start_sqlite_insert) * 1000
    print(f"SQLite Full Insert Time (from CSV): {sqlite_insert_time:.2f} ms")

    # Benchmarking query reading times across accuracy targets
    sql = "SELECT COUNT(*) FROM users GROUP BY country"
    accuracies = [0.1, 0.5, 0.9, 0.95, 0.99]
    print(f"\\n--- [READ/EXECUTE] AQE vs SQLite  ---")
    print(f"Query: {sql}\\n")
    
    # Exact SQLite Time
    start_sqlite_q = time.perf_counter()
    cursor.execute("SELECT country, COUNT(*) FROM users GROUP BY country")
    results = cursor.fetchall()
    sqlite_q_time = (time.perf_counter() - start_sqlite_q) * 1000
    
    print(f"SQLite (Exact, In-Memory): {sqlite_q_time:.2f} ms")
    for row in results:
        print(f"    Country {row[0]}: {row[1]}")
    
    print("\\nRust AQE Estimations:")
    for acc in accuracies:
        print(f"\\nTarget Accuracy: {acc}")
        stats = driver.execute(sql, accuracy_target=acc)
        print(f"  Internal Time: {stats['rust_internal_time_ms']:.2f} ms")
        print(f"  Total Process Time: {stats['total_process_time_ms']:.2f} ms")
        
        # Display all group results with confidence
        if isinstance(stats['results'], dict):
            for country, data in stats['results'].items():
                print(f"  Country {country} Est: {data['value']} (Confidence: {data['confidence']})")

if __name__ == "__main__":
    run_benchmarks()
