import subprocess
import time
import re
import os
import sqlite3

class StressTester:
    def __init__(self, binary_path="target/release/lsm"):
        self.binary_path = binary_path
        self._set_config(0.95)
        print("Building Rust binary in release mode...")
        subprocess.run(["cargo", "build", "--release"], check=True)

    def _set_config(self, accuracy_target):
        config_data = f"""
accuracy_target = {accuracy_target}
k = 2.0
low_confidence_threshold = 100
seed = 42
bloom_fpr = 0.01
min_group_rows = 50
memtable_row_limit = 1048576
memtable_size_limit = 104857600
verify_crc = false

# Analytical engine params
compaction_strategy = "stcs"
compaction_interval_secs = 30
min_compaction_files = 4
row_group_size = 1024
lcs_l1_max_bytes = 10485760
lcs_l2_max_bytes = 104857600
"""
        with open("config.toml", "w") as f:
            f.write(config_data.strip())

    def run_gen(self, num_rows, csv_path):
        print(f"Generating {num_rows:,} rows via Python tool...")
        start = time.perf_counter()
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        
        # Clean old data to prevent mixing (only AQE data, keeping CSV)
        subprocess.run("rm -rf data/*.aqe data/segments/*", shell=True)
        
        # 1. Run Python Generator (Skipping because the 10M row CSV is already generated)
        print("Data already generated, skipping Python generator...")
        # subprocess.run(["python3", "tools/gen_data.py", "--num", str(num_rows), "--output", csv_path], check=True, capture_output=False)
        
        # 2. Run Rust Loader
        print("Loading CSV into LSM storage...")
        subprocess.run([self.binary_path, "load", csv_path], check=True, capture_output=False)
        
        # 3. Compact to Columnar
        print("Compacting to columnar format...")
        subprocess.run([self.binary_path, "compact"], check=True, capture_output=False)
        
        duration = time.perf_counter() - start
        print(f"Generation, Load & Compaction completed in {duration:.2f}s")
        return duration

    def run_query(self, sql, accuracy):
        self._set_config(accuracy)
        start = time.perf_counter()
        result = subprocess.run([self.binary_path, "query", sql], capture_output=True, text=True)
        duration = (time.perf_counter() - start) * 1000
        
        # Parse rows scanned
        rows_match = re.search(r"Rows Scanned: (\d+)", result.stdout)
        rows_read = int(rows_match.group(1)) if rows_match else 0
        
        conf_map = {"High": 0.99, "Low": 0.5, "Exact": 1.0}
        
        # Parse scalar result
        scalar_match = re.search(r"SCALAR: ([\d.-]+)\|(\w+)", result.stdout)
        if scalar_match:
            return float(scalar_match.group(1)), conf_map.get(scalar_match.group(2), 0.0), duration, rows_read
        
        # Parse group results
        groups = {}
        for line in result.stdout.splitlines():
            if line.startswith("GROUP:"):
                parts = line.replace("GROUP: ", "").split("|")
                # format: key | value | conf_enum
                if len(parts) >= 3:
                    val = float(parts[1].strip())
                    conf = conf_map.get(parts[2].strip(), 0.0)
                    groups[parts[0].strip()] = (val, conf)
        
        if groups:
            return groups, None, duration, rows_read
            
        return None, None, duration, rows_read

def run_stress_test():
    tester = StressTester()
    NUM_ROWS = 10_000_000
    CSV_PATH = "data/stress_test.csv"
    
    tester.run_gen(NUM_ROWS, CSV_PATH)

    print("\n--- SQLite Baseline Setup (10M Rows, Disk-Backed) ---")
    start_sqlite = time.perf_counter()
    db_path = "data/sqlite_bench.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Speed up inserts for the sake of the benchmark setup
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA journal_mode = MEMORY")
    
    # Correct schema for SQLite based on our schema.toml (+ the new 'level' column)
    cursor.execute("CREATE TABLE users (user_id INTEGER, status INTEGER, country INTEGER, timestamp INTEGER, level INTEGER)")
    
    with open(CSV_PATH, "r") as f:
        next(f) # skip header
        count = 0
        batch = []
        for line in f:
            batch.append(line.strip().split(","))
            count += 1
            if count % 100000 == 0:
                cursor.executemany("INSERT INTO users VALUES (?, ?, ?, ?, ?)", batch)
                conn.commit()
                batch = []
    if batch:
        cursor.executemany("INSERT INTO users VALUES (?, ?, ?, ?, ?)", batch)
        conn.commit()
    
    sqlite_setup_time = time.perf_counter() - start_sqlite
    print(f"SQLite Loaded 10M rows in {sqlite_setup_time:.2f}s")

    queries = [
        ("SELECT COUNT(*) FROM users WHERE status = 0", "COUNT WHERE STATUS=0"),
        ("SELECT SUM(level) FROM users WHERE country = 1", "SUM WHERE COUNTRY=1"),
        ("SELECT AVG(timestamp) FROM users", "AVG GLOBAL"),
        ("SELECT COUNT(*) FROM users GROUP BY country", "COUNT GRP BY COUNTRY")
    ]

    print("\n--- Benchmarking Queries (Rust AQE vs SQLite) ---")
    print(f"{'Query Type':<25} | {'Source':<10} | {'Result':<32} | {'Time (ms)':<10} | {'Conf':<8} | {'Rows':<8}")
    print("-" * 105)

    for sql, label in queries:
        # SQLite Exact
        s_start = time.perf_counter()
        
        sqlite_sql = sql
        if "GROUP BY" in sql.upper():
            group_col = sql.split("GROUP BY")[1].strip()
            sqlite_sql = sql.replace("SELECT ", f"SELECT {group_col}, ")
            
        cursor.execute(sqlite_sql)
        res_rows = cursor.fetchall()
        s_duration = (time.perf_counter() - s_start) * 1000
        
        if len(res_rows) == 1 and len(res_rows[0]) == 1:
            # Scalar
            s_res_str = f"{res_rows[0][0]:.2f}"
            print(f"{label:<25} | {'SQLite':<10} | {s_res_str:<32} | {s_duration:<10.2f} | {'1.0000':<8} | {'10.0M':<8}")
        else:
            # Group By
            s_dict = {str(row[0]): row[1] for row in res_rows}
            s_res_str = str(s_dict)
            print(f"{label:<25} | {'SQLite':<10} | {s_res_str:<32} | {s_duration:<10.2f} | {'1.0000':<8} | {'10.0M':<8}")

        # Rust AQE across accuracies
        for acc in [0.7, 0.9, 0.99]:
            r_val, r_conf, r_duration, r_rows = tester.run_query(sql, acc)
            if r_val is not None:
                row_str = f"{r_rows/1000:,.1f}k" if r_rows >= 1000 else str(r_rows)
                if isinstance(r_val, dict):
                    # For group by, format the string and extract min conf
                    r_str = str({k: f"{v[0]:.2f}" for k, v in r_val.items()})
                    min_conf = min(v[1] for v in r_val.values()) if r_val else 0.0
                    print(f"{label:<25} | {'AQE ' + str(acc):<10} | {r_str:<32} | {r_duration:<10.2f} | {min_conf:<8.4f} | {row_str:<8}")
                else:
                    r_str = f"{r_val:.2f}"
                    print(f"{label:<25} | {'AQE ' + str(acc):<10} | {r_str:<32} | {r_duration:<10.2f} | {r_conf:<8.4f} | {row_str:<8}")

    print("\nStress Test Complete.")

if __name__ == "__main__":
    run_stress_test()
