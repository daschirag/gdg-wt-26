import os
import subprocess
import re
import sys

def run_aqe(sql, acc):
    print(f"\n=== AQE QUERY: {sql} (acc={acc}) ===")
    cmd = ["cargo", "run", "--release", "--", "query", sql, "--accuracy", str(acc)]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd="/home/tba/projects/lsm")
    print(result.stdout)
    for line in result.stderr.split('\n'):
        if "[PROFILE]" in line or "Rows Scanned" in line:
            print(line)
    return result.stdout, result.stderr

def run_sqlite(sql):
    print(f"\n=== SQLITE QUERY (No Index): {sql} ===")
    script_path = "/tmp/sqlite_query.sql"
    with open(script_path, "w") as f:
        f.write(".timer on\n")
        f.write(f"{sql};\n")
    
    cmd = ["sqlite3", "data/sqlite_bench.db", ".read " + script_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)
    return result.stdout, result.stderr

if __name__ == "__main__":
    # Ensure SQLite index is dropped
    subprocess.run(["sqlite3", "data/sqlite_bench.db", "DROP INDEX IF EXISTS idx_level;"])

    sql_sum = "SELECT SUM(level) FROM lsm WHERE level > 500"
    
    print("\n--- MULTI-TIER PERFORMANCE (LSM vs SQLite Full Scan) ---")
    run_aqe(sql_sum, 0.5)  # Fast
    run_aqe(sql_sum, 1.0)  # Full Scan
    run_sqlite(sql_sum)    # SQLite Full Scan
