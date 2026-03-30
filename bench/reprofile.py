import os
import time
import subprocess

def run_query(sql, acc=0.99):
    cmd = ["cargo", "run", "--release", "--", "query", sql, "--accuracy", str(acc)]
    print(f"\n--- Running AQE Query: {sql} (acc={acc}) ---")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd="/home/tba/projects/lsm")
    print(result.stdout)
    if result.stderr:
        print(f"STDERR: {result.stderr}")

if __name__ == "__main__":
    # Ensure release build is up to date
    print("Building release...")
    subprocess.run(["cargo", "build", "--release"], cwd="/home/tba/projects/lsm")

    # Q1: SELECT COUNT(*) FROM logs GROUP BY country
    run_query("SELECT COUNT(*) FROM logs GROUP BY country", acc=0.99)

    # Q2: SELECT SUM(level) FROM logs WHERE level > 0
    run_query("SELECT SUM(level) FROM logs WHERE level > 0", acc=0.999)

    # Q3: SELECT COUNT(*) FROM logs WHERE level > 500
    run_query("SELECT COUNT(*) FROM logs WHERE level > 500", acc=0.70)
