#!/usr/bin/env python3
"""
AQE vs SQLite Full Benchmark — 10M rows
Covers all query types, accuracy levels, and per-query profiling.
Run: python3 bench/full_benchmark.py
"""

import subprocess
import sqlite3
import time
import json
import csv
import os
import sys
import random
import struct
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional

# ── Config ────────────────────────────────────────────────────────────────────

AQE_BIN      = "target/release/lsm"
DATA_DIR     = "data"
DB_PATH      = "data/bench_10m.db"
LOG_CSV      = "bench/results_10m.csv"
LOG_JSON     = "bench/results_10m.json"
NUM_ROWS     = 10_000_000
SEED         = 42
ACCURACIES   = [0.70, 0.90, 0.95, 0.99]

# Dataset schema
# user_id:   u64  sequential
# status:    u8   0=active(70%) 1=inactive(20%) 2=banned(10%)
# country:   u8   0..4 uniform
# level:     i64  0..1000 uniform
# timestamp: i64  unix ms 2020..2024

QUERIES = [
    # (label, sql, aqe_query, category)

    # ── Metadata fast path (should be ~2ms) ──────────────────────────────────
    ("AVG_GLOBAL",
     "SELECT AVG(level) FROM logs",
     "SELECT AVG(level) FROM logs",
     "metadata_fastpath"),

    ("COUNT_GLOBAL",
     "SELECT COUNT(*) FROM logs",
     "SELECT COUNT(*) FROM logs",
     "metadata_fastpath"),

    ("SUM_GLOBAL",
     "SELECT SUM(level) FROM logs",
     "SELECT SUM(level) FROM logs",
     "metadata_fastpath"),

    # ── Equality filter ───────────────────────────────────────────────────────
    ("COUNT_WHERE_STATUS_0",
     "SELECT COUNT(*) FROM logs WHERE status = 0",
     "SELECT COUNT(*) FROM logs WHERE status = 0",
     "equality_filter"),

    ("COUNT_WHERE_STATUS_2",
     "SELECT COUNT(*) FROM logs WHERE status = 2",
     "SELECT COUNT(*) FROM logs WHERE status = 2",
     "equality_filter"),

    ("COUNT_WHERE_COUNTRY_1",
     "SELECT COUNT(*) FROM logs WHERE country = 1",
     "SELECT COUNT(*) FROM logs WHERE country = 1",
     "equality_filter"),

    ("SUM_WHERE_COUNTRY_1",
     "SELECT SUM(level) FROM logs WHERE country = 1",
     "SELECT SUM(level) FROM logs WHERE country = 1",
     "equality_filter"),

    ("AVG_WHERE_STATUS_0",
     "SELECT AVG(level) FROM logs WHERE status = 0",
     "SELECT AVG(level) FROM logs WHERE status = 0",
     "equality_filter"),

    # ── Range filter ─────────────────────────────────────────────────────────
    ("COUNT_LEVEL_GT_500",
     "SELECT COUNT(*) FROM logs WHERE level > 500",
     "SELECT COUNT(*) FROM logs WHERE level > 500",
     "range_filter"),

    ("COUNT_LEVEL_LT_200",
     "SELECT COUNT(*) FROM logs WHERE level < 200",
     "SELECT COUNT(*) FROM logs WHERE level < 200",
     "range_filter"),

    ("SUM_LEVEL_GT_750",
     "SELECT SUM(level) FROM logs WHERE level > 750",
     "SELECT SUM(level) FROM logs WHERE level > 750",
     "range_filter"),

    ("COUNT_TIMESTAMP_RANGE",
     "SELECT COUNT(*) FROM logs WHERE timestamp > 1672531200000",
     "SELECT COUNT(*) FROM logs WHERE timestamp > 1672531200000",
     "range_filter"),

    # ── GROUP BY ─────────────────────────────────────────────────────────────
    ("COUNT_GROUP_BY_COUNTRY",
     "SELECT country, COUNT(*) FROM logs GROUP BY country",
     "SELECT COUNT(*) FROM logs GROUP BY country",
     "group_by"),

    ("COUNT_GROUP_BY_STATUS",
     "SELECT status, COUNT(*) FROM logs GROUP BY status",
     "SELECT COUNT(*) FROM logs GROUP BY status",
     "group_by"),

    ("SUM_GROUP_BY_COUNTRY",
     "SELECT country, SUM(level) FROM logs GROUP BY country",
     "SELECT SUM(level) FROM logs GROUP BY country",
     "group_by"),

    ("AVG_GROUP_BY_STATUS",
     "SELECT status, AVG(level) FROM logs GROUP BY status",
     "SELECT AVG(level) FROM logs GROUP BY status",
     "group_by"),

    # ── Filter + GROUP BY ─────────────────────────────────────────────────────
    ("COUNT_STATUS0_GROUP_COUNTRY",
     "SELECT country, COUNT(*) FROM logs WHERE status=0 GROUP BY country",
     "SELECT COUNT(*) FROM logs WHERE status = 0 GROUP BY country",
     "filter_group_by"),

    ("SUM_LEVEL_GT500_GROUP_STATUS",
     "SELECT status, SUM(level) FROM logs WHERE level > 500 GROUP BY status",
     "SELECT SUM(level) FROM logs WHERE level > 500 GROUP BY status",
     "filter_group_by"),

    # ── High selectivity (rare values) ───────────────────────────────────────
    ("COUNT_STATUS_2_BANNED",
     "SELECT COUNT(*) FROM logs WHERE status = 2",
     "SELECT COUNT(*) FROM logs WHERE status = 2",
     "high_selectivity"),

    ("COUNT_LEVEL_GT_950",
     "SELECT COUNT(*) FROM logs WHERE level > 950",
     "SELECT COUNT(*) FROM logs WHERE level > 950",
     "high_selectivity"),
]

# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class QueryResult:
    label:           str
    category:        str
    source:          str        # sqlite | aqe_0.70 | aqe_0.90 | aqe_0.99
    accuracy:        float
    time_ms:         float
    result_value:    str
    rows_scanned:    int
    confidence:      float
    error_pct:       float      # vs sqlite exact, 0 if source=sqlite
    speedup:         float      # sqlite_ms / aqe_ms, 1.0 if source=sqlite
    bytes_read:      int
    bytes_read_pct:  float
    segments_scanned: int
    segments_total:  int
    col_files_opened: int
    row_groups_scanned: int
    row_groups_skipped: int
    filter_ms:       float
    aggregation_ms:  float
    io_ms:           float
    rayon_parallel:  bool
    storage_path:    str
    warnings:        str

# ── SQLite setup ──────────────────────────────────────────────────────────────

def generate_data_csv(path: str, n: int):
    print(f"Generating {n:,} rows → {path}")
    random.seed(SEED)
    base_ts = 1577836800000  # 2020-01-01 UTC ms
    end_ts  = 1735689600000  # 2025-01-01 UTC ms

    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["user_id","status","country","level","timestamp"])
        for i in range(n):
            status    = 0 if random.random() < 0.70 else (1 if random.random() < 0.67 else 2)
            country   = random.randint(0, 4)
            level     = random.randint(0, 1000)
            timestamp = random.randint(base_ts, end_ts)
            w.writerow([i, status, country, level, timestamp])
            if i % 1_000_000 == 0 and i > 0:
                print(f"  {i:,} rows written...")
    print(f"  Done: {n:,} rows")


def setup_sqlite(csv_path: str) -> sqlite3.Connection:
    if os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH)
        count = conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
        if count == NUM_ROWS:
            print(f"SQLite: {count:,} rows already loaded, skipping.")
            return conn
        conn.close()
        os.remove(DB_PATH)

    print(f"Loading {NUM_ROWS:,} rows into SQLite → {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=100000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            user_id   INTEGER,
            status    INTEGER,
            country   INTEGER,
            level     INTEGER,
            timestamp INTEGER
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status  ON logs(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_country ON logs(country)")

    t0 = time.time()
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        batch = []
        for i, row in enumerate(reader):
            batch.append((
                int(row["user_id"]), int(row["status"]),
                int(row["country"]), int(row["level"]),
                int(row["timestamp"])
            ))
            if len(batch) == 100_000:
                conn.executemany(
                    "INSERT INTO logs VALUES (?,?,?,?,?)", batch)
                conn.commit()
                batch = []
                if i % 1_000_000 == 0:
                    print(f"  {i:,} rows inserted ({time.time()-t0:.1f}s)")
        if batch:
            conn.executemany("INSERT INTO logs VALUES (?,?,?,?,?)", batch)
            conn.commit()

    print(f"SQLite loaded in {time.time()-t0:.1f}s")
    return conn


def run_sqlite_query(conn: sqlite3.Connection, sql: str, n_runs: int = 3) -> tuple[float, str]:
    times = []
    result = None
    for _ in range(n_runs):
        t0 = time.perf_counter()
        cur = conn.execute(sql)
        rows = cur.fetchall()
        elapsed = (time.perf_counter() - t0) * 1000
        times.append(elapsed)
        if result is None:
            if len(rows) == 1 and len(rows[0]) == 1:
                result = str(rows[0][0])
            else:
                result = str(sorted(rows))
    return min(times), result

# ── AQE runner ────────────────────────────────────────────────────────────────

def parse_aqe_profile(output: str) -> dict:
    profile = {
        "total_ms": 0.0, "bytes_read": 0, "bytes_read_pct": 0.0,
        "segments_scanned": 0, "segments_total": 0, "segments_skipped": 0,
        "col_files_opened": 0, "row_groups_scanned": 0, "row_groups_skipped": 0,
        "row_groups_evaluated": 0, "rows_scanned": 0, "rows_after_filter": 0,
        "filter_ms": 0.0, "aggregation_ms": 0.0, "io_ms": 0.0,
        "rayon_parallel": False, "storage_path": "unknown",
        "confidence": 1.0, "result_value": "", "warnings": "",
        "sst_rate": 0.0, "row_rate": 0.0, "effective_rate": 0.0,
    }

    for line in output.splitlines():
        line = line.strip()
        if line.startswith("[PROFILE] total_ms="):
            profile["total_ms"] = float(line.split("=")[1])
        elif line.startswith("[PROFILE] stage.filter_ms="):
            profile["filter_ms"] = float(line.split("=")[1])
        elif line.startswith("[PROFILE] stage.aggregation_ms="):
            profile["aggregation_ms"] = float(line.split("=")[1])
        elif line.startswith("[PROFILE] stage.col_open_ms="):
            profile["io_ms"] = float(line.split("=")[1])
        elif line.startswith("[PROFILE] io.bytes_read="):
            profile["bytes_read"] = int(line.split("=")[1])
        elif line.startswith("[PROFILE] io.col_files_opened="):
            profile["col_files_opened"] = int(line.split("=")[1])
        elif line.startswith("[PROFILE] io.row_groups_evaluated="):
            profile["row_groups_evaluated"] = int(line.split("=")[1])
        elif line.startswith("[SCAN] total_segments_scanned="):
            profile["segments_scanned"] = int(line.split("=")[1])
        elif line.startswith("[SCAN] total_segments_available="):
            profile["segments_total"] = int(line.split("=")[1])
        elif line.startswith("[SCAN] total_segments_skipped="):
            profile["segments_skipped"] = int(line.split("=")[1])
        elif line.startswith("[SCAN] total_row_groups_scanned="):
            profile["row_groups_scanned"] = int(line.split("=")[1])
        elif line.startswith("[SCAN] total_row_groups_skipped="):
            profile["row_groups_skipped"] = int(line.split("=")[1])
        elif line.startswith("[SCAN] bytes_read_pct="):
            profile["bytes_read_pct"] = float(line.split("=")[1])
        elif line.startswith("[SCAN] total_rows_after_filter="):
            profile["rows_after_filter"] = int(line.split("=")[1])
        elif line.startswith("[SCAN] rayon_parallel="):
            profile["rayon_parallel"] = line.split("=")[1].lower() == "true"
        elif line.startswith("[PROFILE] sampling.sst_rate="):
            profile["sst_rate"] = float(line.split("=")[1])
        elif line.startswith("[PROFILE] sampling.row_rate="):
            profile["row_rate"] = float(line.split("=")[1])
        elif line.startswith("[PROFILE] sampling.effective_rate="):
            profile["effective_rate"] = float(line.split("=")[1])
        elif line.startswith("[PROFILE] rows.after_sample="):
            profile["rows_scanned"] = int(line.split("=")[1])
        elif line.startswith("Storage Path:"):
            profile["storage_path"] = line.split(":")[1].strip()
        elif line.startswith("Effective Sampling Rate:"):
            try:
                profile["confidence"] = float(line.split(":")[1].strip())
            except:
                pass
        elif line.startswith("SCALAR:"):
            profile["result_value"] = line.split(":")[1].split("|")[0].strip()
        elif line.startswith("GROUP:"):
            profile["result_value"] += line.split(":")[1].split("|")[0].strip() + ","
        elif line.startswith("Warnings:"):
            profile["warnings"] = line[9:].strip()

    return profile


def run_aqe_query(query: str, accuracy: float, n_runs: int = 3) -> tuple[float, dict]:
    best_time = float("inf")
    best_profile = {}

    for _ in range(n_runs):
        cmd = [
            AQE_BIN, "query", query,
            "--accuracy", str(accuracy)
        ]
        t0 = time.perf_counter()
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env={**os.environ, "RUSTFLAGS": "-Awarnings"}
        )
        elapsed = (time.perf_counter() - t0) * 1000
        output = result.stdout + result.stderr

        if result.returncode != 0:
            print(f"  [ERROR] AQE failed: {result.stderr[:200]}")
            continue

        profile = parse_aqe_profile(output)
        reported_ms = profile.get("total_ms", elapsed)
        if reported_ms < best_time:
            best_time = reported_ms
            best_profile = profile

    return best_time, best_profile


def compute_error(sqlite_result: str, aqe_result: str) -> float:
    try:
        sv = float(sqlite_result.replace(",", ""))
        av = float(aqe_result.replace(",", ""))
        if sv == 0:
            return 0.0 if av == 0 else 100.0
        return abs(sv - av) / abs(sv) * 100.0
    except:
        return -1.0  # non-numeric (GROUP BY results)


# ── Main benchmark ────────────────────────────────────────────────────────────

def main():
    os.makedirs("bench", exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)

    # ── Build AQE ────────────────────────────────────────────────────────────
    print("Building AQE (release)...")
    r = subprocess.run(
        ["cargo", "build", "--release"],
        capture_output=True, text=True
    )
    if r.returncode != 0:
        print(f"Build failed:\n{r.stderr}")
        sys.exit(1)
    print("Build OK.\n")

    # ── Generate data ─────────────────────────────────────────────────────────
    csv_path = f"{DATA_DIR}/bench_10m.csv"
    if not os.path.exists(csv_path):
        generate_data_csv(csv_path, NUM_ROWS)
    else:
        print(f"Data CSV exists: {csv_path}")

    # ── Load into AQE ─────────────────────────────────────────────────────────
    print("\nChecking AQE segments...")
    seg_dir = Path(DATA_DIR) / "segments"
    segments = list(seg_dir.glob("seg_*")) if seg_dir.exists() else []
    if len(segments) < 4:
        print(f"Only {len(segments)} segments found. Loading + compacting...")
        r = subprocess.run(
            [AQE_BIN, "load", csv_path],
            capture_output=True, text=True
        )
        if r.returncode != 0:
            print(f"Load failed:\n{r.stderr}")
            sys.exit(1)
        r = subprocess.run(
            [AQE_BIN, "compact"],
            capture_output=True, text=True
        )
        print(f"Compact: {r.stdout[-200:]}")
        segments = list(seg_dir.glob("seg_*"))
    print(f"AQE has {len(segments)} segments.\n")

    # ── Setup SQLite ──────────────────────────────────────────────────────────
    print("Setting up SQLite...")
    conn = setup_sqlite(csv_path)
    print()

    # ── Run benchmarks ────────────────────────────────────────────────────────
    all_results: list[QueryResult] = []
    sqlite_times: dict[str, float] = {}
    sqlite_results: dict[str, str] = {}

    total_queries = len(QUERIES) * (1 + len(ACCURACIES))
    done = 0

    print(f"Running {len(QUERIES)} queries × "
          f"(1 SQLite + {len(ACCURACIES)} AQE) = {total_queries} total runs\n")
    print("─" * 90)

    for label, sql, aqe_q, category in QUERIES:
        print(f"\n[{category}] {label}")

        # ── SQLite ────────────────────────────────────────────────────────────
        print(f"  SQLite... ", end="", flush=True)
        sq_time, sq_result = run_sqlite_query(conn, sql)
        sqlite_times[label]   = sq_time
        sqlite_results[label] = sq_result
        print(f"{sq_time:.1f}ms → {sq_result[:60]}")

        r = QueryResult(
            label=label, category=category, source="sqlite",
            accuracy=1.0, time_ms=sq_time, result_value=sq_result,
            rows_scanned=NUM_ROWS, confidence=1.0, error_pct=0.0,
            speedup=1.0, bytes_read=0, bytes_read_pct=1.0,
            segments_scanned=0, segments_total=0, col_files_opened=0,
            row_groups_scanned=0, row_groups_skipped=0,
            filter_ms=0.0, aggregation_ms=0.0, io_ms=sq_time,
            rayon_parallel=False, storage_path="row",
            warnings="",
        )
        all_results.append(r)

        # ── AQE at each accuracy ──────────────────────────────────────────────
        for acc in ACCURACIES:
            print(f"  AQE acc={acc}... ", end="", flush=True)
            aqe_time, prof = run_aqe_query(aqe_q, acc)
            err = compute_error(sq_result, prof.get("result_value", ""))
            speedup = sq_time / aqe_time if aqe_time > 0 else 0.0

            print(
                f"{aqe_time:.1f}ms "
                f"({speedup:.1f}x) "
                f"err={err:.2f}% "
                f"segs={prof.get('segments_scanned',0)}/"
                f"{prof.get('segments_total',0)} "
                f"bytes={prof.get('bytes_read',0)//1024}KB "
                f"rayon={prof.get('rayon_parallel',False)}"
            )

            r = QueryResult(
                label=label, category=category,
                source=f"aqe_{acc}", accuracy=acc,
                time_ms=aqe_time,
                result_value=prof.get("result_value", ""),
                rows_scanned=prof.get("rows_scanned", 0),
                confidence=prof.get("confidence", 0.0),
                error_pct=err,
                speedup=speedup,
                bytes_read=prof.get("bytes_read", 0),
                bytes_read_pct=prof.get("bytes_read_pct", 0.0),
                segments_scanned=prof.get("segments_scanned", 0),
                segments_total=prof.get("segments_total", 0),
                col_files_opened=prof.get("col_files_opened", 0),
                row_groups_scanned=prof.get("row_groups_scanned", 0),
                row_groups_skipped=prof.get("row_groups_skipped", 0),
                filter_ms=prof.get("filter_ms", 0.0),
                aggregation_ms=prof.get("aggregation_ms", 0.0),
                io_ms=prof.get("io_ms", 0.0),
                rayon_parallel=prof.get("rayon_parallel", False),
                storage_path=prof.get("storage_path", ""),
                warnings=prof.get("warnings", ""),
            )
            all_results.append(r)
            done += 1

    # ── Write CSV ─────────────────────────────────────────────────────────────
    print(f"\n\nWriting results → {LOG_CSV}")
    with open(LOG_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(all_results[0]).keys()))
        w.writeheader()
        for r in all_results:
            w.writerow(asdict(r))

    # ── Write JSON ────────────────────────────────────────────────────────────
    with open(LOG_JSON, "w") as f:
        json.dump([asdict(r) for r in all_results], f, indent=2)

    # ── Summary tables ────────────────────────────────────────────────────────
    print("\n" + "═" * 100)
    print("SUMMARY — AQE vs SQLite (best speedup per query)")
    print("═" * 100)
    print(f"{'Query':<35} {'SQLite':>8} {'AQE 0.7':>8} {'AQE 0.9':>8} "
          f"{'AQE 0.99':>9} {'Best err%':>9} {'Best speedup':>12}")
    print("─" * 100)

    for label, _, _, category in QUERIES:
        sq_t = sqlite_times.get(label, 0)
        row = f"{label:<35} {sq_t:>7.1f}ms"
        best_err = 999.0
        best_spd = 0.0
        for acc in ACCURACIES:
            matches = [r for r in all_results
                      if r.label == label and r.source == f"aqe_{acc}"]
            if matches:
                m = matches[0]
                row += f" {m.time_ms:>7.1f}ms"
                if m.error_pct >= 0:
                    best_err = min(best_err, m.error_pct)
                best_spd = max(best_spd, m.speedup)
            else:
                row += f" {'N/A':>8}"
        err_str = f"{best_err:.2f}%" if best_err < 999 else "N/A"
        row += f" {err_str:>9} {best_spd:>10.1f}x"
        print(row)

    print("─" * 100)

    # ── Category summary ──────────────────────────────────────────────────────
    print("\nSUMMARY BY CATEGORY")
    print(f"{'Category':<25} {'Avg speedup':>12} {'Avg err%':>10} {'Queries':>8}")
    print("─" * 58)
    categories = list(dict.fromkeys(c for _, _, _, c in QUERIES))
    for cat in categories:
        cat_results = [r for r in all_results
                      if r.category == cat and r.source != "sqlite"]
        if not cat_results:
            continue
        avg_spd = sum(r.speedup for r in cat_results) / len(cat_results)
        valid_err = [r.error_pct for r in cat_results if r.error_pct >= 0]
        avg_err = sum(valid_err) / len(valid_err) if valid_err else -1
        n_queries = len([q for q in QUERIES if q[3] == cat])
        print(f"{cat:<25} {avg_spd:>11.1f}x {avg_err:>9.2f}% {n_queries:>8}")

    # ── IO efficiency ─────────────────────────────────────────────────────────
    print("\nIO EFFICIENCY (bytes_read_pct at acc=0.70)")
    print(f"{'Query':<35} {'bytes_pct':>10} {'segs':>6} {'col_files':>10}")
    print("─" * 65)
    for label, _, _, _ in QUERIES:
        matches = [r for r in all_results
                  if r.label == label and r.source == "aqe_0.7"]
        if matches:
            m = matches[0]
            print(f"{label:<35} {m.bytes_read_pct*100:>9.2f}% "
                  f"{m.segments_scanned:>3}/{m.segments_total:<3} "
                  f"{m.col_files_opened:>10}")

    print(f"\nResults saved to {LOG_CSV} and {LOG_JSON}")
    print("Done.")


if __name__ == "__main__":
    main()