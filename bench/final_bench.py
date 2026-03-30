#!/usr/bin/env python3
"""
Canonical SQLite-vs-AQE benchmark harness.

Validates correctness across scalar, boolean, grouped, and filter+group queries,
and captures AQE profile/timing metrics in JSON + CSV outputs.
"""

from __future__ import annotations

import argparse
import ast
import csv
import json
import math
import os
import shutil
import sqlite3
import statistics
import subprocess
import sys
import time
import tomllib
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
AQE_BIN = ROOT / "target" / "release" / "lsm"
DATA_DIR = ROOT / "data"
BENCH_DIR = ROOT / "bench"
DEFAULT_ROWS = 1_000_000
DEFAULT_CSV = DATA_DIR / "final_bench_10m.csv"
DEFAULT_SQLITE = DATA_DIR / "final_bench_10m.db"
DEFAULT_RESULTS_JSON = BENCH_DIR / "final_bench_results.json"
DEFAULT_RESULTS_CSV = BENCH_DIR / "final_bench_results.csv"
CONFIG_PATH = ROOT / "config.toml"
SCHEMA_PATH = ROOT / "schema.toml"


QUERY_MATRIX = [
    ("COUNT_GLOBAL", "metadata_fastpath", "SELECT COUNT(*) FROM logs"),
    ("SUM_GLOBAL", "metadata_fastpath", "SELECT SUM(level) FROM logs"),
    ("AVG_GLOBAL", "metadata_fastpath", "SELECT AVG(level) FROM logs"),
    ("COUNT_STATUS_EQ_0", "equality_filter", "SELECT COUNT(*) FROM logs WHERE status = 0"),
    ("SUM_COUNTRY_EQ_1", "equality_filter", "SELECT SUM(level) FROM logs WHERE country = 1"),
    ("COUNT_LEVEL_GT_500", "range_filter", "SELECT COUNT(*) FROM logs WHERE level > 500"),
    ("COUNT_LEVEL_LT_200", "range_filter", "SELECT COUNT(*) FROM logs WHERE level < 200"),
    ("COUNT_TS_GE_2023", "range_filter", "SELECT COUNT(*) FROM logs WHERE timestamp >= 1672531200000"),
    ("COUNT_BOOL_AND", "boolean_filter", "SELECT COUNT(*) FROM logs WHERE status = 0 AND country = 1"),
    ("COUNT_BOOL_OR", "boolean_filter", "SELECT COUNT(*) FROM logs WHERE level > 500 OR status = 2"),
    ("COUNT_BOOL_NOT", "boolean_filter", "SELECT COUNT(*) FROM logs WHERE NOT status = 1 AND country = 2"),
    ("COUNT_BOOL_PRECEDENCE", "boolean_filter", "SELECT COUNT(*) FROM logs WHERE status = 0 AND country = 1 AND level > 500"),
    ("COUNT_GROUP_COUNTRY", "group_by", "SELECT COUNT(*) FROM logs GROUP BY country"),
    ("COUNT_GROUP_STATUS", "group_by", "SELECT COUNT(*) FROM logs GROUP BY status"),
    ("SUM_GROUP_COUNTRY", "group_by", "SELECT SUM(level) FROM logs GROUP BY country"),
    ("AVG_GROUP_STATUS", "group_by", "SELECT AVG(level) FROM logs GROUP BY status"),
    ("COUNT_STATUS0_GROUP_COUNTRY", "filter_group_by", "SELECT COUNT(*) FROM logs WHERE status = 0 GROUP BY country"),
    ("SUM_LEVEL_GT500_GROUP_STATUS", "filter_group_by", "SELECT SUM(level) FROM logs WHERE level > 500 GROUP BY status"),
    ("COUNT_BOOL_GROUP_COUNTRY", "boolean_group_by", "SELECT COUNT(*) FROM logs WHERE NOT status = 1 AND country = 2 GROUP BY country"),
]


@dataclass
class BenchmarkRow:
    label: str
    category: str
    sql: str
    source: str
    accuracy: float
    dataset_rows: int
    status: str
    execution_ms: float
    result_repr: str
    reference_repr: str
    matched_reference: bool
    error_delta: float | None
    error_pct: float | None
    speedup_vs_sqlite: float | None
    rows_scanned: int
    storage_path: str
    warnings: str
    aqe_profile_total_ms: float | None
    aqe_profile_filter_ms: float | None
    aqe_profile_aggregation_ms: float | None
    aqe_profile_io_ms: float | None
    aqe_profile_col_open_ms: float | None
    aqe_profile_decode_ms: float | None
    aqe_profile_filter_agg_ms: float | None
    aqe_bitmap_load_ms: float | None
    aqe_bitmap_eval_ms: float | None
    aqe_bitmap_combine_ms: float | None
    aqe_bitmap_iter_ms: float | None
    aqe_bitmap_match_rows: int | None
    aqe_bitmap_match_ratio: float | None
    aqe_bitmap_values_touched: int | None
    aqe_bitmap_leaf_count: int | None
    aqe_group_index_ms: float | None
    aqe_group_emit_ms: float | None
    aqe_rle_scan_ms: float | None
    aqe_agg_sum_ms: float | None
    aqe_agg_count_ms: float | None
    aqe_segments: int | None
    aqe_columns_opened: int | None
    aqe_rows_total: int | None
    aqe_rows_filtered: int | None
    aqe_rows_sampled: int | None
    aqe_strategy: str | None
    aqe_planner_reason: str | None
    aqe_planner_est_match_ratio: float | None
    aqe_planner_est_values_touched: int | None
    aqe_planner_est_path_cost: float | None
    raw_error: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Final SQLite-vs-AQE benchmark harness")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--csv-path", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--sqlite-path", type=Path, default=DEFAULT_SQLITE)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_RESULTS_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_RESULTS_CSV)
    parser.add_argument("--reuse-data", action="store_true")
    parser.add_argument("--reuse-sqlite", action="store_true")
    parser.add_argument("--reuse-aqe", action="store_true")
    parser.add_argument("--accuracies", type=float, nargs="*", default=[0.70, 0.90, 0.95, 0.99])
    parser.add_argument("--runs", type=int, default=1)
    return parser.parse_args()


def run_checked(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, text=True, capture_output=True, cwd=ROOT, **kwargs)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
    return proc


def csv_row_count(path: Path) -> int:
    with path.open("r", newline="") as f:
        return max(sum(1 for _ in f) - 1, 0)


def sqlite_row_count(path: Path) -> int | None:
    if not path.exists():
        return None
    conn = sqlite3.connect(path)
    try:
        return conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
    except sqlite3.DatabaseError:
        return None
    finally:
        conn.close()


def aqe_segment_row_count(seg_dir: Path) -> int | None:
    if not seg_dir.exists():
        return None
    total = 0
    found = False
    for meta in seg_dir.glob("*/meta.toml"):
        found = True
        with meta.open("rb") as f:
            data = tomllib.load(f)
        total += int(data.get("row_count", 0))
    return total if found else None


def ensure_csv(rows: int, csv_path: Path, reuse_hint: bool) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        existing_rows = csv_row_count(csv_path)
        if existing_rows == rows:
            print(f"CSV reuse: {csv_path} ({existing_rows:,} rows)")
            return
        if reuse_hint:
            print(f"CSV row mismatch ({existing_rows:,} != {rows:,}); regenerating.")
    print(f"Generating CSV: {csv_path} ({rows:,} rows)")
    run_checked(
        [
            sys.executable,
            str(ROOT / "tools" / "gen_data.py"),
            "--num",
            str(rows),
            "--output",
            str(csv_path),
            "--schema",
            str(SCHEMA_PATH),
            "--seed",
            "42",
        ]
    )


def ensure_sqlite(rows: int, csv_path: Path, sqlite_path: Path, reuse_hint: bool) -> None:
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    existing_rows = sqlite_row_count(sqlite_path)
    if existing_rows == rows:
        print(f"SQLite reuse: {sqlite_path} ({existing_rows:,} rows)")
        return
    if sqlite_path.exists():
        if reuse_hint:
            print(f"SQLite row mismatch ({existing_rows} != {rows}); rebuilding.")
        sqlite_path.unlink()

    print(f"Loading SQLite: {sqlite_path}")
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA locking_mode=EXCLUSIVE")
        conn.execute("PRAGMA cache_size=100000")
        conn.execute(
            """
            CREATE TABLE logs (
                user_id INTEGER,
                status INTEGER,
                country INTEGER,
                timestamp INTEGER,
                level INTEGER
            )
            """
        )
        with csv_path.open("r", newline="") as f:
            reader = csv.DictReader(f)
            batch: list[tuple[int, int, int, int, int]] = []
            inserted = 0
            conn.execute("BEGIN")
            for row in reader:
                batch.append(
                    (
                        int(row["user_id"]),
                        int(row["status"]),
                        int(row["country"]),
                        int(row["timestamp"]),
                        int(row["level"]),
                    )
                )
                if len(batch) >= 100_000:
                    conn.executemany("INSERT INTO logs VALUES (?,?,?,?,?)", batch)
                    inserted += len(batch)
                    print(f"  SQLite inserted {inserted:,}/{rows:,} rows")
                    batch.clear()
            if batch:
                conn.executemany("INSERT INTO logs VALUES (?,?,?,?,?)", batch)
                inserted += len(batch)
                print(f"  SQLite inserted {inserted:,}/{rows:,} rows")
            conn.commit()

        # print("  Creating SQLite indexes...")
        # conn.execute("CREATE INDEX idx_status ON logs(status)")
        # conn.execute("CREATE INDEX idx_country ON logs(country)")
        # conn.execute("CREATE INDEX idx_level ON logs(level)")
        # conn.execute("CREATE INDEX idx_timestamp ON logs(timestamp)")
        # conn.commit()
    finally:
        conn.close()


def clean_aqe_storage() -> None:
    for aqe_file in DATA_DIR.glob("*.aqe"):
        aqe_file.unlink()
    seg_dir = DATA_DIR / "segments"
    if seg_dir.exists():
        for child in seg_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()


def ensure_aqe(rows: int, csv_path: Path, reuse_hint: bool) -> None:
    seg_dir = DATA_DIR / "segments"
    existing_rows = aqe_segment_row_count(seg_dir)
    if existing_rows == rows:
        print(f"AQE reuse: {seg_dir} ({existing_rows:,} rows)")
        return
    if reuse_hint and existing_rows is not None:
        print(f"AQE row mismatch ({existing_rows:,} != {rows:,}); rebuilding.")
    clean_aqe_storage()
    print("Loading AQE storage...")
    run_checked([str(AQE_BIN), "load", str(csv_path)])
    print("Compacting AQE storage...")
    run_checked([str(AQE_BIN), "compact"])


@contextmanager
def temporary_accuracy(accuracy: float):
    original = CONFIG_PATH.read_text()
    data = tomllib.loads(original)
    lines = original.splitlines()
    replaced = False
    for idx, line in enumerate(lines):
        if line.strip().startswith("accuracy_target"):
            lines[idx] = f"accuracy_target = {accuracy}"
            replaced = True
            break
    if not replaced:
        lines.insert(0, f"accuracy_target = {accuracy}")
    CONFIG_PATH.write_text("\n".join(lines) + "\n")
    try:
        yield
    finally:
        CONFIG_PATH.write_text(original)


def sqlite_sql_for_query(sql: str) -> str:
    upper = sql.upper()
    if "GROUP BY" in upper and not sql.strip().upper().startswith("SELECT COUNT(*)") and "SUM(" not in upper and "AVG(" not in upper:
        return sql
    if "GROUP BY" not in upper:
        return sql
    group_col = sql.split("GROUP BY", 1)[1].strip()
    if "COUNT(*)" in upper:
        return sql.replace("SELECT COUNT(*)", f"SELECT {group_col}, COUNT(*)")
    if "SUM(" in upper:
        target = sql[sql.upper().find("SUM("): sql.find(")", sql.upper().find("SUM(")) + 1]
        return sql.replace(f"SELECT {target}", f"SELECT {group_col}, {target}")
    if "AVG(" in upper:
        target = sql[sql.upper().find("AVG("): sql.find(")", sql.upper().find("AVG(")) + 1]
        return sql.replace(f"SELECT {target}", f"SELECT {group_col}, {target}")
    return sql


def normalize_sqlite_result(rows: list[tuple[Any, ...]]) -> tuple[str, Any]:
    if len(rows) == 1 and len(rows[0]) == 1:
        value = rows[0][0]
        return str(value), value
    mapping = {str(row[0]): float(row[1]) for row in rows}
    return json.dumps(dict(sorted(mapping.items())), sort_keys=True), mapping


def parse_profile_line(output: str) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "total_ms": 0.0,
        "stage.col_open_ms": 0.0,
        "stage.decode_ms": 0.0,
        "stage.filter_agg_ms": 0.0,
        "io.seg": 0,
        "io.col": 0,
        "rows.tot": 0,
        "rows.filt": 0,
        "rows.samp": 0,
        "rate.sst": 0.0,
        "rate.row": 0.0,
        "mfp": False,
        "parallel": False,
        "strategy": "",
        "bitmap.load_ms": 0.0,
        "bitmap.eval_ms": 0.0,
        "bitmap.combine_ms": 0.0,
        "bitmap.iter_ms": 0.0,
        "bitmap.match_rows": 0,
        "bitmap.match_ratio": 0.0,
        "bitmap.values_touched": 0,
        "bitmap.leaf_count": 0,
        "group.index_ms": 0.0,
        "group.emit_ms": 0.0,
        "rle.scan_ms": 0.0,
        "agg.sum_ms": 0.0,
        "agg.count_ms": 0.0,
        "planner.reason": "",
        "planner.est_match_ratio": 0.0,
        "planner.est_values_touched": 0,
        "planner.est_path_cost": 0.0,
    }
    for line in output.splitlines():
        line = line.strip()
        if not line.startswith("[PROFILE] "):
            continue
        payload = line[len("[PROFILE] "):]
        for token in payload.split():
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            if key in {"query"}:
                profile[key] = value
            elif value.lower() in {"true", "false"}:
                profile[key] = value.lower() == "true"
            else:
                try:
                    profile[key] = int(value)
                except ValueError:
                    try:
                        profile[key] = float(value)
                    except ValueError:
                        profile[key] = value
    return profile


def parse_aqe_output(output: str) -> tuple[str, Any, dict[str, Any], int, str, str]:
    profile = parse_profile_line(output)
    rows_scanned = 0
    storage_path = "unknown"
    warnings = ""
    scalar: float | None = None
    groups: dict[str, float] = {}

    for line in output.splitlines():
        line = line.strip()
        if line.startswith("Rows Scanned:"):
            rows_scanned = int(line.split(":", 1)[1].strip())
        elif line.startswith("Storage Path:"):
            storage_path = line.split(":", 1)[1].strip()
        elif line.startswith("Warnings:"):
            warnings = line.split(":", 1)[1].strip()
        elif line.startswith("SCALAR:"):
            scalar = float(line.split(":", 1)[1].split("|", 1)[0].strip())
        elif line.startswith("GROUP:"):
            parts = line.split(":", 1)[1].split("|")
            if len(parts) >= 2:
                groups[parts[0].strip()] = float(parts[1].strip())

    if scalar is not None:
        return str(scalar), scalar, profile, rows_scanned, storage_path, warnings
    if groups:
        normalized = dict(sorted(groups.items()))
        return json.dumps(normalized, sort_keys=True), normalized, profile, rows_scanned, storage_path, warnings
    if "EMPTY" in output:
        return "EMPTY", None, profile, rows_scanned, storage_path, warnings
    raise ValueError("Unable to parse AQE result from output")


def scalar_delta(reference: float, candidate: float) -> tuple[float, float]:
    delta = abs(candidate - reference)
    if reference == 0:
        return delta, 0.0 if candidate == 0 else 100.0
    return delta, delta / abs(reference) * 100.0


def compare_results(reference: Any, candidate: Any) -> tuple[bool, float | None, float | None]:
    if isinstance(reference, dict) and isinstance(candidate, dict):
        if reference.keys() != candidate.keys():
            return False, None, None
        worst_delta = 0.0
        worst_pct = 0.0
        for key in reference:
            if not math.isclose(reference[key], candidate[key], rel_tol=1e-9, abs_tol=1e-6):
                delta, pct = scalar_delta(reference[key], candidate[key])
                worst_delta = max(worst_delta, delta)
                worst_pct = max(worst_pct, pct)
        return worst_delta == 0.0, worst_delta, worst_pct
    if reference is None or candidate is None:
        return reference == candidate, None, None
    if math.isclose(float(reference), float(candidate), rel_tol=1e-9, abs_tol=1e-6):
        return True, 0.0, 0.0
    delta, pct = scalar_delta(float(reference), float(candidate))
    return False, delta, pct


def run_sqlite_query(conn: sqlite3.Connection, sql: str, runs: int) -> tuple[float, str, Any]:
    actual_sql = sqlite_sql_for_query(sql)
    timings = []
    result_repr = ""
    raw: Any = None
    for _ in range(runs):
        start = time.perf_counter()
        rows = conn.execute(actual_sql).fetchall()
        timings.append((time.perf_counter() - start) * 1000.0)
        if raw is None:
            result_repr, raw = normalize_sqlite_result(rows)
    return min(timings), result_repr, raw


def run_aqe_query(sql: str, accuracy: float, runs: int) -> tuple[float, str, Any, dict[str, Any], int, str, str]:
    best_time = float("inf")
    best_payload: tuple[str, Any, dict[str, Any], int, str, str] | None = None
    with temporary_accuracy(accuracy):
        for _ in range(runs):
            start = time.perf_counter()
            proc = subprocess.run(
                [str(AQE_BIN), "query", sql, "--accuracy", str(accuracy)],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            elapsed = (time.perf_counter() - start) * 1000.0
            output = proc.stdout + proc.stderr
            if proc.returncode != 0:
                raise RuntimeError(output.strip())
            parsed = parse_aqe_output(output)
            measured = parsed[2].get("total_ms", elapsed) or elapsed
            if measured < best_time:
                best_time = measured
                best_payload = parsed
    assert best_payload is not None
    return (best_time, *best_payload)


def build_summary(rows: list[BenchmarkRow]) -> dict[str, Any]:
    aqe_rows = [row for row in rows if row.source.startswith("aqe_")]
    categories = sorted(set(row.category for row in rows))
    category_summary: dict[str, Any] = {}
    for category in categories:
        cat_rows = [row for row in aqe_rows if row.category == category]
        if not cat_rows:
            continue
        sqlite_times = [
            row.execution_ms
            for row in rows
            if row.category == category and row.source == "sqlite"
        ]
        aqe_times = [row.execution_ms for row in cat_rows if row.status == "ok"]
        failed = [row for row in cat_rows if not row.matched_reference or row.status != "ok"]
        med_sqlite = statistics.median(sqlite_times) if sqlite_times else None
        med_aqe = statistics.median(aqe_times) if aqe_times else None
        med_speedup = (med_sqlite / med_aqe) if med_sqlite and med_aqe and med_aqe > 0 else None

        # Per-query breakdown: group by label, pick median AQE time and strategy.
        query_labels = dict.fromkeys(row.label for row in cat_rows)  # preserves order
        query_details: list[dict[str, Any]] = []
        for label in query_labels:
            label_sqlite = next(
                (r.execution_ms for r in rows if r.label == label and r.source == "sqlite"), None
            )
            label_aqe_rows = [r for r in cat_rows if r.label == label and r.status == "ok"]
            label_aqe_ms = statistics.median([r.execution_ms for r in label_aqe_rows]) if label_aqe_rows else None
            strategy = label_aqe_rows[0].aqe_strategy if label_aqe_rows else None
            reason = label_aqe_rows[0].aqe_planner_reason if label_aqe_rows else None
            est_match_ratio = (
                statistics.median([r.aqe_planner_est_match_ratio for r in label_aqe_rows if r.aqe_planner_est_match_ratio is not None])
                if any(r.aqe_planner_est_match_ratio is not None for r in label_aqe_rows)
                else None
            )
            match_ratio = (
                statistics.median([r.aqe_bitmap_match_ratio for r in label_aqe_rows if r.aqe_bitmap_match_ratio is not None])
                if any(r.aqe_bitmap_match_ratio is not None for r in label_aqe_rows)
                else None
            )
            values_touched = (
                statistics.median([r.aqe_bitmap_values_touched for r in label_aqe_rows if r.aqe_bitmap_values_touched is not None])
                if any(r.aqe_bitmap_values_touched is not None for r in label_aqe_rows)
                else None
            )
            label_speedup = (label_sqlite / label_aqe_ms) if label_sqlite and label_aqe_ms and label_aqe_ms > 0 else None
            query_details.append({
                "label": label,
                "sqlite_ms": label_sqlite,
                "aqe_ms": label_aqe_ms,
                "speedup": label_speedup,
                "strategy": strategy,
                "reason": reason,
                "est_match_ratio": est_match_ratio,
                "match_ratio": match_ratio,
                "values_touched": values_touched,
                "passed": all(r.matched_reference for r in label_aqe_rows),
            })

        category_summary[category] = {
            "total_cases": len(cat_rows),
            "passed": len(cat_rows) - len(failed),
            "failed": len(failed),
            "median_sqlite_ms": med_sqlite,
            "median_aqe_ms": med_aqe,
            "median_speedup": med_speedup,
            "worst_failure": (
                max(
                    failed,
                    key=lambda row: (
                        row.error_pct if row.error_pct is not None else -1,
                        1 if row.status != "ok" else 0,
                    ),
                ).label
                if failed
                else None
            ),
            "queries": query_details,
            "median_match_ratio": (
                statistics.median(
                    [
                        r.aqe_bitmap_match_ratio
                        for r in cat_rows
                        if r.status == "ok" and r.aqe_bitmap_match_ratio is not None
                    ]
                )
                if any(r.status == "ok" and r.aqe_bitmap_match_ratio is not None for r in cat_rows)
                else None
            ),
            "median_values_touched": (
                statistics.median(
                    [
                        r.aqe_bitmap_values_touched
                        for r in cat_rows
                        if r.status == "ok" and r.aqe_bitmap_values_touched is not None
                    ]
                )
                if any(r.status == "ok" and r.aqe_bitmap_values_touched is not None for r in cat_rows)
                else None
            ),
        }
    return {
        "total_rows": len(rows),
        "aqe_cases": len(aqe_rows),
        "aqe_passed": sum(1 for row in aqe_rows if row.matched_reference and row.status == "ok"),
        "aqe_failed": sum(1 for row in aqe_rows if not row.matched_reference or row.status != "ok"),
        "by_category": category_summary,
    }


def print_summary(summary: dict[str, Any]) -> None:
    print("\n" + "=" * 100)
    print("FINAL SUMMARY")
    print("=" * 100)
    print(
        f"AQE cases: {summary['aqe_cases']} | "
        f"passed: {summary['aqe_passed']} | "
        f"failed: {summary['aqe_failed']}"
    )
    print("-" * 100)
    print(f"{'Category':<20} {'Cases':>5} {'Pass':>5} {'Fail':>5} {'SQLite med':>12} {'AQE med':>12} {'Speedup':>10} {'Worst':>20}")
    for category, data in summary["by_category"].items():
        sqlite_med = f"{data['median_sqlite_ms']:.2f}ms" if data["median_sqlite_ms"] is not None else "n/a"
        aqe_med = f"{data['median_aqe_ms']:.2f}ms" if data["median_aqe_ms"] is not None else "n/a"
        spd_med = f"{data['median_speedup']:.2f}x" if data["median_speedup"] is not None else "n/a"
        worst = data["worst_failure"] or "-"
        print(
            f"{category:<20} {data['total_cases']:>5} {data['passed']:>5} {data['failed']:>5} "
            f"{sqlite_med:>12} {aqe_med:>12} {spd_med:>10} {worst:>20}"
        )
        if data.get("median_match_ratio") is not None or data.get("median_values_touched") is not None:
            match_ratio = (
                f"{data['median_match_ratio']:.3f}"
                if data.get("median_match_ratio") is not None
                else "n/a"
            )
            values_touched = (
                f"{int(data['median_values_touched'])}"
                if data.get("median_values_touched") is not None
                else "n/a"
            )
            print(f"    diagnostics                           match_ratio={match_ratio:>8}  values_touched={values_touched:>8}")
        for q in data.get("queries", []):
            sq = f"{q['sqlite_ms']:.1f}ms" if q["sqlite_ms"] is not None else "n/a"
            aq = f"{q['aqe_ms']:.1f}ms" if q["aqe_ms"] is not None else "n/a"
            sp = f"{q['speedup']:.1f}x" if q["speedup"] is not None else "n/a"
            st = q["strategy"] or "?"
            rs = q["reason"] or "?"
            emr = f"{q['est_match_ratio']:.3f}" if q["est_match_ratio"] is not None else "n/a"
            mr = f"{q['match_ratio']:.3f}" if q["match_ratio"] is not None else "n/a"
            vt = f"{int(q['values_touched'])}" if q["values_touched"] is not None else "n/a"
            ok = "" if q["passed"] else " FAIL"
            print(
                f"    {q['label']:<36} sqlite={sq:>9}  aqe={aq:>9}  {sp:>7}  "
                f"[{st}] reason={rs} est_match={emr} match_ratio={mr} values_touched={vt}{ok}"
            )


def main() -> None:
    args = parse_args()
    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("Building AQE release binary...")
    run_checked(["cargo", "build", "--release"])

    ensure_csv(args.rows, args.csv_path, args.reuse_data)
    ensure_sqlite(args.rows, args.csv_path, args.sqlite_path, args.reuse_sqlite)
    ensure_aqe(args.rows, args.csv_path, args.reuse_aqe)

    conn = sqlite3.connect(args.sqlite_path)
    rows: list[BenchmarkRow] = []

    try:
        for label, category, sql in QUERY_MATRIX:
            print(f"\n[{category}] {label}")
            sqlite_ms, sqlite_repr, sqlite_raw = run_sqlite_query(conn, sql, args.runs)
            print(f"  SQLite: {sqlite_ms:.2f}ms -> {sqlite_repr[:80]}")
            rows.append(
                BenchmarkRow(
                    label=label,
                    category=category,
                    sql=sql,
                    source="sqlite",
                    accuracy=1.0,
                    dataset_rows=args.rows,
                    status="ok",
                    execution_ms=sqlite_ms,
                    result_repr=sqlite_repr,
                    reference_repr=sqlite_repr,
                    matched_reference=True,
                    error_delta=0.0,
                    error_pct=0.0,
                    speedup_vs_sqlite=1.0,
                    rows_scanned=args.rows,
                    storage_path="row",
                    warnings="",
                    aqe_profile_total_ms=None,
                    aqe_profile_filter_ms=None,
                    aqe_profile_aggregation_ms=None,
                    aqe_profile_io_ms=None,
                    aqe_profile_col_open_ms=None,
                    aqe_profile_decode_ms=None,
                    aqe_profile_filter_agg_ms=None,
                    aqe_bitmap_load_ms=None,
                    aqe_bitmap_eval_ms=None,
                    aqe_bitmap_combine_ms=None,
                    aqe_bitmap_iter_ms=None,
                    aqe_bitmap_match_rows=None,
                    aqe_bitmap_match_ratio=None,
                    aqe_bitmap_values_touched=None,
                    aqe_bitmap_leaf_count=None,
                    aqe_group_index_ms=None,
                    aqe_group_emit_ms=None,
                    aqe_rle_scan_ms=None,
                    aqe_agg_sum_ms=None,
                    aqe_agg_count_ms=None,
                    aqe_segments=None,
                    aqe_columns_opened=None,
                    aqe_rows_total=None,
                    aqe_rows_filtered=None,
                    aqe_rows_sampled=None,
                    aqe_strategy=None,
                    aqe_planner_reason=None,
                    aqe_planner_est_match_ratio=None,
                    aqe_planner_est_values_touched=None,
                    aqe_planner_est_path_cost=None,
                    raw_error="",
                )
            )

            for accuracy in args.accuracies:
                source = f"aqe_{accuracy}"
                try:
                    aqe_ms, aqe_repr, aqe_raw, profile, rows_scanned, storage_path, warnings = run_aqe_query(
                        sql, accuracy, args.runs
                    )
                    matched, delta, pct = compare_results(sqlite_raw, aqe_raw)
                    speedup = sqlite_ms / aqe_ms if aqe_ms > 0 else None
                    status = "ok"
                    raw_error = ""
                    print(
                        f"  {source}: {aqe_ms:.2f}ms -> {aqe_repr[:80]} | "
                        f"match={matched} strategy={profile.get('strategy', '')} "
                        f"reason={profile.get('planner.reason', '')} "
                        f"est_match={profile.get('planner.est_match_ratio', 0.0):.3f} "
                        f"match_ratio={profile.get('bitmap.match_ratio', 0.0):.3f} "
                        f"values_touched={profile.get('bitmap.values_touched', 0)}"
                    )
                except Exception as exc:
                    aqe_ms = 0.0
                    aqe_repr = ""
                    profile = {}
                    rows_scanned = 0
                    storage_path = "unknown"
                    warnings = ""
                    matched = False
                    delta = None
                    pct = None
                    speedup = None
                    status = "error"
                    raw_error = str(exc)
                    print(f"  {source}: ERROR -> {raw_error[:120]}")

                rows.append(
                    BenchmarkRow(
                        label=label,
                        category=category,
                        sql=sql,
                        source=source,
                        accuracy=accuracy,
                        dataset_rows=args.rows,
                        status=status,
                        execution_ms=aqe_ms,
                        result_repr=aqe_repr,
                        reference_repr=sqlite_repr,
                        matched_reference=matched,
                        error_delta=delta,
                        error_pct=pct,
                        speedup_vs_sqlite=speedup,
                        rows_scanned=rows_scanned,
                        storage_path=storage_path,
                        warnings=warnings,
                        aqe_profile_total_ms=profile.get("total_ms"),
                        aqe_profile_filter_ms=profile.get("filter_ms"),
                        aqe_profile_aggregation_ms=profile.get("aggregation_ms"),
                        aqe_profile_io_ms=profile.get("io_ms"),
                        aqe_profile_col_open_ms=profile.get("stage.col_open_ms"),
                        aqe_profile_decode_ms=profile.get("stage.decode_ms"),
                        aqe_profile_filter_agg_ms=profile.get("stage.filter_agg_ms"),
                        aqe_bitmap_load_ms=profile.get("bitmap.load_ms"),
                        aqe_bitmap_eval_ms=profile.get("bitmap.eval_ms"),
                        aqe_bitmap_combine_ms=profile.get("bitmap.combine_ms"),
                        aqe_bitmap_iter_ms=profile.get("bitmap.iter_ms"),
                        aqe_bitmap_match_rows=profile.get("bitmap.match_rows"),
                        aqe_bitmap_match_ratio=profile.get("bitmap.match_ratio"),
                        aqe_bitmap_values_touched=profile.get("bitmap.values_touched"),
                        aqe_bitmap_leaf_count=profile.get("bitmap.leaf_count"),
                        aqe_group_index_ms=profile.get("group.index_ms"),
                        aqe_group_emit_ms=profile.get("group.emit_ms"),
                        aqe_rle_scan_ms=profile.get("rle.scan_ms"),
                        aqe_agg_sum_ms=profile.get("agg.sum_ms"),
                        aqe_agg_count_ms=profile.get("agg.count_ms"),
                        aqe_segments=profile.get("io.seg"),
                        aqe_columns_opened=profile.get("io.col"),
                        aqe_rows_total=profile.get("rows.tot"),
                        aqe_rows_filtered=profile.get("rows.filt"),
                        aqe_rows_sampled=profile.get("rows.samp"),
                        aqe_strategy=profile.get("strategy"),
                        aqe_planner_reason=profile.get("planner.reason"),
                        aqe_planner_est_match_ratio=profile.get("planner.est_match_ratio"),
                        aqe_planner_est_values_touched=profile.get("planner.est_values_touched"),
                        aqe_planner_est_path_cost=profile.get("planner.est_path_cost"),
                        raw_error=raw_error,
                    )
                )
    finally:
        conn.close()

    summary = build_summary(rows)
    args.json_out.write_text(json.dumps({"summary": summary, "rows": [asdict(row) for row in rows]}, indent=2))
    with args.csv_out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))

    print_summary(summary)
    print(f"\nWrote JSON: {args.json_out}")
    print(f"Wrote CSV:  {args.csv_out}")


if __name__ == "__main__":
    main()
