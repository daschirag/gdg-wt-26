from __future__ import annotations

import csv
import json
import os
import sqlite3
import subprocess
import sys
import time
import tomllib
import tomli_w
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from engines import aqe as aqe_engine
from engines import sqlite_engine
import live
from queries import PRESET_QUERIES
from bench_queries import BENCH_QUERIES, CATEGORIES

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CONFIG_PATH = DATA_DIR / "config.toml"
BENCH_RESULTS_JSON = ROOT / "bench" / "final_bench_results.json"

app = FastAPI(title="AQE Backend", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class QueryRequest(BaseModel):
    sql: str


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "aqe_available": aqe_engine.is_available(),
        "sqlite_rows": sqlite_engine.row_count(),
    }


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class ConfigUpdate(BaseModel):
    params: dict[str, Any]


@app.get("/api/config")
def get_config():
    with open(CONFIG_PATH, "rb") as f:
        raw = tomllib.load(f)
    raw.pop("schema", None)
    return raw


@app.post("/api/config")
def set_config(update: ConfigUpdate):
    with open(CONFIG_PATH, "rb") as f:
        raw = tomllib.load(f)
    raw.update({k: v for k, v in update.params.items() if k != "schema"})
    with open(CONFIG_PATH, "wb") as f:
        tomli_w.dump(raw, f)
    aqe_engine._conn = None
    return {"status": "ok", "config": {k: v for k, v in raw.items() if k != "schema"}}


# ---------------------------------------------------------------------------
# Preset query lab
# ---------------------------------------------------------------------------

@app.get("/api/presets")
def get_presets():
    return PRESET_QUERIES


@app.post("/api/query")
def run_query(req: QueryRequest):
    if not req.sql.strip():
        raise HTTPException(status_code=400, detail="SQL cannot be empty")
    return {
        "sql": req.sql,
        "aqe": aqe_engine.run_query(req.sql),
        "sqlite": sqlite_engine.run_query(req.sql),
    }


@app.get("/api/sample")
def get_sample(n: int = 20):
    rows = aqe_engine.reservoir_sample(min(n, 200))
    return {"rows": rows, "count": len(rows)}


@app.get("/api/presets/run-all")
def run_all_presets():
    def generate():
        for p in PRESET_QUERIES:
            payload = {
                "id": p["id"],
                "label": p["label"],
                "type": p["type"],
                "sql": p["sql"],
                "aqe": aqe_engine.run_query(p["sql"]),
                "sqlite": sqlite_engine.run_query(p["sql"]),
            }
            yield json.dumps(payload) + "\n"
    return StreamingResponse(generate(), media_type="application/x-ndjson")


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------

@app.get("/api/benchmark/queries")
def benchmark_queries():
    """Return the full QUERY_MATRIX with cached results if available."""
    cached = _load_cached_results()
    queries = []
    for label, category, sql in BENCH_QUERIES:
        entry = {"label": label, "category": category, "sql": sql}
        if cached and label in cached:
            entry["cached"] = cached[label]
        queries.append(entry)
    return {"queries": queries, "categories": CATEGORIES}


@app.get("/api/benchmark/run-all")
def benchmark_run_all():
    """Stream live benchmark results for all 19 queries."""
    def generate():
        for label, category, sql in BENCH_QUERIES:
            aqe_r = aqe_engine.run_query(sql)
            sq_r = sqlite_engine.run_query(sql)
            aqe_ms = aqe_r.get("elapsed_ms", 0)
            sq_ms = sq_r.get("elapsed_ms", 0)
            speedup = round(sq_ms / aqe_ms, 2) if aqe_ms > 0 else None
            payload = {
                "label": label,
                "category": category,
                "sql": sql,
                "aqe_ms": aqe_ms,
                "sqlite_ms": sq_ms,
                "speedup": speedup,
                "aqe_result": aqe_r,
                "sqlite_result": sq_r,
            }
            yield json.dumps(payload) + "\n"
    return StreamingResponse(generate(), media_type="application/x-ndjson")


@app.get("/api/benchmark/cached")
def benchmark_cached():
    """Return the stored final_bench_results.json from the bench/ dir."""
    cached = _load_cached_results()
    if not cached:
        raise HTTPException(status_code=404, detail="No cached benchmark results found. Run the benchmark first.")
    # Flatten into a list ordered by BENCH_QUERIES
    rows = []
    for label, category, sql in BENCH_QUERIES:
        r = cached.get(label)
        if r:
            rows.append({
                "label": label,
                "category": category,
                "sql": sql,
                **r,
            })
    return {"rows": rows, "categories": CATEGORIES}


# ---------------------------------------------------------------------------
# Init / Setup
# ---------------------------------------------------------------------------

@app.post("/api/init/dataset")
def init_dataset(rows: int = 1_000_000):
    """Generate a fresh CSV dataset using tools/gen_data.py."""
    csv_path = DATA_DIR / "final_bench_10m.csv"
    try:
        result = subprocess.run(
            [
                sys.executable,
                str(ROOT / "tools" / "gen_data.py"),
                "--num", str(rows),
                "--output", str(csv_path),
                "--schema", str(ROOT / "schema.toml"),
                "--seed", "42",
            ],
            capture_output=True, text=True, timeout=300,
        )
        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=result.stderr or "gen_data.py failed")
        return {"status": "ok", "rows": rows, "path": str(csv_path)}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Dataset generation timed out")


@app.post("/api/init/sqlite")
def init_sqlite(rows: int = 1_000_000):
    """Build SQLite DB from the CSV. Creates data/final_bench_10m.db."""
    csv_path = DATA_DIR / "final_bench_10m.csv"
    db_path = DATA_DIR / "final_bench_10m.db"

    if not csv_path.exists():
        raise HTTPException(status_code=400, detail="CSV not found. Run /api/init/dataset first.")

    # Drop and recreate
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=100000")
        conn.execute("""
            CREATE TABLE logs (
                user_id INTEGER, status INTEGER, country INTEGER,
                timestamp INTEGER, level INTEGER
            )
        """)
        inserted = 0
        batch: list = []
        conn.execute("BEGIN")
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                batch.append((int(row["user_id"]), int(row["status"]), int(row["country"]),
                               int(row["timestamp"]), int(row["level"])))
                if len(batch) >= 100_000:
                    conn.executemany("INSERT INTO logs VALUES (?,?,?,?,?)", batch)
                    inserted += len(batch)
                    batch.clear()
            if batch:
                conn.executemany("INSERT INTO logs VALUES (?,?,?,?,?)", batch)
                inserted += len(batch)
        conn.commit()
        # reset cached connection so the engine picks up the new DB
        sqlite_engine._conn = None
        return {"status": "ok", "rows": inserted, "path": str(db_path)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/api/init/aqe")
def init_aqe():
    """Load the CSV into the AQE engine (calls lsm load <csv_path>)."""
    csv_path = DATA_DIR / "final_bench_10m.csv"
    bin_path = ROOT / "target" / "release" / "lsm"
    if not bin_path.exists():
        bin_path = ROOT / "target" / "debug" / "lsm"
    if not bin_path.exists():
        raise HTTPException(status_code=400, detail="AQE binary not found. Run `cargo build` first.")
    if not csv_path.exists():
        raise HTTPException(status_code=400, detail="CSV not found. Run /api/init/dataset first.")
    try:
        result = subprocess.run(
            [str(bin_path), "load", str(csv_path)],
            capture_output=True, text=True, timeout=120,
            cwd=str(ROOT),
        )
        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=result.stderr or "AQE load failed")
        # reset AQE connection so it reloads SSTables
        aqe_engine._conn = None
        return {"status": "ok", "output": result.stdout.strip()}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="AQE load timed out")


# ---------------------------------------------------------------------------
# Live ingestion
# ---------------------------------------------------------------------------

@app.post("/api/live/start")
def live_start():
    live.start()
    return live.status()


@app.post("/api/live/stop")
def live_stop():
    live.stop()
    return live.status()


@app.get("/api/live/status")
def live_status():
    return live.status()


@app.get("/api/live/sample")
def live_sample(n: int = 20):
    return {"rows": live.sample(min(n, 200))}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_cached_results() -> dict | None:
    if not BENCH_RESULTS_JSON.exists():
        return None
    try:
        with open(BENCH_RESULTS_JSON) as f:
            data = json.load(f)
        # Flatten: label -> {aqe_ms, sqlite_ms, speedup, passed}
        flat: dict = {}
        for cat_data in data.get("summary", {}).get("by_category", {}).values():
            for q in cat_data.get("queries", []):
                flat[q["label"]] = {
                    "aqe_ms": q.get("aqe_ms"),
                    "sqlite_ms": q.get("sqlite_ms"),
                    "speedup": q.get("speedup"),
                    "passed": q.get("passed"),
                    "strategy": q.get("strategy"),
                }
        return flat
    except Exception:
        return None
