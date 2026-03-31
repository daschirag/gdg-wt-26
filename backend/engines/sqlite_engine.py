"""SQLite engine wrapper — queries data/final_bench_10m.db."""
import sqlite3
import time
import os
import re

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
DB_PATH = os.path.join(_ROOT, "data", "final_bench_10m.db")

_conn: sqlite3.Connection | None = None


def _get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
    return _conn


def translate_sql(sql: str) -> str:
    """Translate AQE-specific APPROX_PERCENTILE to a SQLite equivalent."""
    m = re.search(r"APPROX_PERCENTILE\((\w+),\s*([\d.]+)\)", sql, re.IGNORECASE)
    if m:
        col = m.group(1)
        q = float(m.group(2))
        return (
            f"SELECT {col} FROM logs ORDER BY {col} "
            f"LIMIT 1 OFFSET (SELECT CAST(COUNT(*)*{q} AS INTEGER) FROM logs)"
        )
    return sql


def run_query(sql: str) -> dict:
    translated = translate_sql(sql)
    conn = _get_conn()
    t0 = time.perf_counter()
    try:
        cur = conn.execute(translated)
        rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        return {"error": str(e), "elapsed_ms": 0.0, "rows": []}
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)

    # Normalise: single-value scalar vs GROUP BY rows
    if len(rows) == 1 and len(rows[0]) == 1:
        result = list(rows[0].values())[0]
    else:
        result = rows

    return {"result": result, "elapsed_ms": elapsed_ms, "rows": rows}


def row_count() -> int:
    try:
        return _get_conn().execute("SELECT COUNT(*) FROM logs").fetchone()[0]
    except Exception:
        return 0
