"""AQE engine wrapper — calls the lsm Rust library (installed via maturin)."""
import time
import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]

try:
    import lsm  # type: ignore  — installed by: uv pip install <wheel>
    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False

_conn = None


def _get_conn():
    global _conn
    if _conn is None:
        if not _AVAILABLE:
            raise RuntimeError(
                "lsm module not found. Run `maturin develop` in the project root first."
            )
        data_dir = os.path.join(_ROOT, "data")
        _conn = lsm.connect(data_dir)
    return _conn


def run_query(sql: str) -> dict:
    conn = _get_conn()
    t0 = time.perf_counter()
    try:
        result = conn.query(sql)
    except Exception as e:
        return {"error": str(e), "elapsed_ms": 0.0}
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)

    # result is a dict from driver/mod.rs:
    # {"type": "scalar"|"groups"|"empty", "value"?, "groups"?,
    #  "confidence", "rows_scanned", "sampling_rate", "storage_path", "warnings",
    #  "next_offset"}
    return {**result, "elapsed_ms": elapsed_ms}


def reservoir_sample(n: int) -> list[dict]:
    try:
        return _get_conn().reservoir_sample(n)
    except Exception:
        return []


def is_available() -> bool:
    return _AVAILABLE
