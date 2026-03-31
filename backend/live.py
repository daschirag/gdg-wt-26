"""Live data generator — inserts synthetic rows into the AQE engine in a background thread."""
import random
import threading
import time

from engines import aqe as aqe_engine

START_TS = 1577836800000  # 2020-01-01 UTC ms
END_TS   = 1735603200000  # 2025-01-01 UTC ms

_lock    = threading.Lock()
_thread: threading.Thread | None = None
_running = False
_total   = 0
_rate    = 0.0  # rows/sec EMA


def _generate_row(seq: int) -> list:
    r = random.randint(0, 99)
    status = 0 if r < 70 else (1 if r < 90 else 2)
    return [
        seq,
        status,
        random.randint(0, 4),
        random.randint(START_TS, END_TS),
        random.randint(0, 1000),
    ]


def _worker() -> None:
    global _running, _total, _rate
    conn = aqe_engine._get_conn()
    seq = _total
    last_t = time.perf_counter()
    while _running:
        conn.insert(_generate_row(seq))
        seq += 1
        with _lock:
            _total += 1
        # update EMA rate every 200 rows
        if seq % 200 == 0:
            now = time.perf_counter()
            dt = now - last_t
            if dt > 0:
                inst = 200.0 / dt
                _rate = 0.7 * _rate + 0.3 * inst
            last_t = now
    _running = False


def start() -> None:
    global _thread, _running
    with _lock:
        if _running:
            return
        _running = True
    _thread = threading.Thread(target=_worker, daemon=True)
    _thread.start()


def stop() -> None:
    global _running
    _running = False


def status() -> dict:
    return {
        "running": _running,
        "total_inserted": _total,
        "rows_per_sec": round(_rate, 1),
    }


def sample(n: int = 20) -> list:
    return aqe_engine.reservoir_sample(n)
