import os
import pandas as pd
import psutil
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field


@dataclass
class MemoryTracker:
    results: list = field(default_factory=list)

    def get_report(self):
        if not self.results:
            return pd.DataFrame(columns=['query', 'peak_mb', 'delta_mb'])
        return pd.DataFrame(self.results).sort_values('peak_mb', ascending=False)

    def clear(self):
        self.results.clear()


_tracker = MemoryTracker()
_process = psutil.Process(os.getpid())


@contextmanager
def track_memory(query_name: str, poll_interval: float = 0.01):
    """
    Track process memory usage during a code block using psutil.

    Measures:
    - peak_mb: Maximum RSS above baseline during execution
    - delta_mb: Net RSS change (after - before)

    Args:
        query_name: Label for this measurement
        poll_interval: Seconds between memory polls (default 10ms)
    """
    peak_rss = 0
    stop_polling = threading.Event()

    def poll_memory():
        nonlocal peak_rss
        while not stop_polling.is_set():
            try:
                current = _process.memory_info().rss
                peak_rss = max(peak_rss, current)
            except Exception:
                pass
            time.sleep(poll_interval)

    before = _process.memory_info().rss
    peak_rss = before

    poller = threading.Thread(target=poll_memory, daemon=True)
    poller.start()

    try:
        yield
    finally:
        stop_polling.set()
        poller.join(timeout=1.0)
        after = _process.memory_info().rss

    _tracker.results.append({
        'query': query_name,
        'peak_mb': (peak_rss - before) / 1024 / 1024,
        'delta_mb': (after - before) / 1024 / 1024,
    })


def get_report():
    return _tracker.get_report()


def clear_report():
    _tracker.clear()
