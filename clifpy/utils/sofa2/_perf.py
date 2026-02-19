"""Lightweight performance profiling utilities for SOFA-2 pipeline.

Provides:
- StepTimer: Collects per-step wall-clock timing via context manager
- NoOpTimer: Zero-cost drop-in replacement when profiling is off
- _register_temp_table / _cleanup_temp_tables: Temp table lifecycle management
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field

import duckdb


# =============================================================================
# Temp Table Registry
# =============================================================================

_TEMP_TABLE_REGISTRY: list[str] = []


def _register_temp_table(name: str):
    """Register a temp table for cleanup after pipeline completes."""
    if name not in _TEMP_TABLE_REGISTRY:
        _TEMP_TABLE_REGISTRY.append(name)


def _cleanup_temp_tables():
    """Drop all registered temp tables. Best-effort, ignores errors."""
    while _TEMP_TABLE_REGISTRY:
        name = _TEMP_TABLE_REGISTRY.pop()
        try:
            duckdb.execute(f"DROP TABLE IF EXISTS {name}")
        except Exception:
            pass


# =============================================================================
# Step Timer
# =============================================================================


@dataclass
class StepTimer:
    """Collects per-step wall-clock timing measurements.

    Usage:
        timer = StepTimer()
        with timer.step("brain"):
            brain_score = _calculate_brain_subscore(...)
        print(timer.report(cohort_size=1000))
    """

    results: list[dict] = field(default_factory=list)
    _pending: dict[str, float] = field(default_factory=dict, repr=False)

    @contextmanager
    def step(self, name: str):
        start = time.perf_counter()
        try:
            yield
        finally:
            self.results.append({
                'step': name,
                'elapsed_s': time.perf_counter() - start,
            })

    def start(self, name: str):
        """Record start time for a named step. Pair with stop()."""
        self._pending[name] = time.perf_counter()

    def stop(self, name: str):
        """Record end time and finalize the step started with start()."""
        if name in self._pending:
            self.results.append({
                'step': name,
                'elapsed_s': time.perf_counter() - self._pending.pop(name),
            })

    @property
    def total(self) -> float:
        return sum(r['elapsed_s'] for r in self.results)

    def report(self, cohort_size: int | None = None) -> str:
        lines = []
        lines.append(f"{'Step':<35} {'Time (s)':<12} {'% Total':<10}")
        lines.append("-" * 57)
        total = self.total
        for r in self.results:
            pct = r['elapsed_s'] / total * 100 if total > 0 else 0
            lines.append(f"{r['step']:<35} {r['elapsed_s']:<12.3f} {pct:<10.1f}")
        lines.append("-" * 57)
        lines.append(f"{'TOTAL':<35} {total:<12.3f}")
        if cohort_size:
            lines.append(f"{'Time per ID (ms)':<35} {total / cohort_size * 1000:<12.2f}")
        return "\n".join(lines)


class NoOpTimer:
    """Zero-cost no-op timer. Drop-in replacement for StepTimer when profiling is off."""

    results: list = []

    @contextmanager
    def step(self, name: str):
        yield

    def start(self, name: str):
        pass

    def stop(self, name: str):
        pass

    @property
    def total(self) -> float:
        return 0.0

    def report(self, cohort_size: int | None = None) -> str:
        return ""
