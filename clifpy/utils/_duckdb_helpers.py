"""Shared DuckDB temp-table lifecycle helpers.

Used by compute-heavy pipelines (currently `clifpy.utils.unit_converter`; sofa2
will migrate here in a separate pass) that promote pandas inputs into DuckDB
temp tables (boundary 2a per `docs/duckdb_perf_guide.md`) and need a single
process-wide registry to drop them at exit.

Functions
---------
- `_register_temp_table(name)`: track a temp table for later cleanup.
- `_drop_temp_table(name)`: drop one specific table and remove from the registry.
- `_cleanup_temp_tables()`: drop everything currently in the registry.
"""
from __future__ import annotations

import duckdb


# =============================================================================
# Temp Table Registry
# =============================================================================
#
# A single module-level list shared by every importer. Functions that promote
# pandas inputs into DuckDB temp tables register them here; the orchestrator's
# `finally` block calls `_cleanup_temp_tables()` to drop them on exit.
#
# Best-effort: drops swallow exceptions (a lost connection, a missing table)
# because cleanup must never raise on top of an existing error.

_TEMP_TABLE_REGISTRY: list[str] = []


def _register_temp_table(name: str) -> None:
    """Register a temp table for cleanup after pipeline completes.

    No-op if the name is already in the registry.
    """
    if name not in _TEMP_TABLE_REGISTRY:
        _TEMP_TABLE_REGISTRY.append(name)


def _drop_temp_table(name: str) -> None:
    """Drop a specific temp table and remove it from the registry.

    Best-effort: silently ignores DuckDB errors and missing-name errors.
    """
    try:
        duckdb.execute(f"DROP TABLE IF EXISTS {name}")
    except Exception:
        pass
    try:
        _TEMP_TABLE_REGISTRY.remove(name)
    except ValueError:
        pass


def _cleanup_temp_tables() -> None:
    """Drop all registered temp tables. Best-effort; safe to call repeatedly."""
    while _TEMP_TABLE_REGISTRY:
        name = _TEMP_TABLE_REGISTRY.pop()
        try:
            duckdb.execute(f"DROP TABLE IF EXISTS {name}")
        except Exception:
            pass
