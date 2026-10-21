"""Shared fixtures and helpers for the ``return_format`` / ``duckdb_con`` tests.

Style follows tests/utils/sofa2/ and tests/utils/med_unit_converter/: module-level
functions and fixtures, no test classes.
"""

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import logging

import duckdb
import pandas as pd
import polars as pl
import pytest

DEMO_DIR = str(Path(__file__).parents[3] / "clifpy" / "data" / "clif_demo")
FORMATS = ("polars", "polars_lazy", "duckdb", "pandas")
ZONES = ("UTC", "US/Eastern", "US/Central")


@pytest.fixture
def demo_dir():
    """Packaged demo parquet: tz-aware UTC ``TIMESTAMPTZ``, the normal case."""
    return DEMO_DIR


@pytest.fixture
def hostile_default_tz():
    """Pin DuckDB's default connection to a zone that is neither UTC nor any target.

    Copied from tests/utils/test_tz_contract.py so a developer machine that happens to
    match the target zone cannot mask a one-hour bug.
    """
    duckdb.sql("SET TimeZone = 'America/Los_Angeles'")
    yield
    duckdb.sql("SET TimeZone = 'UTC'")


@pytest.fixture
def naive_dttm_parquet(tmp_path):
    """Parquet with a tz-NAIVE ``*_dttm`` column -- the ``replace_time_zone`` branch.

    The demo data is entirely tz-aware, so this branch is otherwise untested.
    """
    path = tmp_path / "clif_naivetbl.parquet"
    pd.DataFrame({
        "hospitalization_id": ["a", "b"],
        "recorded_dttm": [datetime(2024, 7, 1, 12, 0), datetime(2024, 12, 1, 12, 0)],
        "value": [1.0, 2.0],
    }).to_parquet(path)
    return str(tmp_path), "naivetbl"


@pytest.fixture
def float_id_parquet(tmp_path):
    """Parquet whose ``hospitalization_id`` is Float64 -- proves 123456.0 -> "123456"."""
    path = tmp_path / "clif_floattbl.parquet"
    pd.DataFrame({
        "hospitalization_id": [123456.0, 234567.0],
        "recorded_dttm": pd.to_datetime(["2024-07-01T12:00:00Z", "2024-07-02T12:00:00Z"]),
        "value": [1.0, 2.0],
    }).to_parquet(path)
    return str(tmp_path), "floattbl"


@pytest.fixture
def demo_csv(tmp_path):
    """CSV copy of a demo table, for the CSV path and the scan_csv String fix."""
    out = tmp_path / "clif_vitals.csv"
    duckdb.sql(
        f"COPY (FROM parquet_scan('{DEMO_DIR}/clif_vitals.parquet') LIMIT 200) "
        f"TO '{out}' (FORMAT CSV, HEADER)"
    )
    return str(tmp_path)


def dttm_utc_instants(obj, col="recorded_dttm"):
    """Normalize any of the four return types to a list of UTC instants.

    Lets cross-format equality be a single comparison. Per docs/tz_dx.md, tz tests
    assert instant preservation rather than hardcoded wall-clock hours.
    """
    if isinstance(obj, pl.LazyFrame):
        obj = obj.collect()
    if isinstance(obj, duckdb.DuckDBPyRelation):
        obj = obj.df()
    if isinstance(obj, pd.DataFrame):
        return [ts.to_pydatetime() for ts in obj[col].dt.tz_convert("UTC")]
    return obj[col].dt.convert_time_zone("UTC").to_list()


def dttm_dtype_tz(obj, col="recorded_dttm"):
    """Return the timezone *label* of a datetime column, whatever the frame type."""
    if isinstance(obj, pl.LazyFrame):
        return obj.collect_schema()[col].time_zone
    if isinstance(obj, pl.DataFrame):
        return obj.schema[col].time_zone
    if isinstance(obj, duckdb.DuckDBPyRelation):
        obj = obj.df()
    return str(obj[col].dt.tz)


@pytest.fixture
def io_log():
    """Capture records from the ``clifpy.utils.io`` logger directly.

    Not caplog: clifpy/utils/logging_config.py:150 sets ``root_logger.propagate =
    False``, so once any other test configures logging, caplog stops seeing these
    records. Attaching our own handler is immune to that global state.
    """
    records = []

    class _Collect(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    logger = logging.getLogger("clifpy.utils.io")
    handler = _Collect()
    logger.addHandler(handler)
    previous = logger.level
    logger.setLevel(logging.WARNING)
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)
