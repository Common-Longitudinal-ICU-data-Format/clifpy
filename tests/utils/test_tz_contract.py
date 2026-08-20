"""Timezone *contract* characterization tests, anchored on ``main``'s behavior.

Purpose
-------
Encode the timezone contract that ``main`` (the branch existing users depend on)
exposes through ``clifpy.utils.io``. These tests are written to **pass on ``main``**
and act as a differential probe: running the *same* file on a feature branch makes
any divergence show up as a failure that points at the exact behavior that changed.

``main``'s contract pinned here:

- **Eager** ``load_data(..., site_tz=X)`` -> **tz-aware** ``*_dttm`` columns in zone X.
- **Lazy** ``load_data(..., lazy=True)`` + ``fetch_lazy_result(..., site_tz=X)`` ->
  **tz-aware** columns in zone X (identical on the dev branch -- proves lazy is stable).
- Timezone conversion is a pure relabeling: the absolute (UTC) instant is preserved.
- Correctness derives from the explicit ``site_tz`` argument, not the machine's OS
  timezone or DuckDB's ambient default-connection zone.

Only APIs present on both ``main`` and the feature branches are exercised
(``load_data``, ``fetch_lazy_result``, ``convert_datetime_columns_to_site_tz``); the
dev-branch-only ``return_rel`` path is intentionally not used, so the file is portable.

Pattern follows ``tests/utils/med_unit_converter/test_unit_converter.py``: module-level
fixtures + module-level ``test_*`` functions, real demo data (no mocks), plain ``assert``
and ``pd.testing.assert_series_equal``.
"""
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from clifpy.utils.io import (
    load_data,
    fetch_lazy_result,
    convert_datetime_columns_to_site_tz,
)


# --- Fixtures ---
@pytest.fixture
def demo_data_dir() -> str:
    """Path to the packaged demo data directory (present on every branch).

    Returns
    -------
    str
        Absolute path to ``clifpy/data/clif_demo``.
    """
    return str(Path(__file__).parent.parent.parent / "clifpy" / "data" / "clif_demo")


@pytest.fixture
def hostile_default_tz():
    """Force DuckDB's process-wide *default* connection to a non-UTC zone.

    Reproduces the issue-#144 ambient state on any runner regardless of its OS
    timezone: a code path that renders a TIMESTAMPTZ through the default connection
    would pick up this hostile zone. ``main``'s eager path uses an isolated
    connection + pandas ``tz_convert`` and is therefore immune -- exactly the
    property asserted by :func:`test_eager_load_correct_under_hostile_default_tz`.
    """
    duckdb.sql("SET TimeZone = 'America/Los_Angeles'")
    try:
        yield
    finally:
        duckdb.sql("SET TimeZone = 'UTC'")


def _tzname(series: pd.Series):
    """Return the zone name of a datetime Series, or ``None`` if tz-naive.

    This is the aware/naive discriminator: ``main`` returns a zone name for eager
    site_tz loads; the dev branch returns ``None`` (naive).
    """
    tz = series.dt.tz
    return str(tz) if tz is not None else None


# ===========================================
# Eager load contract  --  main: tz-AWARE (the regression sentinel)
# ===========================================
@pytest.mark.parametrize("site_tz", ["UTC", "US/Eastern", "US/Central"])
def test_eager_load_site_tz_is_tz_aware(demo_data_dir, site_tz):
    """Eager ``load_data(site_tz=X)`` returns tz-aware datetimes in zone X.

    This is the contract that separates ``main`` (aware) from the dev branch
    (naive) -- the single sharpest discriminator.

    Parameters
    ----------
    demo_data_dir : str
        Fixture path to demo data.
    site_tz : str
        Target timezone under test.
    """
    df: pd.DataFrame = load_data(
        table_name="vitals",
        table_path=demo_data_dir,
        table_format_type="parquet",
        sample_size=5,
        site_tz=site_tz,
        return_format="pandas",
    )
    assert "recorded_dttm" in df.columns
    assert df["recorded_dttm"].dt.tz is not None, (
        f"eager load_data(site_tz={site_tz!r}) must be tz-aware on main; got tz-naive"
    )
    assert _tzname(df["recorded_dttm"]) == site_tz


def test_eager_load_site_tz_none_is_utc_aware(demo_data_dir):
    """With ``site_tz=None`` the eager load returns tz-aware UTC (no conversion)."""
    df: pd.DataFrame = load_data(
        table_name="vitals",
        table_path=demo_data_dir,
        table_format_type="parquet",
        sample_size=5,
        site_tz=None,
        return_format="pandas",
    )
    assert df["recorded_dttm"].dt.tz is not None
    assert _tzname(df["recorded_dttm"]) in ("UTC", "utc")


@pytest.mark.parametrize(
    "site_tz,expected_offsets",
    [("US/Eastern", {4, 5}), ("US/Central", {5, 6})],
)
def test_eager_load_hour_offset(demo_data_dir, site_tz, expected_offsets):
    """Converted local hour trails UTC by the (DST-dependent) zone offset.

    Characterizes conversion correctness; modulo handles day wrap so every sampled
    row must match one of the zone's offsets.
    """
    common = dict(
        table_name="vitals",
        table_path=demo_data_dir,
        table_format_type="parquet",
        sample_size=5,
    )
    df_utc: pd.DataFrame = load_data(**common, site_tz=None, return_format="pandas")
    df_local: pd.DataFrame = load_data(**common, site_tz=site_tz, return_format="pandas")
    diffs = (df_utc["recorded_dttm"].dt.hour - df_local["recorded_dttm"].dt.hour) % 24
    assert set(diffs.unique()).issubset(expected_offsets), (
        f"{site_tz}: expected offsets {expected_offsets}, got {sorted(diffs.unique())}"
    )


def test_eager_load_multiple_dttm_columns_all_aware(demo_data_dir):
    """ADT has two dttm columns (``in_dttm``, ``out_dttm``); both must be tz-aware."""
    df: pd.DataFrame = load_data(
        table_name="adt",
        table_path=demo_data_dir,
        table_format_type="parquet",
        sample_size=5,
        site_tz="US/Eastern",
        return_format="pandas",
    )
    dttm_cols = [c for c in df.columns if "dttm" in c.lower()]
    assert dttm_cols, "expected at least one dttm column in adt"
    for col in dttm_cols:
        assert df[col].dt.tz is not None, f"{col} must be tz-aware"


# ===========================================
# Lazy load contract  --  identical on main and dev (stable, not a regression)
# ===========================================
def test_lazy_fetch_result_is_tz_aware(demo_data_dir):
    """Lazy path (``lazy=True`` + ``fetch_lazy_result(site_tz)``) is tz-aware.

    Passes on both branches -- included to prove the lazy path is NOT a source of
    divergence.
    """
    rel = load_data(
        table_name="vitals",
        table_path=demo_data_dir,
        table_format_type="parquet",
        sample_size=5,
        lazy=True,
    )
    df: pd.DataFrame = fetch_lazy_result(rel, site_tz="US/Eastern")
    assert df["recorded_dttm"].dt.tz is not None
    assert _tzname(df["recorded_dttm"]) == "US/Eastern"


# ===========================================
# Instant preservation  --  conversion is a pure relabeling
# ===========================================
def test_site_tz_conversion_preserves_utc_instant(demo_data_dir):
    """Site-tz conversion must preserve the absolute UTC instant."""
    common = dict(
        table_name="vitals",
        table_path=demo_data_dir,
        table_format_type="parquet",
        sample_size=5,
    )
    df_utc: pd.DataFrame = load_data(**common, site_tz=None, return_format="pandas")
    df_local: pd.DataFrame = load_data(**common, site_tz="US/Eastern", return_format="pandas")
    utc_instants = df_utc["recorded_dttm"].dt.tz_convert("UTC").reset_index(drop=True)
    local_instants = (
        df_local["recorded_dttm"].dt.tz_convert("UTC").reset_index(drop=True)
    )
    pd.testing.assert_series_equal(utc_instants, local_instants, check_names=False)


# ===========================================
# Machine / ambient-zone independence
# ===========================================
def test_eager_load_correct_under_hostile_default_tz(demo_data_dir, hostile_default_tz):
    """Eager load stays correctly Eastern even with the default connection in LA.

    Proves correctness derives from the ``site_tz`` argument, not the ambient zone.
    """
    df: pd.DataFrame = load_data(
        table_name="vitals",
        table_path=demo_data_dir,
        table_format_type="parquet",
        sample_size=5,
        site_tz="US/Eastern",
        return_format="pandas",
    )
    assert df["recorded_dttm"].dt.tz is not None
    assert _tzname(df["recorded_dttm"]) == "US/Eastern"


# ===========================================
# convert_datetime_columns_to_site_tz helper (pandas, tz-aware)
# ===========================================
def test_convert_datetime_helper_returns_tz_aware():
    """The pandas helper returns tz-aware columns in the target zone, instant preserved."""
    df = pd.DataFrame(
        {
            "event_dttm": pd.to_datetime(
                ["2023-01-01 12:00:00", "2023-06-01 12:00:00"]
            ).tz_localize("UTC"),
            "value": [1, 2],
        }
    )
    result: pd.DataFrame = convert_datetime_columns_to_site_tz(
        df, "US/Central", verbose=False
    )
    assert result["event_dttm"].dt.tz is not None
    assert _tzname(result["event_dttm"]) == "US/Central"
    assert (
        result["event_dttm"].dt.tz_convert("UTC").tolist()
        == df["event_dttm"].tolist()
    )
