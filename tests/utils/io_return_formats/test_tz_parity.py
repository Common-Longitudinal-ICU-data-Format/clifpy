"""polars must decode timezones like pytz -- the constraint from docs/tz_dx.md 11.

CLIF-MIMIC de-identifies by shifting dates ~150 years forward, past pytz's 2037 DST
cliff, and its ETL *encoded* with pytz. Decoding with a rule-based engine (zoneinfo,
DuckDB ICU) shifts every DST-season timestamp by an hour and corrupts time-of-day
analyses. polars uses chrono-tz, which freezes its table like pytz -- these tests exist
so a future polars that switches to rule projection fails loudly instead of silently.

Cases:
- 2180-07-15 17:00 UTC -> US/Eastern is the canonical case from tz_dx.md 11
- the two known divergences (Python-object export, spring-forward gaps) are asserted
  as tests so nobody "corrects" the engine-level assertions to match them
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import polars as pl
import pytest
import pytz

from clifpy.utils.io import load_data

FAR_FUTURE_UTC = datetime(2180, 7, 15, 17, 0, tzinfo=ZoneInfo("UTC"))
PYTZ_EQUIVALENT_ZONES = [
    "US/Eastern", "America/New_York", "US/Central", "America/Chicago", "America/Los_Angeles",
]


def _pandas_hour(zone):
    s = pd.Series(pd.to_datetime(["2180-07-15 17:00:00+00:00"]))
    return s.dt.tz_convert(pytz.timezone(zone)).dt.hour.iloc[0]


def _polars_hour(zone):
    return pl.Series("t", [FAR_FUTURE_UTC]).dt.convert_time_zone(zone).dt.hour().item()


@pytest.mark.tz_conversion
@pytest.mark.parametrize("zone", PYTZ_EQUIVALENT_ZONES)
def test_polars_matches_pytz_at_far_future_dates(zone):
    """polars' engine agrees with the pandas/pytz decoder past the 2037 DST cliff.

    If this fails, polars has switched to projecting DST rules forward and the polars
    load path can no longer decode CLIF-MIMIC correctly. Do NOT relax the assertion --
    see docs/io_return_formats_dx.md 3.1 before changing anything here.
    """
    assert _polars_hour(zone) == _pandas_hour(zone)


@pytest.mark.tz_conversion
def test_canonical_tz_dx_case_is_eastern_standard_time():
    """The exact case tz_dx.md 11 documents: 12:00 EST, not 13:00 EDT."""
    assert _polars_hour("US/Eastern") == 12
    assert _pandas_hour("US/Eastern") == 12


@pytest.mark.tz_conversion
def test_python_object_export_diverges_from_the_engine():
    """KNOWN DIVERGENCE, asserted so it stays visible.

    polars' Rust engine says 12:00 EST; ``.to_list()`` hands the instant to zoneinfo,
    which projects the DST rule and says 13:00 EDT. Column-level work is safe; per-row
    Python extraction is not. This test documents the gap -- it is not a bug to "fix"
    by relaxing test_canonical_tz_dx_case_is_eastern_standard_time.
    """
    s = pl.Series("t", [FAR_FUTURE_UTC]).dt.convert_time_zone("US/Eastern")
    assert s.dt.hour().item() == 12            # engine / chrono-tz
    assert s.to_list()[0].hour == 13           # Python object / zoneinfo


@pytest.mark.tz_conversion
def test_spring_forward_gap_nulls_in_polars_but_shifts_in_pandas():
    """KNOWN DIVERGENCE: polars has no equivalent of nonexistent='shift_forward'.

    A naive wall-clock inside the spring-forward gap nulls out under polars'
    non_existent='null'; pandas shifts it forward instead.
    """
    gap = datetime(2024, 3, 10, 2, 30)          # does not exist in US/Eastern

    got = (
        pl.Series("t", [gap])
        .dt.replace_time_zone("US/Eastern", ambiguous="earliest", non_existent="null")
        .to_list()[0]
    )
    assert got is None

    shifted = (
        pd.Series([gap]).dt
        .tz_localize("US/Eastern", ambiguous=True, nonexistent="shift_forward")
        .iloc[0]
    )
    assert shifted is not pd.NaT and shifted.hour == 3


@pytest.mark.tz_conversion
@pytest.mark.parametrize("site_tz", ["US/Eastern", "US/Central"])
def test_polars_and_pandas_load_paths_agree_on_wall_clock(demo_dir, site_tz, hostile_default_tz):
    """End to end: the two load paths produce the same local wall-clock hours."""
    common = dict(table_path=demo_dir, table_format_type="parquet",
                  sample_size=200, site_tz=site_tz)
    pl_hours = load_data("vitals", **common, return_format="polars")["recorded_dttm"].dt.hour().to_list()
    pd_hours = list(load_data("vitals", **common, return_format="pandas")["recorded_dttm"].dt.hour)
    assert pl_hours == pd_hours


@pytest.mark.tz_conversion
@pytest.mark.parametrize("site_tz", ["US/Eastern", "US/Central"])
def test_naive_column_is_localized_not_shifted(naive_dttm_parquet, site_tz):
    """A naive *_dttm keeps its wall-clock and gains the zone, matching tz_localize."""
    directory, table = naive_dttm_parquet
    df = load_data(table, directory, "parquet", site_tz=site_tz, return_format="polars")

    assert df.schema["recorded_dttm"].time_zone == site_tz
    assert df["recorded_dttm"].dt.hour().to_list() == [12, 12]   # wall-clock preserved
