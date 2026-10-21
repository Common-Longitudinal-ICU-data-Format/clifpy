"""How `site_tz` is resolved, and that the resolution survives a hostile environment.

Resolution is two steps, which is worth keeping distinct:

1. **resolve** — explicit ``site_tz`` argument, else ``timezone`` from the config file,
   else ``None``
2. **apply** — convert to the resolved zone, or to UTC when resolution produced nothing

The UTC default is the last resort *after* resolution, not a competing rule.

Step 2 of resolution used to be skipped whenever the caller passed ``table_path`` and
``table_format_type`` explicitly — the most common call shape — so a configured site
timezone was silently ignored and everything came back UTC.
"""

from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pytest

from clifpy.utils.io import load_data


@pytest.fixture
def eastern_site(tmp_path):
    """A CLIF directory whose config declares US/Eastern, holding MIMIC-shaped data.

    Two rows — one summer, one winter — dated ~150 years forward, the way CLIF-MIMIC
    de-identifies. Both encode an intended 13:00 US/Eastern: MIMIC's ETL localized with
    pytz, whose DST table is frozen past 2037, so both land on 18:00 UTC (EST, -5).
    """
    duckdb.sql("SET timezone='UTC'")
    duckdb.sql(f"""COPY (
        SELECT '1'::VARCHAR AS hospitalization_id,
               TIMESTAMPTZ '2180-07-15 18:00:00-00' AS recorded_dttm,
               'heart_rate' AS vital_category, 88.0 AS vital_value
        UNION ALL
        SELECT '2', TIMESTAMPTZ '2180-12-15 18:00:00-00', 'heart_rate', 92.0
    ) TO '{tmp_path}/clif_vitals.parquet' (FORMAT PARQUET)""")
    (tmp_path / "config.yaml").write_text(
        f'data_directory: "{tmp_path}"\nfiletype: "parquet"\ntimezone: "US/Eastern"\n'
    )
    return str(tmp_path), str(tmp_path / "config.yaml")


def _tz(df):
    return df.schema["recorded_dttm"].time_zone


@pytest.mark.tz_conversion
def test_config_timezone_used_when_paths_omitted(eastern_site):
    directory, cfg = eastern_site
    assert _tz(load_data("vitals", config_path=cfg)) == "US/Eastern"


@pytest.mark.tz_conversion
def test_config_timezone_used_when_paths_given_explicitly(eastern_site):
    """The regression: passing the path must not discard the configured timezone."""
    directory, cfg = eastern_site
    assert _tz(load_data("vitals", directory, "parquet", config_path=cfg)) == "US/Eastern"


@pytest.mark.tz_conversion
def test_explicit_site_tz_beats_config(eastern_site):
    directory, cfg = eastern_site
    got = load_data("vitals", directory, "parquet", config_path=cfg, site_tz="US/Central")
    assert _tz(got) == "US/Central"


@pytest.mark.tz_conversion
def test_utc_when_no_config_resolves(demo_dir, tmp_path, monkeypatch):
    """No config anywhere is a supported state, not an error: fall back to UTC."""
    monkeypatch.chdir(tmp_path)          # nothing auto-detectable in cwd
    got = load_data("vitals", demo_dir, "parquet", sample_size=5)
    assert _tz(got) == "UTC"


@pytest.mark.tz_conversion
@pytest.mark.parametrize("fmt", ["polars", "polars_lazy", "pandas"])
def test_mimic_from_a_non_eastern_machine(eastern_site, hostile_default_tz, fmt):
    """The real-world case: MIMIC data, Eastern config, analyst on a Chicago machine.

    Three hazards at once, and all three must hold:

    - the configured zone wins even though the paths were passed explicitly
    - the machine/connection zone (America/Chicago, via hostile_default_tz) never leaks
    - DST stays frozen, matching MIMIC's pytz encoder -- summer and winter both read
      13:00. A rule-projecting decoder would give 14:00 in summer and 13:00 in winter,
      a one-hour split across the year that would look like real variation.
    """
    directory, cfg = eastern_site
    got = load_data("vitals", directory, "parquet", config_path=cfg, return_format=fmt)

    if fmt == "pandas":
        assert str(got["recorded_dttm"].dt.tz) == "US/Eastern"
        assert list(got["recorded_dttm"].dt.hour) == [13, 13]
    else:
        if isinstance(got, pl.LazyFrame):
            got = got.collect()
        assert got.schema["recorded_dttm"].time_zone == "US/Eastern"
        assert got["recorded_dttm"].dt.hour().to_list() == [13, 13]


@pytest.mark.tz_conversion
def test_polars_and_pandas_agree_on_mimic_dates(eastern_site, hostile_default_tz):
    """Both load paths decode far-future dates identically."""
    directory, cfg = eastern_site
    common = dict(table_path=directory, table_format_type="parquet", config_path=cfg)
    pl_hours = load_data("vitals", **common, return_format="polars")["recorded_dttm"].dt.hour().to_list()
    pd_hours = list(load_data("vitals", **common, return_format="pandas")["recorded_dttm"].dt.hour)
    assert pl_hours == pd_hours == [13, 13]
