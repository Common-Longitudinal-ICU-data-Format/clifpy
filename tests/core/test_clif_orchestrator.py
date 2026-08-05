"""Tests for ClifOrchestrator — timezone handling around dose-unit conversion (#144).

Issue #144
----------
A timezone specified on the orchestrator (``ClifOrchestrator(timezone=X)``) was not
honored in ``df_converted['admin_dttm']`` after dose-unit conversion: the converted
timestamps came back in the *machine's* local zone instead of ``X``.

Root cause (documentation + future warning)
-------------------------------------------
Load and convert use *different DuckDB connections with different TimeZone settings*:

- The **load** path (``load_parquet_with_tz``) reads on a fresh **isolated** connection
  (``duckdb.connect()``), pins ``SET timezone='UTC'`` on that con only, ``fetchdf()``s and
  ``close()``s it, then applies the site timezone in pandas (``tz_convert``) -> tz-aware,
  correct.
- The **converter** (``convert_dose_units_by_med_category``) runs bare ``duckdb.sql(...)`` /
  ``.to_df()`` on the process-wide **default** connection, which is never pinned and so
  renders ``TIMESTAMPTZ`` in the machine's OS zone.

The bug is a *label-only* error (the instant is preserved) and is visible only when the
configured ``timezone`` differs from the machine/default-connection zone -- which is why
these tests force that difference via ``hostile_default_tz`` and parametrize the timezone
(catching the subtle 1-hour case, not just the glaring UTC one).

Prevention: any code materializing a ``TIMESTAMPTZ`` to pandas on the default connection
must pin that connection's TimeZone first (or carry timestamps tz-naive/UTC and attach the
zone in pandas) -- never rely on the ambient default-connection zone.
"""
from pathlib import Path

import duckdb
import pytest

from clifpy import ClifOrchestrator

_DEMO_DIR = str(Path(__file__).parents[2] / "clifpy" / "data" / "clif_demo")


@pytest.fixture
def hostile_default_tz():
    """Pin DuckDB's *default* connection to a non-UTC zone (Los Angeles).

    Distinct from every parametrized ``timezone`` below, so configured != machine in
    every case -- the condition under which #144 manifests. Restores UTC on teardown.
    """
    duckdb.sql("SET TimeZone = 'America/Los_Angeles'")
    try:
        yield
    finally:
        duckdb.sql("SET TimeZone = 'UTC'")


def _assert_admin_dttm_unchanged(converted_df, src_series):
    """The converted ``admin_dttm`` must match the loaded one in tz-label and instants."""
    out = converted_df["admin_dttm"]
    assert str(out.dt.tz) == str(src_series.dt.tz), (
        f"conversion changed admin_dttm tz {src_series.dt.tz} -> {out.dt.tz} (issue #144)"
    )
    assert sorted(out.dropna().tolist()) == sorted(src_series.dropna().tolist())


@pytest.mark.parametrize("timezone", ["UTC", "US/Eastern", "US/Central"])
def test_orchestrator_continuous_conversion_respects_timezone(
    timezone, hostile_default_tz, tmp_path
):
    """Configured ``timezone`` must survive continuous-med dose conversion end-to-end.

    Reproduces the #144 flow: ``ClifOrchestrator(timezone=X)`` ->
    ``convert_dose_units_for_continuous_meds`` -> assert the saved
    ``df_converted['admin_dttm']`` is still in ``X`` (equal to the loaded ``df``), not the
    hostile Los Angeles default-connection zone.
    """
    co = ClifOrchestrator(
        data_directory=_DEMO_DIR,
        filetype="parquet",
        timezone=timezone,
        output_directory=str(tmp_path),
    )
    co.convert_dose_units_for_continuous_meds(
        preferred_units={"fentanyl": "mcg/min"}, override=True, save_to_table=True
    )
    src = co.medication_admin_continuous.df["admin_dttm"]
    _assert_admin_dttm_unchanged(co.medication_admin_continuous.df_converted, src)


@pytest.mark.parametrize("timezone", ["UTC", "US/Eastern", "US/Central"])
def test_orchestrator_intermittent_conversion_respects_timezone(
    timezone, hostile_default_tz, tmp_path
):
    """Same as the continuous case for intermittent meds (same underlying converter)."""
    co = ClifOrchestrator(
        data_directory=_DEMO_DIR,
        filetype="parquet",
        timezone=timezone,
        output_directory=str(tmp_path),
    )
    co.convert_dose_units_for_intermittent_meds(
        preferred_units={"fentanyl": "mg"}, override=True, save_to_table=True
    )
    src = co.medication_admin_intermittent.df["admin_dttm"]
    _assert_admin_dttm_unchanged(co.medication_admin_intermittent.df_converted, src)
