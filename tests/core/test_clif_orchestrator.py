"""Tests for ClifOrchestrator — timezone handling around dose-unit conversion (#144).

Issue #144
----------
A timezone specified on the orchestrator (``ClifOrchestrator(timezone=X)``) is not
honored in ``df_converted['admin_dttm']`` after dose-unit conversion: the converted
timestamps come back in the *machine's* local zone instead of ``X``.

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
configured ``timezone`` differs from the machine/default-connection zone -- hence the
``hostile_default_tz`` fixture and the parametrized timezones (catching the subtle 1-hour
case, not only the glaring UTC one).

Why orchestrator-level (not a bare-converter test) on ``main``
--------------------------------------------------------------
These tests drive the *issue's actual flow* end-to-end through ``ClifOrchestrator``. A
standalone ``convert_dose_units_by_med_category`` call is **not viable on main** for demo
data: the demo ``medication_admin_continuous`` has no ``weight_kg`` column, so
``standardize_dose_to_base_units`` unconditionally calls ``find_most_recent_weight(med_df,
vitals_df=None)``, whose SQL references an unregistered ``vitals_df`` relation and raises
``InvalidInputException`` -- an error orthogonal to #144 that ``xfail`` would silently
mask. The orchestrator loads vitals internally and passes it to the converter, so the
converter's #144 mis-render is still exercised here, faithfully and without that setup trap.
(The bare-converter mechanism guard lives on the dev branch's
``tests/utils/med_unit_converter/test_unit_converter.py``, where the converter tolerates a
missing ``vitals_df``.)

xfail on ``main``
-----------------
Marked ``xfail(strict=True)`` because #144 is unfixed here: these assert the *correct*
behavior, so they fail on ``main`` (recorded as xfailed -> suite stays green). Verified the
failure reason is the #144 tz-mismatch assertion (``admin_dttm tz X -> America/Los_Angeles``),
NOT an unrelated error, before applying the marker. When the main-compatible fix lands they
xpass -> strict turns that into a failure, prompting removal of the marker.

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

    Distinct from every parametrized ``timezone`` below, so configured != machine in every
    case -- the condition under which #144 manifests. Restores UTC on teardown.
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


@pytest.mark.xfail(strict=True, reason="#144: configured timezone lost in df_converted on main")
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


@pytest.mark.xfail(strict=True, reason="#144: configured timezone lost in df_converted on main")
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
