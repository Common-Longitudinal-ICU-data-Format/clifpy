"""Why ``_convert_dttm_cols_polars`` emits ``convert_time_zone`` unconditionally.

A polars_lazy frame is a PYTHON SCAN over a live DuckDB relation, so its tz label
otherwise follows whatever the connection's TimeZone says at .collect() time. Because
conversion is instant-preserving, emitting it always pins the label deterministically.

The call looks like a no-op when site_tz is None and the source is already UTC. It is
not. These tests fail if someone removes it.
"""

import duckdb
import polars as pl
import pytest

from clifpy.utils.io import load_data
from tests.utils.io_return_formats.conftest import dttm_dtype_tz


@pytest.fixture
def repin_global_tz():
    """Re-pin DuckDB's default connection mid-test, then restore it."""
    def _repin(zone):
        duckdb.execute(f"SET timezone='{zone}';")
    yield _repin
    duckdb.execute("SET timezone='UTC';")


@pytest.mark.tz_conversion
def test_lazy_label_survives_global_repin_with_site_tz_none(demo_dir, repin_global_tz):
    """site_tz=None still pins the label to UTC after the connection moves.

    THIS is the test the unconditional convert exists for. Remove that call and the
    label follows the connection to Asia/Tokyo.
    """
    lf = load_data("vitals", demo_dir, "parquet", sample_size=50,
                   return_format="polars_lazy")
    repin_global_tz("Asia/Tokyo")
    assert lf.collect().schema["recorded_dttm"].time_zone == "UTC"


@pytest.mark.tz_conversion
def test_lazy_label_survives_global_repin_with_site_tz_set(demo_dir, repin_global_tz):
    """An explicit site_tz is likewise unaffected by a later re-pin."""
    lf = load_data("vitals", demo_dir, "parquet", sample_size=50,
                   site_tz="US/Eastern", return_format="polars_lazy")
    repin_global_tz("Asia/Tokyo")
    assert lf.collect().schema["recorded_dttm"].time_zone == "US/Eastern"


@pytest.mark.tz_conversion
@pytest.mark.parametrize("fmt", ["polars", "pandas"])
def test_materialized_formats_are_immune_to_repin(demo_dir, repin_global_tz, fmt):
    """polars and pandas materialize at call time, so a re-pin cannot reach them."""
    obj = load_data("vitals", demo_dir, "parquet", sample_size=50, return_format=fmt)
    repin_global_tz("Asia/Tokyo")
    assert dttm_dtype_tz(obj) == "UTC"


@pytest.mark.tz_conversion
def test_duckdb_relation_does_float_by_design(demo_dir, repin_global_tz):
    """The 'duckdb' relation DOES follow the connection -- documented, not a bug.

    This is why site_tz is never applied to a relation, and why duckdb_con exists.
    """
    rel = load_data("vitals", demo_dir, "parquet", sample_size=50, return_format="duckdb")
    assert str(rel.df()["recorded_dttm"].dt.tz) == "UTC"
    repin_global_tz("Asia/Tokyo")
    assert str(rel.df()["recorded_dttm"].dt.tz) == "Asia/Tokyo"


@pytest.mark.tz_conversion
def test_instants_unchanged_by_the_float(demo_dir, repin_global_tz):
    """Whatever the label does, the underlying instants never move."""
    rel = load_data("vitals", demo_dir, "parquet", sample_size=50, return_format="duckdb")
    before = list(rel.df()["recorded_dttm"].dt.tz_convert("UTC"))
    repin_global_tz("Asia/Tokyo")
    assert list(rel.df()["recorded_dttm"].dt.tz_convert("UTC")) == before
