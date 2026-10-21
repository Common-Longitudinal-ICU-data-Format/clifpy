"""The deprecated surface: ``return_rel``, ``lazy``, ``LazyRelation``.

Nothing is removed in this change. These tests pin that the old flags still work and
now warn, and that combining them with return_format is an error rather than a guess.
Removal order is driven by call-site counts -- see docs/io_return_formats_dx.md 6.
"""

import duckdb
import pandas as pd
import polars as pl
import pytest

from clifpy.utils.io import (
    LazyRelation, close_lazy_relation, fetch_lazy_result, load_data,
)


def test_return_rel_still_works_and_warns(demo_dir):
    """26 downstream call sites depend on this; it warns but keeps working."""
    with pytest.warns(DeprecationWarning, match="return_format='duckdb'"):
        rel = load_data("vitals", demo_dir, "parquet", sample_size=10, return_rel=True)
    assert isinstance(rel, duckdb.DuckDBPyRelation)


def test_lazy_still_works_and_warns(demo_dir):
    """lazy=True has zero users anywhere, but is deprecated rather than deleted."""
    with pytest.warns(DeprecationWarning, match="LazyRelation"):
        rel = load_data("vitals", demo_dir, "parquet", sample_size=10, lazy=True)
    assert isinstance(rel, LazyRelation)
    rel.close()


def test_fetch_lazy_result_still_works(demo_dir):
    with pytest.warns(DeprecationWarning):
        rel = load_data("vitals", demo_dir, "parquet", sample_size=10, lazy=True)
    df = fetch_lazy_result(rel, site_tz="US/Eastern")
    assert isinstance(df, pd.DataFrame)
    assert str(df["recorded_dttm"].dt.tz) == "US/Eastern"


@pytest.mark.parametrize("flag", ["return_rel", "lazy"])
def test_legacy_flag_with_return_format_raises(demo_dir, flag):
    """Ambiguity is an error, not a silent precedence rule."""
    with pytest.raises(ValueError, match="cannot be combined"):
        load_data("vitals", demo_dir, "parquet", return_format="polars", **{flag: True})


def test_return_rel_and_lazy_are_still_mutually_exclusive(demo_dir):
    with pytest.raises(ValueError, match="mutually exclusive"):
        load_data("vitals", demo_dir, "parquet", return_rel=True, lazy=True)


@pytest.mark.tz_conversion
def test_return_rel_still_ignores_site_tz(demo_dir):
    """The pre-existing contract: a relation is aware-UTC regardless of site_tz."""
    with pytest.warns(DeprecationWarning):
        rel = load_data("vitals", demo_dir, "parquet", sample_size=10,
                        site_tz="US/Eastern", return_rel=True)
    assert str(rel.df()["recorded_dttm"].dt.tz) == "UTC"


def test_lazyrelation_close_breaks_a_dependent_lazyframe(demo_dir):
    """The LazyRelation footgun, documented: closing invalidates dependents.

    duckdb_con avoids this by giving the caller an explicit connection lifetime.
    """
    con = duckdb.connect()
    con.execute("SET timezone='UTC';")
    lf = con.sql(f"SELECT * FROM parquet_scan('{demo_dir}/clif_vitals.parquet') LIMIT 5").pl(lazy=True)
    assert lf.collect().height == 5
    con.close()
    with pytest.raises(Exception, match="[Cc]onnection"):
        lf.collect()
