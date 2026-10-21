"""The polars and DuckDB backends must agree.

`'polars'` / `'polars_lazy'` scan with polars' own readers so predicates reach the file;
`'duckdb'` / `'pandas'` go through DuckDB SQL. Two backends means two ways to interpret
the caller's `columns` / `filters` / `sample_size` — which is exactly how the old
io_polars.py grew a bug where CSV `*_dttm` columns silently stayed strings.

Both compile from one spec (`_compile_filters_sql` / `_compile_filters_polars`); these
tests assert the results actually match across a matrix of argument combinations.
"""

import duckdb
import polars as pl
import pytest

from clifpy.utils.io import load_data, new_duckdb_con

COLS = ["hospitalization_id", "recorded_dttm", "vital_category", "vital_value"]

MATRIX = [
    pytest.param({}, id="no-args"),
    pytest.param({"columns": COLS}, id="columns"),
    pytest.param({"filters": {"vital_category": "heart_rate"}}, id="filter-scalar"),
    pytest.param({"filters": {"vital_category": ["heart_rate", "spo2"]}}, id="filter-list"),
    pytest.param({"sample_size": 25}, id="sample_size"),
    pytest.param(
        {"columns": COLS, "filters": {"vital_category": ["heart_rate", "spo2"]}, "sample_size": 25},
        id="all-three",
    ),
]


def _normalize(obj):
    """Any return type -> a sorted polars DataFrame, for comparison."""
    if isinstance(obj, pl.LazyFrame):
        obj = obj.collect()
    if isinstance(obj, duckdb.DuckDBPyRelation):
        obj = obj.pl()
    if not isinstance(obj, pl.DataFrame):
        obj = pl.from_pandas(obj)
    return obj.sort(obj.columns)


@pytest.mark.parametrize("kwargs", MATRIX)
@pytest.mark.parametrize("fmt", ["polars", "polars_lazy"])
def test_polars_backend_matches_duckdb(demo_dir, fmt, kwargs):
    """Same arguments, same rows and columns, whichever backend served them."""
    polars_side = _normalize(
        load_data("vitals", demo_dir, "parquet", return_format=fmt, **kwargs)
    )
    duckdb_side = _normalize(
        load_data("vitals", demo_dir, "parquet", return_format="duckdb", **kwargs)
    )

    assert polars_side.columns == duckdb_side.columns
    assert polars_side.height == duckdb_side.height
    # sample_size has no ORDER BY, so which rows come back is not guaranteed to match;
    # for the deterministic cases compare the data itself.
    if "sample_size" not in kwargs:
        assert polars_side.equals(duckdb_side)


@pytest.mark.parametrize("kwargs", MATRIX)
def test_csv_backend_parity(demo_csv, kwargs):
    """The CSV path too — this is where the historical divergence actually happened."""
    polars_side = _normalize(load_data("vitals", demo_csv, "csv", return_format="polars", **kwargs))
    duckdb_side = _normalize(load_data("vitals", demo_csv, "csv", return_format="duckdb", **kwargs))
    assert polars_side.columns == duckdb_side.columns
    assert polars_side.height == duckdb_side.height


@pytest.mark.tz_conversion
def test_csv_scan_parses_dates(demo_csv):
    """`pl.scan_csv` needs try_parse_dates=True; without it *_dttm stays String.

    This is the precise bug the old io_polars.load_csv_polars shipped with, and the one
    a native polars CSV path can re-introduce. Assert it directly.
    """
    dtype = load_data("vitals", demo_csv, "csv", return_format="polars").schema["recorded_dttm"]
    assert isinstance(dtype, pl.Datetime), f"expected Datetime, got {dtype}"
    assert dtype.time_zone is not None


def test_native_scan_is_used_by_default(demo_dir):
    """polars formats scan the file directly, so later filters can push down."""
    plan = load_data("vitals", demo_dir, "parquet", return_format="polars_lazy").explain()
    assert "Parquet SCAN" in plan
    assert "PYTHON SCAN" not in plan


def test_duckdb_con_rejected_for_polars_formats(demo_dir):
    """duckdb_con only makes sense for the relation format.

    The materialized formats relabel unconditionally, so the connection cannot affect
    the result -- accepting it would silently do nothing while forcing a slower path.
    """
    con = new_duckdb_con()
    for fmt in ("polars", "polars_lazy", "pandas"):
        with pytest.raises(ValueError, match="only meaningful with return_format='duckdb'"):
            load_data("vitals", demo_dir, "parquet", return_format=fmt, duckdb_con=con)
    con.close()
