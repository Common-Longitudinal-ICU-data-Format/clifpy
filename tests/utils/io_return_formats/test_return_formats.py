"""The four ``return_format`` values: types, equivalence, and the CSV path.

The load-bearing test here is ``test_instants_identical_across_formats`` -- whatever
else differs between the formats, they must describe the same moments in time.
"""

import duckdb
import pandas as pd
import polars as pl
import pytest

from clifpy.utils.io import load_data
from tests.utils.io_return_formats.conftest import (
    FORMATS, ZONES, dttm_dtype_tz, dttm_utc_instants,
)

EXPECTED_TYPE = {
    "polars": pl.DataFrame,
    "polars_lazy": pl.LazyFrame,
    "duckdb": duckdb.DuckDBPyRelation,
    "pandas": pd.DataFrame,
}


@pytest.fixture
def load(demo_dir):
    def _load(**kwargs):
        kwargs.setdefault("sample_size", 200)
        return load_data("vitals", demo_dir, "parquet", **kwargs)
    return _load


@pytest.mark.parametrize("fmt", FORMATS)
def test_each_format_returns_its_type(load, fmt):
    """Each return_format yields exactly its documented type."""
    assert isinstance(load(return_format=fmt), EXPECTED_TYPE[fmt])


def test_default_is_polars_dataframe(load):
    """The no-argument default is a polars DataFrame (changed from pandas in 0.6.0)."""
    assert isinstance(load(), pl.DataFrame)


def test_unknown_format_raises(load):
    """An unrecognised format fails loudly and names the valid options."""
    with pytest.raises(ValueError, match="Unknown return_format"):
        load(return_format="pola")


@pytest.mark.tz_conversion
@pytest.mark.parametrize("site_tz", ZONES)
def test_instants_identical_across_formats(load, site_tz, hostile_default_tz):
    """All four formats describe the same instants, under a hostile default zone.

    Per docs/tz_dx.md, assert instant preservation rather than wall-clock hours.
    """
    instants = {f: dttm_utc_instants(load(site_tz=site_tz, return_format=f)) for f in FORMATS}
    baseline = instants["pandas"]
    for fmt, got in instants.items():
        assert got == baseline, f"{fmt} instants diverge from pandas"


@pytest.mark.tz_conversion
@pytest.mark.parametrize("site_tz", ZONES)
def test_site_tz_label_applied_to_materialized_formats(load, site_tz, hostile_default_tz):
    """site_tz labels every format except 'duckdb', which stays aware-UTC."""
    for fmt in ("polars", "polars_lazy", "pandas"):
        assert dttm_dtype_tz(load(site_tz=site_tz, return_format=fmt)) == site_tz

    # A relation has nowhere to carry a label -- see docs/io_return_formats_dx.md 4.4.
    assert dttm_dtype_tz(load(site_tz=site_tz, return_format="duckdb")) == "UTC"


@pytest.mark.parametrize("fmt", FORMATS)
def test_columns_and_filters_agree_across_formats(demo_dir, fmt):
    """columns=/filters=/sample_size= select the same data whatever the format."""
    cols = ["hospitalization_id", "recorded_dttm", "vital_category", "vital_value"]
    kwargs = dict(columns=cols, filters={"vital_category": "heart_rate"}, sample_size=50)

    got = load_data("vitals", demo_dir, "parquet", return_format=fmt, **kwargs)
    ref = load_data("vitals", demo_dir, "parquet", return_format="pandas", **kwargs)

    if isinstance(got, pl.LazyFrame):
        got = got.collect()
    if isinstance(got, duckdb.DuckDBPyRelation):
        got = got.df()
    assert list(got.columns) == cols
    assert len(got) == len(ref)


@pytest.mark.parametrize("fmt", ["polars", "polars_lazy", "pandas"])
def test_id_columns_are_strings(load, fmt):
    """ID columns load as strings -- CLIF treats every *_id as a string."""
    df = load(return_format=fmt)
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    dtype = df["hospitalization_id"].dtype if isinstance(df, pl.DataFrame) \
        else df["hospitalization_id"].dtype
    assert str(dtype) in ("String", "Utf8", "string")


@pytest.mark.parametrize("fmt", ["polars", "pandas"])
def test_float_ids_lose_their_decimal(float_id_parquet, fmt):
    """A Float64 id becomes "123456", not "123456.0"."""
    directory, table = float_id_parquet
    df = load_data(table, directory, "parquet", return_format=fmt)
    values = df["hospitalization_id"].to_list() if isinstance(df, pl.DataFrame) \
        else list(df["hospitalization_id"])
    assert sorted(values) == ["123456", "234567"]


def test_polars_lazy_is_not_collected(load):
    """polars_lazy defers: it is a LazyFrame, and collects twice without complaint."""
    lf = load(return_format="polars_lazy")
    assert isinstance(lf, pl.LazyFrame)
    first, second = lf.collect(), lf.collect()
    assert first.equals(second)
    assert first.equals(load(return_format="polars"))


@pytest.mark.parametrize("fmt", FORMATS)
def test_csv_supports_every_format(demo_csv, fmt):
    """CSV serves all four formats.

    Previously return_rel=True on a CSV silently downgraded to a DataFrame; that
    downgrade is gone, since a silent type change is worse now the default is polars.
    """
    assert isinstance(load_data("vitals", demo_csv, "csv", return_format=fmt),
                      EXPECTED_TYPE[fmt])


@pytest.mark.tz_conversion
def test_csv_dttm_is_parsed_not_string(demo_csv):
    """CSV ``*_dttm`` comes back tz-aware Datetime, not String.

    io_polars.load_csv_polars used pl.scan_csv without try_parse_dates=True, so these
    columns arrived as String and were silently skipped by the tz converter. Routing
    CSV through DuckDB's reader fixes it.
    """
    df = load_data("vitals", demo_csv, "csv", return_format="polars", sample_size=20)
    dtype = df.schema["recorded_dttm"]
    assert isinstance(dtype, pl.Datetime), f"expected Datetime, got {dtype}"
    assert dtype.time_zone is not None
