"""Parity net for the deprecated ``clifpy.utils.io_polars`` shims.

That module had ZERO tests before this change, so these are the only guarantee that
folding it into io.py preserved its contract. Each shim keeps its exact signature,
argument order and defaults -- note load_clif_table_polars takes data_directory FIRST,
and every shim defaults lazy=True (the opposite of load_data).
"""

import polars as pl
import pytest

from clifpy.utils.io import load_data
from clifpy.utils.io_polars import (
    load_clif_table_polars, load_csv_polars, load_data_polars, load_parquet_polars,
)

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def _schema(obj):
    return obj.collect_schema() if isinstance(obj, pl.LazyFrame) else obj.schema


def test_all_shims_warn(demo_dir):
    for fn, args in [
        (load_data_polars, ("vitals", demo_dir, "parquet")),
        (load_parquet_polars, (f"{demo_dir}/clif_vitals.parquet",)),
        (load_clif_table_polars, (demo_dir, "vitals")),
    ]:
        with pytest.warns(DeprecationWarning, match="deprecated"):
            fn(*args)


@pytest.mark.parametrize("lazy,expected", [(True, pl.LazyFrame), (False, pl.DataFrame)])
def test_lazy_default_is_true(demo_dir, lazy, expected):
    """lazy defaults to True here, unlike load_data which defaults to eager polars."""
    assert isinstance(load_data_polars("vitals", demo_dir, "parquet", lazy=lazy), expected)
    assert isinstance(load_data_polars("vitals", demo_dir, "parquet"), pl.LazyFrame)


def test_shims_preserve_nanosecond_precision(demo_dir):
    """Shims keep Datetime('ns'); load_data keeps the source unit (us).

    join_asof requires exact dtype match, which is why this distinction matters.
    """
    shim = _schema(load_data_polars("vitals", demo_dir, "parquet"))["recorded_dttm"]
    native = load_data("vitals", demo_dir, "parquet", return_format="polars").schema["recorded_dttm"]
    assert shim.time_unit == "ns"
    assert native.time_unit == "us"


def test_load_clif_table_polars_argument_order(demo_dir):
    """data_directory comes FIRST, then table_name -- the reverse of load_data."""
    out = load_clif_table_polars(demo_dir, "vitals", "parquet", lazy=False)
    assert isinstance(out, pl.DataFrame) and out.height > 0


def test_hospitalization_ids_sugar_equals_filters(demo_dir):
    """hospitalization_ids=[...] maps to filters={'hospitalization_id': [...]}."""
    everything = load_clif_table_polars(demo_dir, "vitals", "parquet", lazy=False)
    ids = everything["hospitalization_id"].unique().head(3).to_list()

    sugar = load_clif_table_polars(demo_dir, "vitals", "parquet",
                                   hospitalization_ids=ids, lazy=False)
    explicit = load_data("vitals", demo_dir, "parquet",
                         filters={"hospitalization_id": ids},
                         return_format="polars", time_unit="ns")
    assert sugar.sort(sugar.columns).equals(explicit.sort(explicit.columns))


def test_columns_and_filters_pass_through(demo_dir):
    cols = ["hospitalization_id", "recorded_dttm", "vital_category"]
    out = load_parquet_polars(
        f"{demo_dir}/clif_vitals.parquet",
        columns=cols, filters={"vital_category": "heart_rate"},
        sample_size=25, lazy=False,
    )
    assert out.columns == cols
    assert out["vital_category"].unique().to_list() == ["heart_rate"]
    assert out.height <= 25


def test_csv_shim_now_parses_datetimes(demo_csv):
    """BEHAVIOUR FIX: pl.scan_csv without try_parse_dates left these as String."""
    dtype = _schema(load_csv_polars(f"{demo_csv}/clif_vitals.csv"))["recorded_dttm"]
    assert isinstance(dtype, pl.Datetime) and dtype.time_zone is not None


def test_site_tz_applies_through_the_shim(demo_dir):
    out = load_data_polars("vitals", demo_dir, "parquet",
                           site_tz="US/Eastern", lazy=False)
    assert out.schema["recorded_dttm"].time_zone == "US/Eastern"
