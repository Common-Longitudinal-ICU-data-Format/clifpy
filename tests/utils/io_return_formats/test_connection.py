"""``duckdb_con``: isolation, non-mutation, and why it has to be opt-in."""

import warnings

import duckdb
import polars as pl
import pytest

from clifpy.utils.io import load_data, new_duckdb_con
from tests.utils.io_return_formats.conftest import dttm_dtype_tz


@pytest.fixture
def shared_con():
    con = new_duckdb_con()
    yield con
    con.close()


def test_relations_cannot_cross_connections(demo_dir):
    """The constraint the whole connection design rests on.

    Two relations built on different connections cannot be joined. This is why
    duckdb_con is opt-in rather than the default: SOFA2 loads 8 tables and joins them
    on one connection.
    """
    c1, c2 = new_duckdb_con(), new_duckdb_con()
    a = load_data("vitals", demo_dir, "parquet", sample_size=10,
                  return_format="duckdb", duckdb_con=c1)
    b = load_data("labs", demo_dir, "parquet", sample_size=10,
                  return_format="duckdb", duckdb_con=c2)
    with pytest.raises(duckdb.InvalidInputException, match="another Connection"):
        c1.sql("FROM a JOIN b USING (hospitalization_id) SELECT 1").fetchall()
    c1.close(); c2.close()


def test_relations_on_a_shared_connection_join(demo_dir, shared_con):
    """Passing one connection to every load keeps the relations composable."""
    vitals = load_data("vitals", demo_dir, "parquet", sample_size=50,
                       return_format="duckdb", duckdb_con=shared_con)
    labs = load_data("labs", demo_dir, "parquet", sample_size=50,
                     return_format="duckdb", duckdb_con=shared_con)
    n = shared_con.sql(
        "FROM vitals JOIN labs USING (hospitalization_id) SELECT count(*) AS n"
    ).fetchone()[0]
    assert isinstance(n, int)


@pytest.mark.tz_conversion
def test_caller_connection_is_never_mutated(demo_dir, io_log):
    """A foreign non-UTC connection keeps its setting, and the caller is warned."""
    foreign = duckdb.connect()
    foreign.execute("SET timezone='Asia/Tokyo';")

    load_data("vitals", demo_dir, "parquet", sample_size=10,
              return_format="duckdb", duckdb_con=foreign)

    assert foreign.sql("SELECT current_setting('TimeZone')").fetchone()[0] == "Asia/Tokyo"
    assert any("not 'UTC'" in m for m in io_log)
    foreign.close()


@pytest.mark.tz_conversion
def test_new_duckdb_con_does_not_warn(demo_dir, io_log):
    """A zone chosen deliberately via new_duckdb_con() is not warned about.

    Warning here would fire on the one sanctioned way to get a labelled relation.
    """
    con = new_duckdb_con(site_tz="US/Eastern")
    load_data("vitals", demo_dir, "parquet", sample_size=10,
              return_format="duckdb", duckdb_con=con)
    assert not any("not 'UTC'" in m for m in io_log)
    con.close()


@pytest.mark.tz_conversion
def test_duckdb_con_gives_a_site_tz_labelled_relation(demo_dir):
    """The gap duckdb_con closes: a relation labelled in a chosen zone."""
    con = new_duckdb_con(site_tz="US/Eastern")
    rel = load_data("vitals", demo_dir, "parquet", sample_size=10,
                    return_format="duckdb", duckdb_con=con)
    assert str(rel.df()["recorded_dttm"].dt.tz) == "US/Eastern"

    duckdb.execute("SET timezone='Asia/Tokyo';")          # global re-pin
    assert str(rel.df()["recorded_dttm"].dt.tz) == "US/Eastern"
    duckdb.execute("SET timezone='UTC';")
    con.close()


@pytest.mark.tz_conversion
@pytest.mark.parametrize("fmt", ["polars", "polars_lazy", "pandas"])
def test_materialized_formats_ignore_the_ambient_connection_zone(demo_dir, fmt, hostile_default_tz):
    """Materialized formats are unaffected by the default connection's zone.

    This is what the unconditional `site_tz or 'UTC'` relabel buys, and it is also why
    duckdb_con is rejected for these formats: there is nothing a connection could change.
    """
    got = load_data("vitals", demo_dir, "parquet", sample_size=50, return_format=fmt)
    assert dttm_dtype_tz(got) == "UTC"


def test_duckdb_con_is_only_valid_for_the_relation_format(demo_dir, shared_con):
    """'duckdb' accepts it; every other format raises rather than ignoring it."""
    assert load_data("vitals", demo_dir, "parquet", sample_size=10,
                     return_format="duckdb", duckdb_con=shared_con) is not None

    for fmt in ("polars", "polars_lazy", "pandas"):
        with pytest.raises(ValueError, match="only meaningful"):
            load_data("vitals", demo_dir, "parquet", sample_size=10,
                      return_format=fmt, duckdb_con=shared_con)


@pytest.mark.tz_conversion
def test_site_tz_with_duckdb_warns_instead_of_being_ignored(demo_dir):
    """site_tz cannot apply to a relation, so say so rather than dropping it silently.

    A relation is an unevaluated plan; its label comes from whichever connection
    materializes it. Passing site_tz here is a no-op, and a silent no-op on a timezone
    argument is exactly the kind of thing that produces quietly wrong analyses.
    """
    with pytest.warns(UserWarning, match="site_tz is ignored"):
        rel = load_data("vitals", demo_dir, "parquet", sample_size=10,
                        site_tz="US/Eastern", return_format="duckdb")
    assert str(rel.df()["recorded_dttm"].dt.tz) == "UTC"


@pytest.mark.tz_conversion
def test_no_warning_when_site_tz_comes_from_config(demo_dir, tmp_path, monkeypatch):
    """An ambient config timezone is not worth warning about on every relation load."""
    (tmp_path / "config.yaml").write_text(
        f'data_directory: "{demo_dir}"\nfiletype: "parquet"\ntimezone: "US/Eastern"\n'
    )
    monkeypatch.chdir(tmp_path)
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        load_data("vitals", config_path=str(tmp_path / "config.yaml"),
                  sample_size=10, return_format="duckdb")
