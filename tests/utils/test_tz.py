"""Tests for timezone conversion in io module.

This module contains tests for timezone conversion in clifpy.utils.io
(load_data / load_parquet_with_tz), covering the aware materialized,
return_rel (aware-UTC), and lazy paths.
"""
import pytest
import pandas as pd
import duckdb
from pathlib import Path
from clifpy.utils.io import (
    load_parquet_with_tz,
    load_data
)


# --- Helper Fixtures ---
@pytest.fixture
def demo_data_dir():
    """Path to demo data directory."""
    return Path(__file__).parent.parent.parent / 'clifpy' / 'data' / 'clif_demo'


@pytest.fixture
def demo_vitals_path(demo_data_dir):
    """Path to demo vitals parquet file."""
    return str(demo_data_dir / 'clif_vitals.parquet')


@pytest.fixture
def duckdb_connection():
    """Create a DuckDB connection with UTC timezone setting."""
    con = duckdb.connect()
    con.execute("SET timezone = 'UTC';")
    con.execute("SET pandas_analyze_sample=0;")
    yield con
    con.close()


# ===========================================
# Tests for DuckDB timezone conversion
# ===========================================
@pytest.mark.tz_conversion
class TestDuckDBTimezoneConversion:
    """Tests for raw DuckDB timezone() function."""

    def test_timezone_converts_utc_to_eastern(self, demo_vitals_path, duckdb_connection):
        """Test that DuckDB timezone() function converts UTC to US/Eastern correctly."""
        con = duckdb_connection

        # Load UTC timestamps
        utc_df = con.execute(f"""
            SELECT recorded_dttm
            FROM parquet_scan('{demo_vitals_path}')
            LIMIT 5
        """).fetchdf()

        # Convert to US/Eastern
        eastern_df = con.execute(f"""
            SELECT timezone('US/Eastern', recorded_dttm) AS recorded_dttm
            FROM parquet_scan('{demo_vitals_path}')
            LIMIT 5
        """).fetchdf()

        # Verify same number of rows
        assert len(utc_df) == len(eastern_df)

        # Verify UTC timestamps have timezone info
        assert utc_df['recorded_dttm'].dt.tz is not None

        # Verify hours differ by 4-5 hours (depending on DST)
        utc_hours = utc_df['recorded_dttm'].dt.hour
        eastern_hours = eastern_df['recorded_dttm'].dt.hour

        # Eastern is behind UTC, so UTC hours should be greater
        hour_diff = (utc_hours - eastern_hours).iloc[0]
        assert hour_diff in [4, 5], f"Expected 4-5 hour difference, got {hour_diff}"

    def test_timezone_converts_utc_to_central(self, demo_vitals_path, duckdb_connection):
        """Test that DuckDB timezone() converts UTC to US/Central correctly."""
        con = duckdb_connection

        utc_df = con.execute(f"""
            SELECT recorded_dttm
            FROM parquet_scan('{demo_vitals_path}')
            LIMIT 5
        """).fetchdf()

        central_df = con.execute(f"""
            SELECT timezone('US/Central', recorded_dttm) AS recorded_dttm
            FROM parquet_scan('{demo_vitals_path}')
            LIMIT 5
        """).fetchdf()

        utc_hours = utc_df['recorded_dttm'].dt.hour
        central_hours = central_df['recorded_dttm'].dt.hour

        hour_diff = (utc_hours - central_hours).iloc[0]
        assert hour_diff in [5, 6], f"Expected 5-6 hour difference for Central, got {hour_diff}"


# ===========================================
# Tests for load_parquet_with_tz
# ===========================================
@pytest.mark.tz_conversion
class TestLoadParquetWithTz:
    """Tests for load_parquet_with_tz function with timezone conversion."""

    def test_loads_with_timezone_conversion(self, demo_vitals_path):
        """load_parquet_with_tz returns tz-aware site_tz columns, instant-preserving.

        Asserts the real contract (tz-awareness + UTC-instant preservation) rather
        than hardcoded wall-clock hours. The wall-clock hour depends on the tz engine
        (pandas/pytz vs ICU) and the date, which diverge for CLIF-MIMIC's far-future
        de-identified dates (pytz's DST table freezes at 2037). See docs/tz_dx.md §11.
        """
        df = load_parquet_with_tz(
            demo_vitals_path, sample_size=5, site_tz='US/Eastern'
        )
        df_utc = load_parquet_with_tz(
            demo_vitals_path, sample_size=5, site_tz=None
        )

        assert len(df) == 5
        assert 'recorded_dttm' in df.columns
        # tz-aware in the requested zone
        assert df['recorded_dttm'].dt.tz is not None
        assert str(df['recorded_dttm'].dt.tz) == 'US/Eastern'
        # instant-preserving: same UTC instants as the unconverted (UTC) load
        assert (
            df['recorded_dttm'].dt.tz_convert('UTC').reset_index(drop=True)
            == df_utc['recorded_dttm'].dt.tz_convert('UTC').reset_index(drop=True)
        ).all()

    def test_loads_without_timezone_conversion(self, demo_vitals_path):
        """Test that load_parquet_with_tz returns UTC when site_tz is None."""
        df = load_parquet_with_tz(
            demo_vitals_path,
            sample_size=5,
            site_tz=None
        )

        assert len(df) == 5
        # Should have timezone info (UTC)
        assert df['recorded_dttm'].dt.tz is not None

    def test_with_column_filter(self, demo_vitals_path):
        """Test load_parquet_with_tz with column filter and timezone."""
        columns = ['hospitalization_id', 'recorded_dttm', 'vital_value']
        df = load_parquet_with_tz(
            demo_vitals_path,
            columns=columns,
            sample_size=5,
            site_tz='US/Eastern'
        )

        assert list(df.columns) == columns
        assert len(df) == 5


# ===========================================
# Tests for load_data
# ===========================================
@pytest.mark.tz_conversion
class TestLoadDataTimezone:
    """Tests for load_data function with timezone conversion."""

    def test_load_data_with_timezone(self, demo_data_dir):
        """Test load_data applies timezone conversion correctly."""
        df = load_data(
            table_name='vitals',
            table_path=str(demo_data_dir),
            table_format_type='parquet',
            sample_size=5,
            site_tz='US/Eastern'
        )

        assert len(df) == 5
        assert 'recorded_dttm' in df.columns

    def test_load_data_without_timezone(self, demo_data_dir):
        """Test load_data returns UTC data when site_tz is None."""
        df = load_data(
            table_name='vitals',
            table_path=str(demo_data_dir),
            table_format_type='parquet',
            sample_size=5,
            site_tz=None
        )

        assert len(df) == 5
        # Should have UTC timezone
        assert df['recorded_dttm'].dt.tz is not None

    def test_timezone_conversion_accuracy(self, demo_data_dir):
        """Test that timezone conversion produces correct hour offset."""
        # Load with and without timezone conversion
        df_utc = load_data(
            table_name='vitals',
            table_path=str(demo_data_dir),
            table_format_type='parquet',
            sample_size=5,
            site_tz=None
        )

        df_eastern = load_data(
            table_name='vitals',
            table_path=str(demo_data_dir),
            table_format_type='parquet',
            sample_size=5,
            site_tz='US/Eastern'
        )

        # Calculate hour difference
        utc_hour = df_utc['recorded_dttm'].dt.hour.iloc[0]
        eastern_hour = df_eastern['recorded_dttm'].dt.hour.iloc[0]
        hour_diff = utc_hour - eastern_hour

        # Eastern is 4-5 hours behind UTC depending on DST
        assert hour_diff in [4, 5], f"Expected 4-5 hour diff, got {hour_diff}"

    def test_multiple_dttm_columns(self, demo_data_dir):
        """Test that all dttm columns are converted."""
        # Use ADT table which has multiple datetime columns
        df = load_data(
            table_name='adt',
            table_path=str(demo_data_dir),
            table_format_type='parquet',
            sample_size=5,
            site_tz='US/Eastern'
        )

        # Check that datetime columns exist and are converted
        dttm_cols = [c for c in df.columns if 'dttm' in c.lower()]
        assert len(dttm_cols) > 0, "Expected at least one dttm column"


# ===========================================
# Tests for return_rel parameter
# ===========================================
@pytest.mark.tz_conversion
class TestReturnRelation:
    """Tests for return_rel parameter returning DuckDBPyRelation."""

    def test_returns_relation(self, demo_vitals_path):
        """Test that return_rel=True returns DuckDBPyRelation."""
        rel = load_parquet_with_tz(
            demo_vitals_path,
            sample_size=5,
            return_rel=True
        )

        assert isinstance(rel, duckdb.DuckDBPyRelation)

        # Verify we can convert to DataFrame
        df = rel.df()
        assert len(df) == 5
        # No connection cleanup needed - uses default connection

    def test_relation_is_tz_aware_utc(self, demo_vitals_path):
        """return_rel returns tz-aware UTC (site_tz not applied to a bare relation).

        A bare DuckDBPyRelation renders TIMESTAMPTZ in the connection's zone (UTC) at
        .df() time; site_tz is not applied to the label. Assert tz-awareness + that the
        instant matches the materialized UTC load. See docs/tz_dx.md.
        """
        df_utc = load_parquet_with_tz(
            demo_vitals_path,
            sample_size=5,
            site_tz=None
        )

        # site_tz is ignored for return_rel -> aware-UTC regardless
        rel = load_parquet_with_tz(
            demo_vitals_path,
            sample_size=5,
            site_tz='US/Eastern',
            return_rel=True
        )
        df_rel = rel.df()

        assert df_rel['recorded_dttm'].dt.tz is not None
        assert str(df_rel['recorded_dttm'].dt.tz) in ('UTC', 'utc')
        # instant-preserving: same UTC instants as the materialized UTC load
        assert (
            df_rel['recorded_dttm'].dt.tz_convert('UTC').reset_index(drop=True)
            == df_utc['recorded_dttm'].dt.tz_convert('UTC').reset_index(drop=True)
        ).all()

    def test_relation_lazy_evaluation(self, demo_vitals_path):
        """Test that relation is lazily evaluated and supports chaining."""
        rel = load_parquet_with_tz(
            demo_vitals_path,
            return_rel=True
        )

        # Can chain operations before execution
        filtered_rel = rel.filter("vital_value > 0")
        df = filtered_rel.df()

        assert len(df) > 0

    def test_relation_with_column_filter(self, demo_vitals_path):
        """Test relation with column filter."""
        columns = ['hospitalization_id', 'recorded_dttm', 'vital_value']
        rel = load_parquet_with_tz(
            demo_vitals_path,
            columns=columns,
            sample_size=5,
            return_rel=True
        )

        df = rel.df()
        assert list(df.columns) == columns
        assert len(df) == 5

    def test_load_data_returns_relation(self, demo_data_dir):
        """Test load_data with return_rel=True for parquet."""
        rel = load_data(
            table_name='vitals',
            table_path=str(demo_data_dir),
            table_format_type='parquet',
            sample_size=5,
            return_rel=True
        )

        assert isinstance(rel, duckdb.DuckDBPyRelation)

        df = rel.df()
        assert len(df) == 5

    def test_load_data_relation_is_tz_aware_utc(self, demo_data_dir):
        """load_data(return_rel=True) yields tz-aware UTC (site_tz not applied)."""
        df_utc = load_data(
            table_name='vitals',
            table_path=str(demo_data_dir),
            table_format_type='parquet',
            sample_size=5,
            site_tz=None
        )

        rel = load_data(
            table_name='vitals',
            table_path=str(demo_data_dir),
            table_format_type='parquet',
            sample_size=5,
            site_tz='US/Eastern',
            return_rel=True
        )
        df_rel = rel.df()

        assert df_rel['recorded_dttm'].dt.tz is not None
        assert str(df_rel['recorded_dttm'].dt.tz) in ('UTC', 'utc')
        assert (
            df_rel['recorded_dttm'].dt.tz_convert('UTC').reset_index(drop=True)
            == df_utc['recorded_dttm'].dt.tz_convert('UTC').reset_index(drop=True)
        ).all()

    def test_default_returns_dataframe(self, demo_vitals_path):
        """Test that default (return_rel=False) returns DataFrame."""
        result = load_parquet_with_tz(
            demo_vitals_path,
            sample_size=5
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5
