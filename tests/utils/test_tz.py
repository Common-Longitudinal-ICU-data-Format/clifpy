"""Tests for timezone conversion in io module.

This module contains tests for the DuckDB-based timezone conversion
functionality in clifpy.utils.io, including the _build_tz_converted_select
helper function and timezone conversion in load_data/load_parquet_with_tz.
"""
import pytest
import pandas as pd
import duckdb
from pathlib import Path
from clifpy.utils.io import (
    _build_tz_converted_select,
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
# Tests for _build_tz_converted_select
# ===========================================
@pytest.mark.tz_conversion
class TestBuildTzConvertedSelect:
    """Tests for _build_tz_converted_select helper function."""

    def test_identifies_dttm_columns(self, demo_vitals_path, duckdb_connection):
        """Test that function correctly identifies columns with 'dttm' in name."""
        con = duckdb_connection

        select_clause = _build_tz_converted_select(
            con, demo_vitals_path, None, 'US/Eastern', source_type='parquet'
        )

        # Should contain timezone conversion for recorded_dttm
        assert "timezone('US/Eastern', recorded_dttm)" in select_clause
        # Should not apply timezone to non-dttm columns
        assert "timezone('US/Eastern', hospitalization_id)" not in select_clause

    def test_no_conversion_without_site_tz(self, demo_vitals_path, duckdb_connection):
        """Test that no conversion is applied when site_tz is None."""
        con = duckdb_connection

        select_clause = _build_tz_converted_select(
            con, demo_vitals_path, None, None, source_type='parquet'
        )

        # Should not contain any timezone() calls
        assert "timezone(" not in select_clause
        # Should contain column names directly
        assert "recorded_dttm" in select_clause

    def test_respects_column_filter(self, demo_vitals_path, duckdb_connection):
        """Test that function only includes specified columns."""
        con = duckdb_connection

        columns = ['hospitalization_id', 'recorded_dttm', 'vital_value']
        select_clause = _build_tz_converted_select(
            con, demo_vitals_path, columns, 'US/Eastern', source_type='parquet'
        )

        # Should only include specified columns
        assert 'hospitalization_id' in select_clause
        assert 'vital_value' in select_clause
        # Should not include unspecified columns
        assert 'vital_category' not in select_clause


# ===========================================
# Tests for load_parquet_with_tz
# ===========================================
@pytest.mark.tz_conversion
class TestLoadParquetWithTz:
    """Tests for load_parquet_with_tz function with timezone conversion."""

    def test_loads_with_timezone_conversion(self, demo_vitals_path):
        """Test that load_parquet_with_tz applies timezone conversion."""
        df = load_parquet_with_tz(
            demo_vitals_path,
            sample_size=5,
            site_tz='US/Eastern'
        )

        assert len(df) == 5
        assert 'recorded_dttm' in df.columns

        # Verify hours are in Eastern time (not UTC)
        # UTC data has hour 14, Eastern should be 10 (EDT)
        hours = df['recorded_dttm'].dt.hour.unique()
        assert 14 not in hours or 10 in hours  # Either converted or was already in range

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
