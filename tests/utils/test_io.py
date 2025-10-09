import pytest
import pandas as pd
import os
import pytz
from datetime import datetime
from unittest.mock import patch, MagicMock
from clifpy.utils.io import (
    _cast_id_cols_to_string,
    load_parquet_with_tz,
    load_data,
    convert_datetime_columns_to_site_tz
)


class TestCastIdColsToString:
    def test_cast_id_cols_with_id_columns(self):
        """Test casting ID columns to string when ID columns exist."""
        df = pd.DataFrame({
            "patient_id": [1, 2, 3],
            "encounter_id": [100, 200, 300],
            "value": [10.5, 20.5, 30.5]
        })
        
        result = _cast_id_cols_to_string(df)
        
        assert result["patient_id"].dtype == "string"
        assert result["encounter_id"].dtype == "string"
        assert result["value"].dtype == "float64"
        assert result["patient_id"].tolist() == ["1", "2", "3"]
        assert result["encounter_id"].tolist() == ["100", "200", "300"]

    def test_cast_id_cols_without_id_columns(self):
        """Test that function is a no-op when no ID columns exist."""
        df = pd.DataFrame({
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.5, 30.5]
        })
        
        result = _cast_id_cols_to_string(df)
        
        # Should be unchanged
        assert result["name"].dtype == "object"
        assert result["value"].dtype == "float64"
        assert id(result) == id(df)  # Should return the same DataFrame object


class TestLoadParquetWithTz:
    @patch('duckdb.connect')
    def test_load_parquet_basic(self, mock_connect):
        """Test basic loading of parquet file."""
        # Setup mock
        mock_con = MagicMock()
        mock_connect.return_value = mock_con
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_con.execute().fetchdf.return_value = mock_df
        
        # Call function
        result = load_parquet_with_tz("test.parquet")
        
        # Verify calls
        mock_connect.assert_called_once()
        mock_con.execute.assert_any_call("SET timezone = 'UTC';")
        mock_con.execute.assert_any_call("SET pandas_analyze_sample=0;")
        mock_con.execute.assert_any_call("SELECT * FROM parquet_scan('test.parquet')")
        mock_con.close.assert_called_once()
        
        # Verify result
        pd.testing.assert_frame_equal(result, mock_df)

    @patch('duckdb.connect')
    def test_load_parquet_with_columns(self, mock_connect):
        """Test loading specific columns from parquet file."""
        # Setup mock
        mock_con = MagicMock()
        mock_connect.return_value = mock_con
        mock_df = pd.DataFrame({"col1": [1, 2]})
        mock_con.execute().fetchdf.return_value = mock_df
        
        # Call function
        result = load_parquet_with_tz("test.parquet", columns=["col1"])
        
        # Verify calls
        mock_con.execute.assert_any_call("SELECT col1 FROM parquet_scan('test.parquet')")
        
        # Verify result
        pd.testing.assert_frame_equal(result, mock_df)

    @patch('duckdb.connect')
    def test_load_parquet_with_filters(self, mock_connect):
        """Test loading parquet file with filters."""
        # Setup mock
        mock_con = MagicMock()
        mock_connect.return_value = mock_con
        mock_df = pd.DataFrame({"col1": [1], "col2": ["a"]})
        mock_con.execute().fetchdf.return_value = mock_df
        
        # Call function
        result = load_parquet_with_tz(
            "test.parquet", 
            filters={"col1": 1, "col2": ["a", "b"]}
        )
        
        # Verify calls
        mock_con.execute.assert_any_call(
            "SELECT * FROM parquet_scan('test.parquet') WHERE col1 = '1' AND col2 IN ('a', 'b')"
        )
        
        # Verify result
        pd.testing.assert_frame_equal(result, mock_df)

    @patch('duckdb.connect')
    def test_load_parquet_with_sample_size(self, mock_connect):
        """Test loading parquet file with sample size limit."""
        # Setup mock
        mock_con = MagicMock()
        mock_connect.return_value = mock_con
        mock_df = pd.DataFrame({"col1": [1], "col2": ["a"]})
        mock_con.execute().fetchdf.return_value = mock_df
        
        # Call function
        result = load_parquet_with_tz("test.parquet", sample_size=100)
        
        # Verify calls
        mock_con.execute.assert_any_call(
            "SELECT * FROM parquet_scan('test.parquet') LIMIT 100"
        )
        
        # Verify result
        pd.testing.assert_frame_equal(result, mock_df)


class TestLoadData:
    @patch('os.path.exists')
    @patch('duckdb.connect')
    def test_load_data_csv(self, mock_connect, mock_exists):
        """Test loading CSV data."""
        # Setup mocks
        mock_exists.return_value = True
        mock_con = MagicMock()
        mock_connect.return_value = mock_con
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_con.execute().fetchdf.return_value = mock_df
        
        # Call function
        result = load_data("test_table", "/path/to/dir", "csv")
        
        # Verify calls
        mock_exists.assert_called_with("/path/to/dir/clif_test_table.csv")
        mock_connect.assert_called_once()
        mock_con.execute.assert_called_with("SELECT * FROM read_csv_auto('/path/to/dir/clif_test_table.csv')")
        mock_con.close.assert_called_once()
        
        # Verify result
        pd.testing.assert_frame_equal(result, mock_df)

    @patch('os.path.exists')
    @patch('clifpy.utils.io.load_parquet_with_tz')
    def test_load_data_parquet(self, mock_load_parquet, mock_exists):
        """Test loading parquet data."""
        # Setup mocks
        mock_exists.return_value = True
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_load_parquet.return_value = mock_df
        
        # Call function
        result = load_data("test_table", "/path/to/dir", "parquet")
        
        # Verify calls
        mock_exists.assert_called_with("/path/to/dir/clif_test_table.parquet")
        mock_load_parquet.assert_called_with(
            "/path/to/dir/clif_test_table.parquet", None, None, None
        )
        
        # Verify result
        pd.testing.assert_frame_equal(result, mock_df)

    @patch('os.path.exists')
    def test_load_data_unsupported_format(self, mock_exists):
        """Test loading data with unsupported format."""
        mock_exists.return_value = True
        
        with pytest.raises(ValueError, match="Unsupported filetype"):
            load_data("test_table", "/path/to/dir", "xlsx")

    @patch('os.path.exists')
    def test_load_data_file_not_found(self, mock_exists):
        """Test loading data when file doesn't exist."""
        mock_exists.return_value = False
        
        with pytest.raises(FileNotFoundError):
            load_data("test_table", "/path/to/dir", "csv")

    @patch('os.path.exists')
    @patch('duckdb.connect')
    def test_load_data_with_filters_and_columns(self, mock_connect, mock_exists):
        """Test loading CSV data with filters and columns."""
        # Setup mocks
        mock_exists.return_value = True
        mock_con = MagicMock()
        mock_connect.return_value = mock_con
        mock_df = pd.DataFrame({"col1": [1]})
        mock_con.execute().fetchdf.return_value = mock_df
        
        # Call function
        result = load_data(
            "test_table", 
            "/path/to/dir", 
            "csv",
            columns=["col1"],
            filters={"status": ["active", "pending"], "type": "urgent"},
            sample_size=10
        )
        
        # Verify calls
        expected_query = (
            "SELECT col1 FROM read_csv_auto('/path/to/dir/clif_test_table.csv') "
            "WHERE status IN ('active', 'pending') AND type = 'urgent' LIMIT 10"
        )
        mock_con.execute.assert_called_with(expected_query)
        
        # Verify result
        pd.testing.assert_frame_equal(result, mock_df)


class TestConvertDatetimeColumnsToSiteTz:
    def test_convert_timezone_aware_columns(self):
        """Test converting timezone-aware datetime columns."""
        # Create test dataframe with timezone-aware columns
        utc_dt = pd.DatetimeIndex([
            '2023-01-01 12:00:00', 
            '2023-01-02 12:00:00'
        ]).tz_localize('UTC')
        
        df = pd.DataFrame({
            'event_dttm': utc_dt,
            'start_dttm': utc_dt,
            'value': [1, 2]
        })
        
        # Convert to US/Central
        result = convert_datetime_columns_to_site_tz(df, 'US/Central', verbose=False)
        
        # Check that timezone was converted
        assert result['event_dttm'].dt.tz.zone == 'US/Central'
        assert result['start_dttm'].dt.tz.zone == 'US/Central'
        
        # Check that the time values were adjusted
        central_offset = pytz.timezone('US/Central').utcoffset(datetime(2023, 1, 1))
        utc_offset = pytz.UTC.utcoffset(datetime(2023, 1, 1))
        hour_diff = (central_offset - utc_offset).total_seconds() / 3600
        
        # In US/Central, times should be 6 hours earlier than UTC in winter
        assert result['event_dttm'].dt.hour.tolist() == [int(12 + hour_diff)] * 2
        assert result['start_dttm'].dt.hour.tolist() == [int(12 + hour_diff)] * 2

    def test_convert_naive_datetime_columns(self):
        """Test handling naive datetime columns."""
        # Create test dataframe with naive datetime columns
        naive_dt = pd.DatetimeIndex(['2023-01-01 12:00:00', '2023-01-02 12:00:00'])
        
        df = pd.DataFrame({
            'event_dttm': naive_dt,
            'value': [1, 2]
        })
        
        # Convert with verbose=False to avoid printing warnings
        result = convert_datetime_columns_to_site_tz(df, 'US/Central', verbose=False)
        
        # Check that timezone was localized
        assert result['event_dttm'].dt.tz.zone == 'US/Central'
        
        # Time values should remain the same as they're assumed to be in the local timezone
        assert result['event_dttm'].dt.hour.tolist() == [12, 12]

    def test_non_datetime_columns_with_dttm_in_name(self):
        """Test handling columns with 'dttm' in name but not datetime type."""
        df = pd.DataFrame({
            'fake_dttm': ['not a date', 'still not a date'],
            'value': [1, 2]
        })
        
        # Should not raise errors
        result = convert_datetime_columns_to_site_tz(df, 'US/Central', verbose=False)
        
        # Column should be unchanged
        assert result['fake_dttm'].tolist() == ['not a date', 'still not a date']

    def test_mixed_timezone_columns(self):
        """Test handling mixed timezone columns."""
        # Create test dataframe with mixed timezone columns
        utc_dt = pd.DatetimeIndex([
            '2023-01-01 12:00:00', 
            '2023-01-02 12:00:00'
        ]).tz_localize('UTC')
        
        est_dt = pd.DatetimeIndex([
            '2023-01-01 12:00:00', 
            '2023-01-02 12:00:00'
        ]).tz_localize('US/Eastern')
        
        df = pd.DataFrame({
            'utc_dttm': utc_dt,
            'est_dttm': est_dt,
            'value': [1, 2]
        })
        
        # Convert to US/Central
        result = convert_datetime_columns_to_site_tz(df, 'US/Central', verbose=False)
        
        # Check that both columns now have US/Central timezone
        assert result['utc_dttm'].dt.tz.zone == 'US/Central'
        assert result['est_dttm'].dt.tz.zone == 'US/Central'
