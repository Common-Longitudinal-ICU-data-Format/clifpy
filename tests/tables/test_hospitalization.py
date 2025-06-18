"""
Tests for the hospitalization table module.
"""
import os
import pytest
import pandas as pd
from datetime import datetime
from pyclif.tables.hospitalization import hospitalization
# from pyclif.utils.validator import ValidationError # Not directly used in patient test, might not be needed if errors are checked via len(obj.errors)

# Fixtures for hospitalization testing
@pytest.fixture
def sample_valid_hospitalization_data():
    """Create a valid hospitalization DataFrame for testing."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002', 'H003', 'H004'],
        'patient_id': ['P001', 'P001', 'P002', 'P003'],
        'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-03-15 12:00:00', '2023-02-01 08:00:00', '2023-04-01 16:00:00']),
        'discharge_dttm': pd.to_datetime(['2023-01-10 17:00:00', '2023-03-20 14:00:00', '2023-02-05 18:00:00', '2023-04-03 11:00:00']),
        'age_at_admission': pd.Series([65, 65, 70, 50], dtype='int'),
        'admission_type_category': ['Emergency', 'Elective', 'Urgent', 'Emergency'],
        'discharge_category': ['Home', 'Expired', 'Acute Inpatient Rehab Facility', 'Home']
    })

@pytest.fixture
def sample_invalid_hospitalization_data():
    """Create an invalid hospitalization DataFrame for testing."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002', 'H003'],
        'patient_id': ['P001', 'P002', 'P003'],
        'admission_dttm': pd.to_datetime(['2023-01-01', '2023-02-02', '2023-03-03']),
        # Missing 'discharge_dttm'
        'age_at_admission': [65, 'invalid_age', 70],  # Invalid data type for age
        'admission_type_category': ['Emergency', 'Elective', 'INVALID_TYPE'], # Invalid category
        'discharge_category': ['Home', 'Expired', 'Rehab']
    })

@pytest.fixture
def mock_hospitalization_file(tmp_path, sample_valid_hospitalization_data):
    """Create a mock hospitalization file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_hospitalization.parquet"
    sample_valid_hospitalization_data.to_parquet(file_path)
    return str(test_dir)

# Tests for hospitalization class initialization
def test_hospitalization_init_with_valid_data(sample_valid_hospitalization_data):
    """Test hospitalization initialization with valid data."""
    hosp_obj = hospitalization(sample_valid_hospitalization_data)
    assert hosp_obj.df is not None
    assert hosp_obj.isvalid() is True
    assert len(hosp_obj.errors) == 0

def test_hospitalization_init_with_invalid_data(sample_invalid_hospitalization_data):
    """Test hospitalization initialization with invalid data."""
    hosp_obj = hospitalization(sample_invalid_hospitalization_data)
    assert hosp_obj.df is not None
    assert hosp_obj.isvalid() is False
    assert len(hosp_obj.errors) > 0
    # Example: Check for specific error types if your validator produces them
    # error_messages = [error["message"] for error in hosp_obj.errors]
    # assert any("Missing columns" in msg for msg in error_messages)
    # assert any("Invalid data type" in msg for msg in error_messages)

def test_hospitalization_init_without_data():
    """Test hospitalization initialization without data."""
    hosp_obj = hospitalization()
    assert hosp_obj.df is None
    assert hosp_obj.isvalid() is True # isvalid() should be true if no data to validate
    assert len(hosp_obj.errors) == 0

# Tests for from_file constructor
def test_hospitalization_from_file(mock_hospitalization_file):
    """Test loading hospitalization data from file."""
    hosp_obj = hospitalization.from_file(mock_hospitalization_file, "parquet")
    assert hosp_obj.df is not None
    assert hosp_obj.isvalid() is True
    assert len(hosp_obj.errors) == 0

def test_hospitalization_from_file_nonexistent(tmp_path):
    """Test loading hospitalization data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir") # from_file expects a directory path
    with pytest.raises(FileNotFoundError): # Or whatever error your load_data raises for missing dir/file
        hospitalization.from_file(non_existent_path, "parquet")

# Tests for isvalid method
def test_hospitalization_isvalid(sample_valid_hospitalization_data, sample_invalid_hospitalization_data):
    """Test isvalid method."""
    valid_hosp = hospitalization(sample_valid_hospitalization_data)
    assert valid_hosp.isvalid() is True
    
    invalid_hosp = hospitalization(sample_invalid_hospitalization_data)
    assert invalid_hosp.isvalid() is False

# Tests for validate method
def test_hospitalization_validate(sample_valid_hospitalization_data, sample_invalid_hospitalization_data):
    """Test validate method."""
    # Test with valid data
    valid_hosp = hospitalization()
    valid_hosp.df = sample_valid_hospitalization_data
    valid_hosp.validate()
    assert valid_hosp.isvalid() is True
    
    # Test with invalid data
    invalid_hosp = hospitalization()
    invalid_hosp.df = sample_invalid_hospitalization_data
    invalid_hosp.validate()
    assert invalid_hosp.isvalid() is False
    
    # Test with no data
    no_data_hosp = hospitalization()
    no_data_hosp.validate()
    assert no_data_hosp.df is None
    assert no_data_hosp.isvalid() is True # Should remain valid if no df

def test_hospitalization_validate_output(sample_valid_hospitalization_data, sample_invalid_hospitalization_data, capsys):
    """Test validate method output messages."""
    # Test with valid data
    valid_hosp = hospitalization(sample_valid_hospitalization_data)
    valid_hosp.validate() # Already called in init, but call again to capture output
    captured = capsys.readouterr()
    assert "Validation completed successfully" in captured.out
    
    # Test with invalid data
    invalid_hosp = hospitalization(sample_invalid_hospitalization_data)
    invalid_hosp.validate() # Already called in init
    captured = capsys.readouterr()
    assert "Validation completed with" in captured.out
    assert "error(s)" in captured.out
    
    # Test with no data
    no_data_hosp = hospitalization()
    no_data_hosp.validate()
    captured = capsys.readouterr()
    assert "No dataframe to validate" in captured.out

# --- Tests for Hospitalization Specific Methods ---

def test_calculate_length_of_stay(sample_valid_hospitalization_data):
    """Test calculate_length_of_stay method."""
    hosp_obj = hospitalization(sample_valid_hospitalization_data)
    los_df = hosp_obj.calculate_length_of_stay()
    assert 'length_of_stay_days' in los_df.columns
    assert not los_df['length_of_stay_days'].isnull().any()
    # Expected LOS for P001, H001: 2023-01-10 17:00:00 - 2023-01-01 10:00:00 = 9 days and 7 hours = 9 + 7/24 days
    expected_los_h001 = (pd.to_datetime('2023-01-10 17:00:00') - pd.to_datetime('2023-01-01 10:00:00')).total_seconds() / (24 * 3600)
    assert los_df[los_df['hospitalization_id'] == 'H001']['length_of_stay_days'].iloc[0] == pytest.approx(expected_los_h001)

    # Test with missing columns
    hosp_obj_missing_cols = hospitalization(pd.DataFrame({'hospitalization_id': ['H005']}))
    los_df_missing = hosp_obj_missing_cols.calculate_length_of_stay()
    assert los_df_missing.empty

def test_get_mortality_rate(sample_valid_hospitalization_data):
    """Test get_mortality_rate method."""
    hosp_obj = hospitalization(sample_valid_hospitalization_data)
    # In sample_valid_hospitalization_data: 1 expired out of 4 hospitalizations
    expected_mortality_rate = (1 / 4) * 100
    assert hosp_obj.get_mortality_rate() == pytest.approx(expected_mortality_rate)

    # Test with no 'Expired' cases
    data_no_expired = sample_valid_hospitalization_data.copy()
    data_no_expired['discharge_category'] = 'Home'
    hosp_obj_no_expired = hospitalization(data_no_expired)
    assert hosp_obj_no_expired.get_mortality_rate() == 0.0

    # Test with no data
    hosp_obj_no_data = hospitalization()
    assert hosp_obj_no_data.get_mortality_rate() == 0.0

    # Test with missing discharge_category column
    data_missing_col = sample_valid_hospitalization_data.drop(columns=['discharge_category'])
    hosp_obj_missing_col = hospitalization(data_missing_col)
    # Validation should fail, but test the method's robustness
    assert hosp_obj_missing_col.get_mortality_rate() == 0.0 

def test_get_summary_stats(sample_valid_hospitalization_data):
    """Test get_summary_stats method."""
    hosp_obj = hospitalization(sample_valid_hospitalization_data)
    stats = hosp_obj.get_summary_stats()
    
    assert stats['total_hospitalizations'] == 4
    assert stats['unique_patients'] == 3
    assert stats['discharge_category_counts']['Home'] == 2
    assert stats['discharge_category_counts']['Expired'] == 1
    assert 'admission_type_counts' in stats
    assert 'date_range' in stats
    assert 'age_stats' in stats
    assert 'length_of_stay_stats' in stats
    assert 'mortality_rate_percent' in stats
    assert stats['mortality_rate_percent'] == pytest.approx(25.0)

    # Test with no data
    hosp_obj_no_data = hospitalization()
    assert hosp_obj_no_data.get_summary_stats() == {}

def test_get_patient_hospitalization_counts(sample_valid_hospitalization_data):
    """Test get_patient_hospitalization_counts method."""
    hosp_obj = hospitalization(sample_valid_hospitalization_data)
    counts_df = hosp_obj.get_patient_hospitalization_counts()
    
    assert not counts_df.empty
    assert 'patient_id' in counts_df.columns
    assert 'hospitalization_count' in counts_df.columns
    assert 'first_admission' in counts_df.columns
    assert 'last_admission' in counts_df.columns
    assert 'care_span_days' in counts_df.columns
    
    # P001 has 2 hospitalizations
    p001_counts = counts_df[counts_df['patient_id'] == 'P001']
    assert p001_counts['hospitalization_count'].iloc[0] == 2
    
    # Check sorting (descending by count)
    assert counts_df['hospitalization_count'].is_monotonic_decreasing

    # Test with no data
    hosp_obj_no_data = hospitalization()
    assert hosp_obj_no_data.get_patient_hospitalization_counts().empty

    # Test with missing patient_id column
    data_missing_pid = sample_valid_hospitalization_data.drop(columns=['patient_id'])
    hosp_obj_missing_pid = hospitalization(data_missing_pid)
    # Validation should fail, but test the method's robustness
    assert hosp_obj_missing_pid.get_patient_hospitalization_counts().empty
