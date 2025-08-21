"""
Tests for the ADT (Admission, Discharge, Transfer) table module.
"""
import pytest
import pandas as pd
from clifpy.tables.adt import Adt

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_adt_data():
    """Create a valid ADT DataFrame for testing."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H002', 'H003'],
        'hospital_id': ['HOSP_A', 'HOSP_A', 'HOSP_B', 'HOSP_A'],
        'in_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-01-01 14:00:00', '2023-01-05 09:00:00', '2023-01-10 12:00:00']).tz_localize('UTC'),
        'out_dttm': pd.to_datetime(['2023-01-01 13:59:00', '2023-01-03 18:00:00', '2023-01-08 11:00:00', '2023-01-12 15:00:00']).tz_localize('UTC'),
        'location_name': ['B06F', 'B06T', 'T09F', 'N23E'],
        'location_category': ['ed', 'icu', 'ward', 'icu'],
        'hospital_type': ['academic', 'academic', 'academic', 'academic'],
        'location_type': ['general_icu', 'medical_icu', 'general_icu', 'general_icu']
    })

@pytest.fixture
def sample_adt_data_missing_cols():
    """Create an ADT DataFrame with schema violations."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        'in_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-01-05 09:00:00']).tz_localize('UTC'),
        'out_dttm': pd.to_datetime(['2023-01-01 13:59:00', '2023-01-08 11:00:00']).tz_localize('UTC'),
        'location_category': ['ed', 'ward']
        # Missing: hospital_id, hospital_type
    })

@pytest.fixture
def sample_adt_data_invalid_category():
    """Create an ADT DataFrame with invalid category values."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        'hospital_id': ['HOSP_A', 'HOSP_B'],
        'in_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-01-05 09:00:00']).tz_localize('UTC'),
        'out_dttm': pd.to_datetime(['2023-01-01 13:59:00', '2023-01-08 11:00:00']).tz_localize('UTC'),
        'location_name': ['B06F', 'B06T'],
        'location_category': ['INVALID_LOCATION', 'ICU'], # Invalid location
        'hospital_type': ['ACADEMIC', 'INVALID_HOSP_TYPE'], # Invalid hospital type
        # Note: location_type is not in the schema, so it won't be validated
    })

@pytest.fixture
def sample_adt_data_invalid_datetime():
    """Create an ADT DataFrame with timezone-naive datetime columns."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        'hospital_id': ['HOSP_A', 'HOSP_B'],
        'in_dttm': (['2023', '2023']), 
        'out_dttm': (['2023', '2023']), 
        'location_name': ['B06F', 'B06T'],
        'location_category': ['ed', 'ward'],
        'hospital_type': ['academic', 'academic']
    })

@pytest.fixture
def mock_adt_file(tmp_path, sample_valid_adt_data):
    """Create a mock patient parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_adt.parquet"
    sample_valid_adt_data.to_parquet(file_path)
    return str(test_dir) # from_file expects directory path

# --- Tests for adt class --- 

# Initialization and Schema Loading
def test_adt_init_with_valid_data(sample_valid_adt_data):
    """Test adt initialization with valid data and mocked schema."""
    adt_obj = Adt(data=sample_valid_adt_data)   
    adt_obj.validate()
    assert adt_obj.df is not None
    assert adt_obj.isvalid() is True
    assert not adt_obj.errors

def test_adt_init_with_invalid_category(sample_adt_data_invalid_category):
    """Test adt initialization with invalid categorical data."""
    adt_obj = Adt(data=sample_adt_data_invalid_category)
    adt_obj.validate()
    assert adt_obj.isvalid() is False
    assert len(adt_obj.errors) > 0
    error_types = {e['type'] for e in adt_obj.errors}
    assert "invalid_category" in error_types
    assert "missing_columns" not in error_types

def test_adt_init_with_missing_columns(sample_adt_data_missing_cols):
    """Test adt initialization with missing required columns."""
    adt_obj = Adt(data=sample_adt_data_missing_cols)
    adt_obj.validate()
    assert adt_obj.isvalid() is False
    assert len(adt_obj.errors) > 0
    error_types = {e['type'] for e in adt_obj.errors}
    assert "missing_columns" in error_types
    missing_cols = next(e['columns'] for e in adt_obj.errors if e['type'] == 'missing_columns')
    # According to the schema, these are the required columns that are missing
    expected_missing = {'hospital_id', 'hospital_type'}
    assert set(missing_cols) == expected_missing


def test_patient_init_without_data():
    """Test adt initialization without data."""
    adt_obj = Adt()
    adt_obj.validate()
    # Note: isvalid() returns False when there's no dataframe to validate
    # This is expected behavior from the base table
    assert adt_obj.df is None

def test_timezone_validation_non_utc_datetime(sample_adt_data_invalid_datetime):
    """Test that timezone-naive datetime columns generate timezone validation info messages but don't fail validation."""
    adt_obj = Adt(data=sample_adt_data_invalid_datetime)
    adt_obj.validate()
    
    # Should pass validation since timezone-naive datetimes are just info, not errors
    assert adt_obj.isvalid() is False
    
    # Check that timezone validation info messages exist
    timezone_results = [e for e in adt_obj.errors if e.get('type') == 'datetime_timezone']
    
    # Note: timezone-naive datetimes have status 'info' and don't get added to errors
    # Only 'warning' and 'error' status results get added to errors
    # So we expect no timezone errors in this case
    assert len(timezone_results) > 0

# from_file constructor
def test_adt_from_file(mock_adt_file):
    """Test loading adt data from a parquet file."""
    adt_obj = Adt.from_file(data_directory=mock_adt_file, filetype="parquet")
    assert adt_obj.df is not None

def test_adt_from_file_nonexistent(tmp_path):
    """Test loading adt data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        Adt.from_file(non_existent_path, filetype="parquet")

# isvalid method
def test_adt_isvalid(sample_valid_adt_data, sample_adt_data_invalid_category):
    """Test isvalid method."""
    valid_adt = Adt(data=sample_valid_adt_data)
    valid_adt.validate()  # Need to call validate first
    assert valid_adt.isvalid() is True
    
    invalid_adt = Adt(data=sample_adt_data_invalid_category)
    invalid_adt.validate()  # Need to call validate first
    assert invalid_adt.isvalid() is False

# validate method
def test_adt_validate_output(sample_valid_adt_data, sample_adt_data_invalid_category, capsys):
    """Test validate method output messages."""
    # Valid data
    valid_adt = Adt(data=sample_valid_adt_data)
    valid_adt.validate()
    captured = capsys.readouterr()
    assert "Validation completed successfully" in captured.out
    
    ## Invalid data - should have multiple validation errors
    invalid_adt = Adt(data=sample_adt_data_invalid_category)
    invalid_adt.validate()
    captured = capsys.readouterr()
    # The validation will find multiple errors (categorical + potentially others)
    assert "Validation completed with" in captured.out
    assert "error(s)" in captured.out
    
    # No data
    adt_no_data = Adt()
    adt_no_data.validate() # Explicitly call validate
    captured = capsys.readouterr()
    assert "No dataframe to validate" in captured.out
