"""
Tests for the ADT (Admission, Discharge, Transfer) table module.
"""
import pytest
import pandas as pd
from clifpy.tables.adt import Adt
# from datetime import datetime
# from typing import Union, Any
# from pyclif.tables.adt import adt
# import pyclif.utils.validator # To patch its os module

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_adt_data():
    """Create a valid ADT DataFrame for testing."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H002', 'H003'],
        'hospital_id': ['HOSP_A', 'HOSP_A', 'HOSP_B', 'HOSP_A'],
        'patient_id': ['P001', 'P001', 'P002', 'P003'],
        'in_dttm': pd.to_datetime(['2023-01-01 10:00:00+00:00 UTC', '2023-01-01 14:00:00+00:00 UTC', '2023-01-05 09:00:00+00:00 UTC', '2023-01-10 12:00:00+00:00 UTC']),
        'out_dttm': pd.to_datetime(['2023-01-01 13:59:00+00:00 UTC', '2023-01-03 18:00:00+00:00 UTC', '2023-01-08 11:00:00+00:00 UTC', '2023-01-12 15:00:00+00:00 UTC']),
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
        'in_dttm': ['2023-01-01 10:00:00+00:00 UTC', '2023-01-05 09:00:00+00:00 UTC'], # Invalid datetime format for direct use / wrong type
        'out_dttm': pd.to_datetime(['2023-01-01 13:59:00+00:00 UTC', '2023-01-08 11:00:00+00:00 UTC']),
        'location_category': ['ed', 'ward']
    })

@pytest.fixture
def sample_adt_data_invalid_category():
    """Create an ADT DataFrame with invalid category values."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        'hospital_id': ['HOSP_A', 'HOSP_B'],
        'patient_id': ['P001', 'P002'],
        'in_dttm': pd.to_datetime(['2023-01-01 10:00:00+00:00 UTC', '2023-01-05 09:00:00+00:00 UTC']),
        'out_dttm': pd.to_datetime(['2023-01-01 13:59:00+00:00 UTC', '2023-01-08 11:00:00+00:00 UTC']),
        'location_name': ['B06F', 'B06T', 'T09F', 'N23E'],
        'location_category': ['INVALID_LOCATION', 'ICU'], # Invalid location
        'hospital_type': ['ACADEMIC', 'INVALID_HOSP_TYPE'], # Invalid hospital type
        'location_type': ['INVALID_icu', 'medical_icu'] # Invalid location type
    })

def sample_adt_data_invalid_datetime():
    """Create an ADT DataFrame with invalid category values."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        'hospital_id': ['HOSP_A', 'HOSP_B'],
        'patient_id': ['P001', 'P002'],
        'in_dttm': pd.to_datetime(['2023', '2023']),
        'out_dttm': pd.to_datetime(['2023-01-01 13:59:00+00:00 EST', '2023-01-08 11:00:00+00:00 EST']),
        'location_name': ['B06F', 'B06T', 'T09F', 'N23E'],
        'location_category': ['INVALID_LOCATION', 'ICU'], # Invalid location
        'hospital_type': ['ACADEMIC', 'INVALID_HOSP_TYPE'], # Invalid hospital type
        'location_type': ['INVALID_icu', 'medical_icu'] # Invalid location type
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
    assert set(missing_cols) == {'hospital_id', 'patient_id', 'location_name', 'location_category', 'hospital_type', 'location_type'}


def test_patient_init_without_data():
    """Test adt initialization without data."""
    adt_obj = Adt()
    adt_obj.validate()
    assert adt_obj.df is None
    assert adt_obj.isvalid() is True # isvalid is True because no errors were generated
    assert not adt_obj.errors

def test_timezone_validation_non_utc_datetime(sample_adt_data_invalid_datetime):
    """Test that non-UTC datetime columns fail timezone validation."""
    adt_obj = Adt(data=sample_adt_data_invalid_datetime)
    adt_obj.validate()
    
    # Should fail due to non-UTC timezone
    assert adt_obj.isvalid() is False
    
    # Check that timezone validation errors exist
    timezone_errors = [e for e in adt_obj.errors if e.get('type') == 'datetime_timezone']
    assert len(timezone_errors) > 0, "Non-UTC datetime should cause timezone validation errors"
    
    # Verify the specific error details
    tz_error = timezone_errors[0]
    assert tz_error['column'] == 'in_dttm'
    assert 'EST' in str(tz_error.get('timezone', '')) 
    assert tz_error['column'] == 'out_dttm'
    assert 'EST' in str(tz_error.get('timezone', '')) 

## Base table does the below, so we don't need to test it here
# def test_schema_loading_file_not_found(tmp_path):
#     """Test schema loading when the schema file is not found."""
#     # Create a temporary directory without schemas
#     test_dir = tmp_path / "test_data"
#     test_dir.mkdir()
    
#     # This should work (schema loading is handled gracefully)
#     patient_obj = Patient(data_directory=str(test_dir), filetype="parquet")
#     assert patient_obj.schema is None  # Schema will be None if not found
#     assert patient_obj.errors == []    # No errors during init

# def test_schema_loading_malformed_yaml(tmp_path, monkeypatch):
#     """Test schema loading when the YAML schema is malformed."""
#     # Create a malformed YAML file
#     schema_dir = tmp_path / "schemas"
#     schema_dir.mkdir()
#     malformed_schema = schema_dir / "patient_schema.yaml"
#     malformed_schema.write_text("invalid: yaml: content: [")
    
#     # Mock the schema directory path
#     def mock_schema_dir(*args, **kwargs):
#         return str(schema_dir)
    
#     monkeypatch.setattr("clifpy.tables.base_table.BaseTable._load_schema", 
#                        lambda self: self._load_schema_from_dir(str(schema_dir)))
    
#     # This should handle the YAML error gracefully
#     patient_obj = Patient(data_directory=str(tmp_path), filetype="parquet")
#     assert patient_obj.schema is None

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
    assert valid_adt.isvalid() is True
    
    invalid_adt = Adt(data=sample_adt_data_invalid_category)
    assert invalid_adt.isvalid() is False

# validate method
def test_adt_validate_output(sample_valid_adt_data, sample_adt_data_invalid_category, capsys):
    """Test validate method output messages."""
    # Valid data
    valid_adt = Adt(data=sample_valid_adt_data)
    valid_adt.validate()
    captured = capsys.readouterr()
    assert "Validation completed successfully" in captured.out
    
    # Invalid data
    invalid_adt = Adt(data=sample_adt_data_invalid_category)
    invalid_adt.validate()
    captured = capsys.readouterr()
    assert "Validation completed with 1 error(s)" in captured.out
    
    # No data
    adt_no_data = Adt()
    adt_no_data.validate() # Explicitly call validate
    captured = capsys.readouterr()
    assert "No dataframe to validate" in captured.out
