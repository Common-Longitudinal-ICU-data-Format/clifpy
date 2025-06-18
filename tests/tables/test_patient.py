"""
Tests for the patient table module.
"""
import os
import pytest
import pandas as pd
from datetime import datetime
from pyclif.tables.patient import patient
from pyclif.utils.validator import ValidationError

# Fixtures for patient testing
@pytest.fixture
def sample_valid_patient_data():
    """Create a valid patient DataFrame for testing."""
    return pd.DataFrame({
        'patient_id': ['P001', 'P002', 'P003'],
        'birth_date': pd.to_datetime(['1980-01-01', '1990-02-02', '2000-03-03']),
        'death_dttm': pd.to_datetime(['2050-01-01', '2060-02-02', '2070-03-03']),
        'race_name': ['White', 'Black', 'Asian'],
        'race_category': ['White', 'Black or African American', 'Asian'],
        'ethnicity_name': ['Not Hispanic', 'Hispanic', 'Not Hispanic'],
        'ethnicity_category': ['Non-Hispanic', 'Hispanic', 'Non-Hispanic'],
        'sex_name': ['Male', 'Female', 'Male'],
        'sex_category': ['Male', 'Female', 'Male'],
        'language_name': ['English', 'Spanish', 'English'],
        'language_category': ['English', 'Spanish', 'English']
    })

@pytest.fixture
def sample_invalid_patient_data():
    """Create an invalid patient DataFrame for testing."""
    return pd.DataFrame({
        'patient_id': ['P001', 'P002', 'P003'],
        'birth_date': pd.to_datetime(['1980-01-01', '1990-02-02', '2000-03-03']),
        'death_dttm': pd.to_datetime(['2050-01-01', '2060-02-02', '2070-03-03']),
        'race_name': ['White', 'Black', 'Asian'],
        'race_category': ['White', 'Black or African American', 'INVALID_VALUE'],  # Invalid value
        'ethnicity_name': ['Not Hispanic', 'Hispanic', 'Not Hispanic'],
        'ethnicity_category': ['Non-Hispanic', 'Hispanic', 'Non-Hispanic'],
        'sex_name': ['Male', 'Female', 'Male'],
        'sex_category': ['Male', 'Female', 'Unknown'],
        'language_name': ['English', 'Spanish', 'English']
        # Missing language_category column
    })

@pytest.fixture
def mock_patient_file(tmp_path, sample_valid_patient_data):
    """Create a mock patient file for testing."""
    # Create a temporary directory and file
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_patient.parquet"
    
    # Save the DataFrame to the file
    sample_valid_patient_data.to_parquet(file_path)
    
    return str(test_dir)

# Tests for patient class initialization
def test_patient_init_with_valid_data(sample_valid_patient_data):
    """Test patient initialization with valid data."""
    patient_obj = patient(sample_valid_patient_data)
    assert patient_obj.df is not None
    assert patient_obj.isvalid() is True
    assert len(patient_obj.errors) == 0

def test_patient_init_with_invalid_data(sample_invalid_patient_data):
    """Test patient initialization with invalid data."""
    patient_obj = patient(sample_invalid_patient_data)
    assert patient_obj.df is not None
    assert patient_obj.isvalid() is False
    assert len(patient_obj.errors) > 0
    
    # Check for specific errors
    error_types = [error["type"] for error in patient_obj.errors]
    assert "missing_columns" in error_types
    assert "invalid_category" in error_types

def test_patient_init_without_data():
    """Test patient initialization without data."""
    patient_obj = patient()
    assert patient_obj.df is None
    assert patient_obj.isvalid() is True
    assert len(patient_obj.errors) == 0

# Tests for from_file constructor
def test_patient_from_file(mock_patient_file, monkeypatch):
    """Test loading patient data from file."""
    # Test with parquet format
    patient_obj = patient.from_file(mock_patient_file, "parquet")
    assert patient_obj.df is not None
    assert patient_obj.isvalid() is True
    assert len(patient_obj.errors) == 0

def test_patient_from_file_nonexistent(tmp_path):
    """Test loading patient data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent")
    with pytest.raises(FileNotFoundError):
        patient.from_file(non_existent_path, "parquet")

# Tests for isvalid method
def test_patient_isvalid(sample_valid_patient_data, sample_invalid_patient_data):
    """Test isvalid method."""
    valid_patient = patient(sample_valid_patient_data)
    assert valid_patient.isvalid() is True
    
    invalid_patient = patient(sample_invalid_patient_data)
    assert invalid_patient.isvalid() is False

# Tests for validate method
def test_patient_validate(sample_valid_patient_data, sample_invalid_patient_data):
    """Test validate method."""
    # Test with valid data
    valid_patient = patient()
    valid_patient.df = sample_valid_patient_data
    valid_patient.validate()
    assert valid_patient.isvalid() is True
    
    # Test with invalid data
    invalid_patient = patient()
    invalid_patient.df = sample_invalid_patient_data
    invalid_patient.validate()
    assert invalid_patient.isvalid() is False
    
    # Test with no data
    no_data_patient = patient()
    no_data_patient.validate()
    assert no_data_patient.df is None

def test_patient_validate_output(sample_valid_patient_data, sample_invalid_patient_data, capsys):
    """Test validate method output messages."""
    # Test with valid data
    valid_patient = patient()
    valid_patient.df = sample_valid_patient_data
    valid_patient.validate()
    captured = capsys.readouterr()
    assert "Validation completed successfully" in captured.out
    
    # Test with invalid data
    invalid_patient = patient()
    invalid_patient.df = sample_invalid_patient_data
    invalid_patient.validate()
    captured = capsys.readouterr()
    assert "Validation completed with" in captured.out
    assert "error(s)" in captured.out
    
    # Test with no data
    no_data_patient = patient()
    no_data_patient.validate()
    captured = capsys.readouterr()
    assert "No dataframe to validate" in captured.out
