"""
Tests for the patient table module.
"""
import os
import pytest
import pandas as pd
import json
from datetime import datetime
from typing import Optional
from pyclif.tables.patient import Patient


# --- Mock Schema ---
@pytest.fixture
def mock_patient_schema_content():
    """Provides the content for a mock PatientModel.json."""
    return {
        "required_columns": ["patient_id", "race_category", "ethnicity_category", "sex_category"],
        "columns": [
            {"name": "patient_id", "data_type": "VARCHAR", "required": True},
            {"name": "birth_date", "data_type": "DATETIME", "required": False},
            {"name": "death_dttm", "data_type": "DATETIME", "required": False},
            {"name": "race_category", "data_type": "VARCHAR", "required": True, "is_category_column": True, "permissible_values": ["White", "Black or African American", "Asian", "Other", "Unknown"]},
            {"name": "ethnicity_category", "data_type": "VARCHAR", "required": True, "is_category_column": True, "permissible_values": ["Hispanic", "Non-Hispanic", "Unknown"]},
            {"name": "sex_category", "data_type": "VARCHAR", "required": True, "is_category_column": True, "permissible_values": ["Male", "Female", "Other", "Unknown"]},
            {"name": "language_category", "data_type": "VARCHAR", "required": False, "is_category_column": True, "permissible_values": ["English", "Spanish", "Other"]}
        ]
    }

@pytest.fixture
def patch_load_spec(monkeypatch, mock_patient_schema_content):
    """
    Patches pyclif.utils.validator._load_spec to return a mock schema
    instead of reading from a file.
    """
    def mock_load_spec(table_name: str, spec_dir: Optional[str] = None):
        if table_name.lower() == "patient":
            return mock_patient_schema_content
        raise FileNotFoundError(f"Mock schema for table '{table_name}' not found.")

    monkeypatch.setattr("pyclif.utils.validator._load_spec", mock_load_spec)


# --- Data Fixtures ---
@pytest.fixture
def sample_valid_patient_data():
    """Create a valid patient DataFrame for testing."""
    return pd.DataFrame({
        'patient_id': ['P001', 'P002', 'P003'],
        'birth_date': pd.to_datetime(['1980-01-01', '1990-02-02', '2000-03-03']),
        'death_dttm': pd.to_datetime([pd.NaT, pd.NaT, '2070-03-03']),
        'race_category': ['White', 'Black or African American', 'Asian'],
        'ethnicity_category': ['Non-Hispanic', 'Hispanic', 'Non-Hispanic'],
        'sex_category': ['Male', 'Female', 'Male'],
        'language_category': ['English', 'Spanish', 'English']
    })

@pytest.fixture
def sample_patient_data_invalid_category():
    """Create a patient DataFrame with invalid categorical values."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'birth_date': pd.to_datetime(['1980-01-01']),
        'race_category': ['INVALID_RACE'],  # Invalid value
        'ethnicity_category': ['Non-Hispanic'],
        'sex_category': ['Male'],
        'language_category': ['English']
    })

@pytest.fixture
def sample_patient_data_missing_cols():
    """Create a patient DataFrame with missing required columns."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'birth_date': pd.to_datetime(['1980-01-01'])
        # Missing race_category, ethnicity_category, sex_category
    })

@pytest.fixture
def mock_patient_file(tmp_path, sample_valid_patient_data):
    """Create a mock patient parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_patient.parquet"
    sample_valid_patient_data.to_parquet(file_path)
    return str(test_dir) # from_file expects directory path

# --- Tests for patient class ---

# Initialization and Schema Loading
@pytest.mark.usefixtures("patch_load_spec")
def test_patient_init_with_valid_data(sample_valid_patient_data):
    """Test patient initialization with valid data and mocked schema."""
    patient_obj = Patient(sample_valid_patient_data)
    assert patient_obj.df is not None
    assert patient_obj.isvalid() is True
    assert not patient_obj.errors

@pytest.mark.usefixtures("patch_load_spec")
def test_patient_init_with_invalid_category(sample_patient_data_invalid_category):
    """Test patient initialization with invalid categorical data."""
    patient_obj = Patient(sample_patient_data_invalid_category)
    assert patient_obj.isvalid() is False
    assert len(patient_obj.errors) > 0
    error_types = {e['type'] for e in patient_obj.errors}
    assert "invalid_category" in error_types
    assert "missing_columns" not in error_types

@pytest.mark.usefixtures("patch_load_spec")
def test_patient_init_with_missing_columns(sample_patient_data_missing_cols):
    """Test patient initialization with missing required columns."""
    patient_obj = Patient(sample_patient_data_missing_cols)
    assert patient_obj.isvalid() is False
    assert len(patient_obj.errors) > 0
    error_types = {e['type'] for e in patient_obj.errors}
    assert "missing_columns" in error_types
    missing_cols = next(e['columns'] for e in patient_obj.errors if e['type'] == 'missing_columns')
    assert set(missing_cols) == {'race_category', 'ethnicity_category', 'sex_category'}


def test_patient_init_without_data():
    """Test patient initialization without data."""
    patient_obj = Patient()
    assert patient_obj.df is None
    assert patient_obj.isvalid() is True # isvalid is True because no errors were generated
    assert not patient_obj.errors

def test_schema_loading_file_not_found(monkeypatch):
    """Test schema loading when the schema file is not found."""
    def mock_load_spec_raises(table_name: str, spec_dir: Optional[str] = None):
        raise FileNotFoundError("Mocked File Not Found")
    monkeypatch.setattr("pyclif.utils.validator._load_spec", mock_load_spec_raises)
    
    df = pd.DataFrame({'patient_id': ['P001']})
    with pytest.raises(FileNotFoundError, match="Mocked File Not Found"):
        Patient(df) # __init__ calls validate(), which calls _load_spec and will fail

def test_schema_loading_json_error(monkeypatch):
    """Test schema loading when the schema file is malformed."""
    def mock_load_spec_raises(table_name: str, spec_dir: Optional[str] = None):
        raise json.JSONDecodeError("Mocked JSON error", "doc", 0)
    monkeypatch.setattr("pyclif.utils.validator._load_spec", mock_load_spec_raises)
    
    df = pd.DataFrame({'patient_id': ['P001']})
    with pytest.raises(json.JSONDecodeError):
        Patient(df)

# from_file constructor
@pytest.mark.usefixtures("patch_load_spec")
def test_patient_from_file(mock_patient_file):
    """Test loading patient data from a parquet file."""
    patient_obj = Patient.from_file(mock_patient_file, table_format_type="parquet")
    assert patient_obj.df is not None
    assert patient_obj.isvalid() is True

def test_patient_from_file_nonexistent(tmp_path):
    """Test loading patient data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        Patient.from_file(non_existent_path, table_format_type="parquet")

# isvalid method
@pytest.mark.usefixtures("patch_load_spec")
def test_patient_isvalid(sample_valid_patient_data, sample_patient_data_invalid_category):
    """Test isvalid method."""
    valid_patient = Patient(sample_valid_patient_data)
    assert valid_patient.isvalid() is True
    
    invalid_patient = Patient(sample_patient_data_invalid_category)
    assert invalid_patient.isvalid() is False

# validate method
@pytest.mark.usefixtures("patch_load_spec")
def test_patient_validate_output(sample_valid_patient_data, sample_patient_data_invalid_category, capsys):
    """Test validate method output messages."""
    # Valid data
    Patient(sample_valid_patient_data)
    captured = capsys.readouterr()
    assert "Validation completed successfully" in captured.out
    
    # Invalid data
    Patient(sample_patient_data_invalid_category)
    captured = capsys.readouterr()
    assert "Validation completed with 1 error(s)" in captured.out
    
    # No data
    p_no_data = Patient()
    p_no_data.validate() # Explicitly call validate
    captured = capsys.readouterr()
    assert "No dataframe to validate" in captured.out
