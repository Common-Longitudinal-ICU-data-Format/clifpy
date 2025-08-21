"""
Tests for the patient table module.
"""
import pytest
import pandas as pd
from clifpy.tables.patient import Patient

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_patient_data():
    """Create a valid patient DataFrame for testing."""
    return pd.DataFrame({
        'patient_id': ['P001', 'P002', 'P003'],
        'birth_date': pd.to_datetime(['1980-01-01', '1990-02-02', '2000-03-03']),
        'death_dttm': pd.to_datetime([pd.NaT, pd.NaT, '2024-12-01 08:15:00+00:00 UTC']),
        'race_name': ['white', 'black or african american', 'asian'],
        'race_category': ['White', 'Black or African American', 'Asian'],
        'ethnicity_name': ['hispanic', 'non-hispanic', 'non-hispanic'],
        'ethnicity_category': ['Non-Hispanic', 'Hispanic', 'Non-Hispanic'],
        'sex_name': ['male', 'female', 'male'],
        'sex_category': ['Male', 'Female', 'Male'],
        'language_name': ['english', 'spanish', 'english'],
        'language_category': ['English', 'Spanish', 'English']
    })

@pytest.fixture
def sample_patient_data_invalid_category():
    """Create a patient DataFrame with invalid categorical values."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'birth_date': pd.to_datetime(['1980-01-01']),
        'death_dttm': pd.to_datetime(['2024-12-01 08:15:00+00:00 UTC']),
        'race_name': ['white'],
        'ethnicity_name': ['hispanic'],
        'sex_name': ['male'],
        'language_name': ['english'],
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
        'birth_date': pd.to_datetime(['1980-01-01']),
        'death_dttm': pd.to_datetime(['2024-12-01 08:15:00+00:00 UTC']),
        'race_name': ['white'],
        'ethnicity_name': ['hispanic'],
        'sex_name': ['male'],
        'language_name': ['english'],
        # Missing race_category, ethnicity_category, sex_category
    })

@pytest.fixture
def sample_patient_data_invalid_datetime():
    """Create a patient DataFrame with invalid categorical values."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'birth_date': pd.to_datetime(['1980-01-01']),
        'death_dttm': pd.to_datetime(['2024-12-01 08:15:00+00:00 EST']),
        'race_name': ['white'],
        'ethnicity_name': ['hispanic'],
        'sex_name': ['male'],
        'language_name': ['english'],
        'race_category': ['INVALID_RACE'],  # Invalid value
        'ethnicity_category': ['Non-Hispanic'],
        'sex_category': ['Male'],
        'language_category': ['English']
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
def test_patient_init_with_valid_data(sample_valid_patient_data):
    """Test patient initialization with valid data and mocked schema."""
    patient_obj = Patient(data=sample_valid_patient_data)   
    patient_obj.validate()
    assert patient_obj.df is not None
    assert patient_obj.isvalid() is True
    assert not patient_obj.errors

def test_patient_init_with_invalid_category(sample_patient_data_invalid_category):
    """Test patient initialization with invalid categorical data."""
    patient_obj = Patient(data=sample_patient_data_invalid_category)
    patient_obj.validate()
    assert patient_obj.isvalid() is False
    assert len(patient_obj.errors) > 0
    error_types = {e['type'] for e in patient_obj.errors}
    assert "invalid_category" in error_types
    assert "missing_columns" not in error_types

def test_patient_init_with_missing_columns(sample_patient_data_missing_cols):
    """Test patient initialization with missing required columns."""
    patient_obj = Patient(data=sample_patient_data_missing_cols)
    patient_obj.validate()
    assert patient_obj.isvalid() is False
    assert len(patient_obj.errors) > 0
    error_types = {e['type'] for e in patient_obj.errors}
    assert "missing_columns" in error_types
    missing_cols = next(e['columns'] for e in patient_obj.errors if e['type'] == 'missing_columns')
    assert set(missing_cols) == {'race_category', 'ethnicity_category', 'sex_category', 'language_category'}


def test_patient_init_without_data():
    """Test patient initialization without data."""
    patient_obj = Patient()
    patient_obj.validate()
    assert patient_obj.df is None
    assert patient_obj.isvalid() is True # isvalid is True because no errors were generated
    assert not patient_obj.errors

def test_timezone_validation_non_utc_datetime(sample_patient_data_non_utc_timezone):
    """Test that non-UTC datetime columns fail timezone validation."""
    patient_obj = Patient(data=sample_patient_data_non_utc_timezone)
    patient_obj.validate()
    
    # Should fail due to non-UTC timezone
    assert patient_obj.isvalid() is False
    
    # Check that timezone validation errors exist
    timezone_errors = [e for e in patient_obj.errors if e.get('type') == 'datetime_timezone']
    assert len(timezone_errors) > 0, "Non-UTC datetime should cause timezone validation errors"
    
    # Verify the specific error details
    tz_error = timezone_errors[0]
    assert tz_error['column'] == 'death_dttm'
    assert 'EST' in str(tz_error.get('timezone', '')) 

# from_file constructor
def test_patient_from_file(mock_patient_file):
    """Test loading patient data from a parquet file."""
    patient_obj = Patient.from_file(data_directory=mock_patient_file, filetype="parquet")
    assert patient_obj.df is not None

def test_patient_from_file_nonexistent(tmp_path):
    """Test loading patient data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        Patient.from_file(non_existent_path, filetype="parquet")

# isvalid method
def test_patient_isvalid(sample_valid_patient_data, sample_patient_data_invalid_category):
    """Test isvalid method."""
    valid_patient = Patient(data=sample_valid_patient_data)
    assert valid_patient.isvalid() is True
    
    invalid_patient = Patient(data=sample_patient_data_invalid_category)
    assert invalid_patient.isvalid() is False

# validate method
def test_patient_validate_output(sample_valid_patient_data, sample_patient_data_invalid_category, capsys):
    """Test validate method output messages."""
    # Valid data
    valid_patient = Patient(data=sample_valid_patient_data)
    valid_patient.validate()
    captured = capsys.readouterr()
    assert "Validation completed successfully" in captured.out
    
    # Invalid data
    invalid_patient = Patient(data=sample_patient_data_invalid_category)
    invalid_patient.validate()
    captured = capsys.readouterr()
    assert "Validation completed with 1 error(s)" in captured.out
    
    # No data
    p_no_data = Patient()
    p_no_data.validate() # Explicitly call validate
    captured = capsys.readouterr()
    assert "No dataframe to validate" in captured.out
