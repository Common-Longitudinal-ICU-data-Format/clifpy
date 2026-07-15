"""
Tests for the ADT (Admission, Discharge, Transfer) table module.
"""
import pytest
import pandas as pd
from clifpy.tables.adt import Adt
# from datetime import datetime
# from typing import Union, Any
# from clifpy.tables.adt import adt
# import clifpy.utils.validator # To patch its os module

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_adt_data():
    """Create a fully valid ADT DataFrame covering the complete mCIDE vocabulary.

    Every permissible value of hospital_type, location_category and
    location_type appears at least once, so no mCIDE coverage gaps are
    reported. Pairings follow the schema rules: only 'icu' rows carry a
    location_type (ICU locations must have one), and non-ICU rows leave it
    null (non-ICU locations must not have an ICU location_type).
    """
    rows = [
        # ('hospitalization_id', 'patient_id', 'hospital_id', 'hospital_type',
        #  'location_name', 'location_category', 'location_type', 'in_dttm', 'out_dttm')

        # --- ICU stays: cover all 10 location_type values ---
        ('H001', 'P001', 'HOSP_A', 'academic', 'B06F', 'icu', 'general_icu', '2023-01-01 08:00', '2023-01-01 12:00'),
        ('H001', 'P001', 'HOSP_A', 'academic', 'B06T', 'icu', 'medical_icu', '2023-01-01 12:00', '2023-01-01 18:00'),
        ('H001', 'P001', 'HOSP_A', 'academic', 'T09F', 'icu', 'surgical_icu', '2023-01-01 18:00', '2023-01-02 06:00'),
        ('H001', 'P001', 'HOSP_A', 'academic', 'C04N', 'icu', 'cardiac_icu', '2023-01-02 06:00', '2023-01-02 14:00'),
        ('H002', 'P002', 'HOSP_B', 'community', 'N23E', 'icu', 'neuro_icu', '2023-01-03 09:00', '2023-01-03 20:00'),
        ('H002', 'P002', 'HOSP_B', 'community', 'BRN1', 'icu', 'burn_icu', '2023-01-03 20:00', '2023-01-04 08:00'),
        ('H002', 'P002', 'HOSP_B', 'community', 'NS02', 'icu', 'neurosurgical_icu', '2023-01-04 08:00', '2023-01-04 19:00'),
        ('H003', 'P003', 'HOSP_C', 'LTACH', 'MN05', 'icu', 'mixed_neuro_icu', '2023-01-05 07:00', '2023-01-05 15:00'),
        ('H003', 'P003', 'HOSP_C', 'LTACH', 'CT03', 'icu', 'cardiothoracic_surgical_icu', '2023-01-05 15:00', '2023-01-06 02:00'),
        ('H003', 'P003', 'HOSP_C', 'LTACH', 'MC07', 'icu', 'mixed_cardiothoracic_icu', '2023-01-06 02:00', '2023-01-06 11:00'),

        # --- Non-ICU stays: cover the other 11 location_category values ---
        ('H004', 'P004', 'HOSP_A', 'academic', 'ED01', 'ed', None, '2023-01-07 06:00', '2023-01-07 10:00'),
        ('H004', 'P004', 'HOSP_A', 'academic', 'W12A', 'ward', None, '2023-01-07 10:00', '2023-01-08 09:00'),
        ('H004', 'P004', 'HOSP_A', 'academic', 'SD03', 'stepdown', None, '2023-01-08 09:00', '2023-01-09 08:00'),
        ('H004', 'P004', 'HOSP_A', 'academic', 'PROC2', 'procedural', None, '2023-01-09 08:00', '2023-01-09 12:00'),
        ('H005', 'P005', 'HOSP_B', 'community', 'LD01', 'l&d', None, '2023-01-10 07:00', '2023-01-10 15:00'),
        ('H005', 'P005', 'HOSP_B', 'community', 'HSP1', 'hospice', None, '2023-01-10 15:00', '2023-01-11 10:00'),
        ('H005', 'P005', 'HOSP_B', 'community', 'PSY2', 'psych', None, '2023-01-11 10:00', '2023-01-12 09:00'),
        ('H005', 'P005', 'HOSP_B', 'community', 'RHB4', 'rehab', None, '2023-01-12 09:00', '2023-01-13 11:00'),
        ('H006', 'P006', 'HOSP_C', 'LTACH', 'RAD1', 'radiology', None, '2023-01-14 08:00', '2023-01-14 11:00'),
        ('H006', 'P006', 'HOSP_C', 'LTACH', 'DIA3', 'dialysis', None, '2023-01-14 11:00', '2023-01-14 16:00'),
        ('H006', 'P006', 'HOSP_C', 'LTACH', 'OTH9', 'other', None, '2023-01-14 16:00', '2023-01-15 09:00'),
    ]
    df = pd.DataFrame(rows, columns=[
        'hospitalization_id', 'patient_id', 'hospital_id', 'hospital_type',
        'location_name', 'location_category', 'location_type', 'in_dttm', 'out_dttm'
    ])
    df['in_dttm'] = pd.to_datetime(df['in_dttm']).dt.tz_localize('UTC')
    df['out_dttm'] = pd.to_datetime(df['out_dttm']).dt.tz_localize('UTC')
    return df

@pytest.fixture
def sample_adt_data_missing_cols():
    """Create an ADT DataFrame with schema violations."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        'in_dttm': ['2023-01-01 10:00:00+00:00', '2023-01-05 09:00:00+00:00'], # Invalid datetime format for direct use / wrong type
        'out_dttm': pd.to_datetime(['2023-01-01 13:59:00+00:00', '2023-01-08 11:00:00+00:00']),
        'location_category': ['ed', 'ward']
    })


@pytest.fixture
def sample_adt_data_invalid_category():
    """Create an ADT DataFrame with invalid category values."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        'hospital_id': ['HOSP_A', 'HOSP_B'],
        'patient_id': ['P001', 'P002'],
        'in_dttm': pd.to_datetime(['2023-01-01 10:00:00+00:00', '2023-01-05 09:00:00+00:00']),
        'out_dttm': pd.to_datetime(['2023-01-01 13:59:00+00:00', '2023-01-08 11:00:00+00:00']),
        'location_name': ['B06F', 'B06T'],
        'location_category': ['INVALID_LOCATION', 'ICU'], # Invalid location
        'hospital_type': ['ACADEMIC', 'INVALID_HOSP_TYPE'], # Invalid hospital type
        'location_type': ['INVALID_icu', 'medical_icu'] # Invalid location type
    })

@pytest.fixture
def sample_adt_data_invalid_datetime():
    """Create an ADT DataFrame with invalid category values."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        'hospital_id': ['HOSP_A', 'HOSP_B'],
        'patient_id': ['P001', 'P002'],
        'in_dttm': pd.to_datetime(['2023-01-01 13:59:00', '2023-01-01 13:59:00']),
        'out_dttm': pd.to_datetime(['2023-01-01 13:59:00+00:00', '2023-01-08 11:00:00+00:00']),
        'location_name': ['B06F', 'B06T'],
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
    assert "Invalid Categorical Values" in error_types
    assert "Missing Required Columns" not in error_types

def test_adt_init_with_missing_columns(sample_adt_data_missing_cols):
    """Test adt initialization with missing required columns."""
    adt_obj = Adt(data=sample_adt_data_missing_cols)
    adt_obj.validate()
    assert adt_obj.isvalid() is False
    assert len(adt_obj.errors) > 0
    error_types = {e['type'] for e in adt_obj.errors}
    assert "Missing Required Columns" in error_types
    # Missing columns are now reported one error per column via details.column,
    # rather than a single error carrying a 'columns' list.
    missing_cols = {
        e['details']['column']
        for e in adt_obj.errors
        if e['type'] == 'Missing Required Columns'
    }
    assert missing_cols == {'hospital_id', 'hospital_type', 'location_type'}

def test_adt_init_without_data():
    """Test adt initialization without data."""
    adt_obj = Adt()
    adt_obj.validate()
    assert adt_obj.df is None
    assert adt_obj.isvalid() is False # isvalid is True because no errors were generated
    assert not adt_obj.errors

def test_timezone_validation_non_utc_datetime(sample_adt_data_invalid_datetime):
    """Test that non-UTC datetime columns fail timezone validation."""
    adt_obj = Adt(data=sample_adt_data_invalid_datetime)
    adt_obj.validate()
    
    # Should fail due to non-UTC timezone
    assert adt_obj.isvalid() is False

# from_file constructor
def test_adt_from_file(mock_adt_file):
    adt_obj = Adt.from_file(data_directory=mock_adt_file, filetype="parquet", timezone="UTC")
    assert adt_obj.df is not None

def test_adt_from_file_nonexistent(tmp_path):
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        Adt.from_file(non_existent_path, filetype="parquet", timezone="UTC")

# isvalid method
def test_adt_isvalid(sample_valid_adt_data, sample_adt_data_invalid_category):
    """Test isvalid method."""
    valid_adt = Adt(data=sample_valid_adt_data)
    valid_adt.validate()
    assert valid_adt.isvalid() is True


    invalid_adt = Adt(data=sample_adt_data_invalid_category)
    invalid_adt.validate()
    assert invalid_adt.isvalid() is False

# validate method
def test_adt_validate_output(sample_adt_data_invalid_category, capsys):
    """Test validate method output messages."""
    # Invalid data
    invalid_adt = Adt(data=sample_adt_data_invalid_category)
    invalid_adt.validate()
    captured = capsys.readouterr()
    # Assert on the message, not the count, so fixture changes don't break this.
    assert "Validation completed with" in captured.out
    
    # No data
    adt_no_data = Adt()
    adt_no_data.validate() # Explicitly call validate
    captured = capsys.readouterr()
    assert "No dataframe to validate" in captured.out
