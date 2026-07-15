"""
Tests for the hospitalization table module.
"""
import pytest
import pandas as pd
from clifpy.tables.hospitalization import Hospitalization

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_hospitalization_data():
    """Create a fully valid hospitalization DataFrame covering the complete mCIDE vocabulary.

    Every permissible value of admission_type_category (6) and discharge_category
    (17) appears at least once, so no mCIDE coverage gaps are reported. There is
    one row per discharge_category (the longest vocabulary, which sets the row
    count) and admission types cycle across those rows; hospitalization_id stays
    unique because it is the composite key.
    """
    rows = [
        # ('patient_id', 'hospitalization_id', 'admission_dttm', 'discharge_dttm', 'age_at_admission',
        #  'admission_type_name', 'admission_type_category', 'discharge_name', 'discharge_category',
        #  'zipcode_nine_digit', 'zipcode_five_digit')
        ('P001', 'H001', '2023-01-01 10:00', '2023-01-05 16:00', 65, 'emergency department', 'ed', 'home', 'Home', '12345-6789', '12345'),
        ('P002', 'H002', '2023-01-08 09:30', '2023-01-15 11:00', 78, 'transfer from facility', 'facility', 'snf', 'Skilled Nursing Facility (SNF)', '23456-7890', '23456'),
        ('P003', 'H003', '2023-02-01 14:30', '2023-02-08 12:00', 81, 'outside hospital', 'osh', 'expired', 'Expired', '34567-8901', '34567'),
        ('P004', 'H004', '2023-02-10 08:15', '2023-02-20 10:00', 59, 'direct admit', 'direct', 'acute rehab', 'Acute Inpatient Rehab Facility', '45678-9012', '45678'),
        ('P005', 'H005', '2023-03-01 08:15', '2023-03-10 09:30', 88, 'elective', 'elective', 'hospice', 'Hospice', '56789-0123', '56789'),
        ('P006', 'H006', '2023-03-12 16:45', '2023-03-30 13:00', 72, 'other', 'other', 'ltach', 'Long Term Care Hospital (LTACH)', '67890-1234', '67890'),
        ('P007', 'H007', '2023-04-02 11:00', '2023-04-06 15:30', 54, 'emergency department', 'ed', 'acute care hospital', 'Acute Care Hospital', '78901-2345', '78901'),
        ('P008', 'H008', '2023-04-14 07:20', '2023-04-22 09:00', 43, 'transfer from facility', 'facility', 'group home', 'Group Home', '89012-3456', '89012'),
        ('P009', 'H009', '2023-05-03 19:00', '2023-05-12 10:15', 37, 'outside hospital', 'osh', 'chemical dependency', 'Chemical Dependency', '90123-4567', '90123'),
        ('P010', 'H010', '2023-05-19 13:10', '2023-05-21 08:45', 29, 'direct admit', 'direct', 'ama', 'Against Medical Advice (AMA)', '01234-5678', '01234'),
        ('P011', 'H011', '2023-06-01 09:00', '2023-06-11 14:00', 84, 'elective', 'elective', 'assisted living', 'Assisted Living', '12345-6780', '12345'),
        # 'Still Admitted' patients must not have a discharge_dttm.
        ('P012', 'H012', '2023-06-15 12:30', None, 67, 'other', 'other', 'still admitted', 'Still Admitted', '23456-7891', '23456'),
        ('P013', 'H013', '2023-07-04 06:00', '2023-07-09 17:00', 51, 'emergency department', 'ed', 'missing', 'Missing', '34567-8902', '34567'),
        ('P014', 'H014', '2023-07-18 15:45', '2023-07-25 11:30', 62, 'transfer from facility', 'facility', 'other', 'Other', '45678-9013', '45678'),
        ('P015', 'H015', '2023-08-02 10:00', '2023-08-16 09:00', 46, 'outside hospital', 'osh', 'psych hospital', 'Psychiatric Hospital', '56789-0124', '56789'),
        ('P016', 'H016', '2023-08-21 20:15', '2023-08-24 12:00', 33, 'direct admit', 'direct', 'shelter', 'Shelter', '67890-1235', '67890'),
        ('P017', 'H017', '2023-09-05 08:30', '2023-09-14 16:45', 40, 'elective', 'elective', 'jail', 'Jail', '78901-2346', '78901'),
    ]
    df = pd.DataFrame(rows, columns=[
        'patient_id', 'hospitalization_id', 'admission_dttm', 'discharge_dttm',
        'age_at_admission', 'admission_type_name', 'admission_type_category',
        'discharge_name', 'discharge_category', 'zipcode_nine_digit', 'zipcode_five_digit',
    ])
    df['admission_dttm'] = pd.to_datetime(df['admission_dttm']).dt.tz_localize('UTC')
    df['discharge_dttm'] = pd.to_datetime(df['discharge_dttm']).dt.tz_localize('UTC')
    return df

@pytest.fixture
def sample_hospitalization_data_invalid_category():
    """Create a hospitalization DataFrame with invalid categorical values."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'hospitalization_id': ['H001'],
        'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00']).tz_localize('UTC'),
        'discharge_dttm': pd.to_datetime(['2023-01-05 16:00:00']).tz_localize('UTC'),
        'age_at_admission': [65],
        'admission_type_name': ['emergency'],
        'admission_type_category': ['Emergency'],
        'discharge_name': ['home'],
        'discharge_category': ['INVALID_DISCHARGE'],  # Invalid value
        'zipcode_nine_digit': ['12345-6789'],
        'zipcode_five_digit': ['12345']
    })

@pytest.fixture
def sample_hospitalization_data_missing_cols():
    """Create a hospitalization DataFrame with missing required columns."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'hospitalization_id': ['H001'],
        'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00']).tz_localize('UTC'),
        'discharge_dttm': pd.to_datetime(['2023-01-05 16:00:00']).tz_localize('UTC'),
        'age_at_admission': [65],
        'admission_type_name': ['emergency'],
        'admission_type_category': ['Emergency'],
        'discharge_name': ['home'],
        'zipcode_nine_digit': ['12345-6789'],
        'zipcode_five_digit': ['12345']
    })

@pytest.fixture
def sample_hospitalization_data_invalid_datetime():
    """Create a hospitalization DataFrame with timezone-naive datetime columns."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'hospitalization_id': ['H001'],
        'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00']),
        'discharge_dttm': pd.to_datetime(['2023-01-05 16:00:00']),
        'age_at_admission': [65],
        'admission_type_name': ['emergency'],
        'admission_type_category': ['Emergency'],
        'discharge_name': ['home'],
        'discharge_category': ['Home'],  # Invalid value
        'zipcode_nine_digit': ['12345-6789'],
        'zipcode_five_digit': ['12345']
    })

@pytest.fixture
def sample_hospitalization_data_non_utc_timezone():
    """Create a hospitalization DataFrame with non-UTC timezone datetime columns."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'hospitalization_id': ['H001'],
        'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00']).tz_localize('America/New_York'),
        'discharge_dttm': pd.to_datetime(['2023-01-05 16:00:00']).tz_localize('UTC'),
        'age_at_admission': [65],
        'admission_type_name': ['emergency'],
        'admission_type_category': ['Emergency'],
        'discharge_name': ['home'],
        'discharge_category': ['Home'],  # Invalid value
        'zipcode_nine_digit': ['12345-6789'],
        'zipcode_five_digit': ['12345']
    })

@pytest.fixture
def sample_hospitalization_data_inccorect_5zip():
    """Create a hospitalization DataFrame with incorrect 5-digit zip codes (too short)."""
    return pd.DataFrame({
        'patient_id': ['P001', 'P002', 'P003'],
        'hospitalization_id': ['H001', 'H002', 'H003'],
        'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-02-01 14:30:00', '2023-03-01 08:15:00']).tz_localize('UTC'),
        'discharge_dttm': pd.to_datetime(['2023-01-05 16:00:00', '2023-02-08 12:00:00', '2023-03-10 09:30:00']).tz_localize('UTC'),
        'age_at_admission': [65, 45, 72],
        'admission_type_name': ['emergency', 'elective', 'urgent'],
        'admission_type_category': ['Emergency', 'Elective', 'Urgent'],
        'discharge_name': ['home', 'snf', 'expired'],
        'discharge_category': ['Home', 'Skilled Nursing Facility (SNF)', 'Expired'],
        'zipcode_nine_digit': ['12345-6789', '23456-7890', '34567-8901'],
        'zipcode_five_digit': ['123', '234', '345']
    })

@pytest.fixture
def sample_hospitalization_data_inccorect_9zip():
    """Create a hospitalization DataFrame with incorrect 9-digit zip codes (missing dash or too short)."""
    return pd.DataFrame({
        'patient_id': ['P001', 'P002', 'P003'],
        'hospitalization_id': ['H001', 'H002', 'H003'],
        'admission_dttm': pd.to_datetime(['2023-01-01 10:00:00', '2023-02-01 14:30:00', '2023-03-01 08:15:00']).tz_localize('UTC'),
        'discharge_dttm': pd.to_datetime(['2023-01-05 16:00:00', '2023-02-08 12:00:00', '2023-03-10 09:30:00']).tz_localize('UTC'),
        'age_at_admission': [65, 45, 72],
        'admission_type_name': ['emergency', 'elective', 'urgent'],
        'admission_type_category': ['Emergency', 'Elective', 'Urgent'],
        'discharge_name': ['home', 'snf', 'expired'],
        'discharge_category': ['Home', 'Skilled Nursing Facility (SNF)', 'Expired'],
        'zipcode_nine_digit': ['123456789', '23456-7890', '34567-8901'],
        'zipcode_five_digit': ['12345', '23456', '34567']
    })

@pytest.fixture
def mock_hospitalization_file(tmp_path, sample_valid_hospitalization_data):
    """Create a mock hospitalization parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_hospitalization.parquet"
    sample_valid_hospitalization_data.to_parquet(file_path)
    return str(test_dir)  # from_file expects directory path

# --- Tests for Hospitalization class ---

# Initialization and Schema Loading
def test_hospitalization_init_with_valid_data(sample_valid_hospitalization_data):
    """Test hospitalization initialization with valid data and mocked schema."""
    hosp_obj = Hospitalization(data=sample_valid_hospitalization_data)   
    hosp_obj.validate()
    assert hosp_obj.df is not None
    assert hosp_obj.isvalid() is True
    assert not hosp_obj.errors

def test_hospitalization_init_with_invalid_category(sample_hospitalization_data_invalid_category):
    """Test hospitalization initialization with invalid categorical data."""
    hosp_obj = Hospitalization(data=sample_hospitalization_data_invalid_category)
    hosp_obj.validate()
    assert hosp_obj.isvalid() is False
    assert len(hosp_obj.errors) > 0
    error_types = {e['type'] for e in hosp_obj.errors}
    assert "Invalid Categorical Values" in error_types

def test_hospitalization_init_with_missing_columns(sample_hospitalization_data_missing_cols):
    """Test hospitalization initialization with missing required columns."""
    hosp_obj = Hospitalization(data=sample_hospitalization_data_missing_cols)
    hosp_obj.validate()
    assert hosp_obj.isvalid() is False
    assert len(hosp_obj.errors) > 0
    error_types = {e['type'] for e in hosp_obj.errors}
    assert "Missing Required Columns" in error_types
    # Missing columns are now reported one error per column via details.column,
    # rather than a single error carrying a 'columns' list.
    missing_cols = {
        e['details']['column']
        for e in hosp_obj.errors
        if e['type'] == 'Missing Required Columns'
    }
    assert missing_cols == {'discharge_category'}

def test_hospitalization_init_without_data():
    """Test hospitalization initialization without data."""
    hosp_obj = Hospitalization()
    hosp_obj.validate()
    assert hosp_obj.df is None

def test_timezone_validation_non_utc_datetime(sample_hospitalization_data_non_utc_timezone):
    """Test that non-UTC datetime columns fail timezone validation."""
    hosp_obj = Hospitalization(data=sample_hospitalization_data_non_utc_timezone)
    hosp_obj.validate()

    # Should fail due to non-UTC timezone
    assert hosp_obj.isvalid() is False

    # Check that timezone validation errors exist
    timezone_errors = [e for e in hosp_obj.errors if e.get('type') == 'datetime_timezone']
    assert len(timezone_errors) > 0, "Non-UTC datetime should cause timezone validation errors"

    # Verify the specific error details
    tz_error = timezone_errors[0]
    assert tz_error['column'] in ['admission_dttm', 'discharge_dttm']
    assert 'America/New_York' in str(tz_error.get('timezone', ''))

# from_file constructor
def test_hospitalization_from_file(mock_hospitalization_file):
    """Test loading hospitalization data from a parquet file."""
    hosp_obj = Hospitalization.from_file(data_directory=mock_hospitalization_file, filetype="parquet", timezone="UTC")
    assert hosp_obj.df is not None

def test_hospitalization_from_file_nonexistent(tmp_path):
    """Test loading hospitalization data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        Hospitalization.from_file(non_existent_path, filetype="parquet", timezone="UTC")

# isvalid method
def test_hospitalization_isvalid(sample_valid_hospitalization_data, sample_hospitalization_data_invalid_category):
    """Test isvalid method."""
    valid_hosp = Hospitalization(data=sample_valid_hospitalization_data)
    valid_hosp.validate()
    assert valid_hosp.isvalid() is True
    
    invalid_hosp = Hospitalization(data=sample_hospitalization_data_invalid_category)
    invalid_hosp.validate()
    assert invalid_hosp.isvalid() is False

# validate method
def test_hospitalization_validate_output(sample_valid_hospitalization_data, sample_hospitalization_data_invalid_category, capsys):
    """Test validate method output messages."""
    # Valid data
    valid_hosp = Hospitalization(data=sample_valid_hospitalization_data)
    valid_hosp.validate()
    captured = capsys.readouterr()
    assert "Validation completed successfully" in captured.out
    
    # Invalid data
    invalid_hosp = Hospitalization(data=sample_hospitalization_data_invalid_category)
    invalid_hosp.validate()
    captured = capsys.readouterr()
    assert "Validation completed with" in captured.out
    assert "error(s)" in captured.out
    
    # No data
    h_no_data = Hospitalization()
    h_no_data.validate()
    captured = capsys.readouterr()
    assert "No dataframe to validate" in captured.out

# # --- Tests for incorrect zip code fixtures ---

# def test_hospitalization_incorrect_5zip(sample_hospitalization_data_inccorect_5zip):
#     """Test that hospitalization data with incorrect 5-digit zip codes is detected as invalid."""
#     hosp_obj = Hospitalization(data=sample_hospitalization_data_inccorect_5zip)
#     hosp_obj.validate()
#     assert not hosp_obj.isvalid()
#     # Check for zip code error in errors
#     zip_errors = [e for e in hosp_obj.errors if 'zipcode_five_digit' in str(e.get('columns', '')) or e.get('column', '') == 'zipcode_five_digit']
#     assert zip_errors, "Should have error(s) related to zipcode_five_digit"

# def test_hospitalization_incorrect_9zip(sample_hospitalization_data_inccorect_9zip):
#     """Test that hospitalization data with incorrect 9-digit zip codes is detected as invalid."""
#     hosp_obj = Hospitalization(data=sample_hospitalization_data_inccorect_9zip)
#     hosp_obj.validate()
#     assert not hosp_obj.isvalid()
#     # Check for zip code error in errors
#     zip_errors = [e for e in hosp_obj.errors if 'zipcode_nine_digit' in str(e.get('columns', '')) or e.get('column', '') == 'zipcode_nine_digit']
#     assert zip_errors, "Should have error(s) related to zipcode_nine_digit"
