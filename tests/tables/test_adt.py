"""
Tests for the ADT (Admission, Discharge, Transfer) table module.
"""
import os
import pytest
import pandas as pd
import json
from datetime import datetime
from typing import Union, Any
from pyclif.tables.adt import adt
import pyclif.utils.validator # To patch its os module

# --- Mock Schema --- 
@pytest.fixture
def mock_adt_schema_content():
    """Provides the content for a mock ADTModel.json."""
    return {
        "columns": [
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True},
            {"name": "hospital_id", "data_type": "VARCHAR", "required": False},
            {"name": "patient_id", "data_type": "VARCHAR", "required": True}, # Assuming patient_id is also key
            {"name": "in_dttm", "data_type": "DATETIME", "required": True},
            {"name": "out_dttm", "data_type": "DATETIME", "required": True},
            {"name": "location_category", "data_type": "VARCHAR", "required": True, "is_category_column": True, "permissible_values": ["ICU", "WARD", "ED", "OR", "OTHER"]},
            {"name": "hospital_type", "data_type": "VARCHAR", "required": False, "is_category_column": True, "permissible_values": ["ACADEMIC", "COMMUNITY", "CRITICAL_ACCESS", "OTHER"]}
        ],
        "required_columns": ["hospitalization_id", "patient_id", "in_dttm", "out_dttm", "location_category"]
        # No ADT-specific schema properties like 'lab_reference_units' in labs
    }

@pytest.fixture
def mock_mcide_dir(tmp_path):
    """Creates a temporary mCIDE directory."""
    mcide_path = tmp_path / "mCIDE"
    mcide_path.mkdir(exist_ok=True)
    return mcide_path

@pytest.fixture
def mock_adt_model_json(mock_mcide_dir, mock_adt_schema_content):
    """Creates a mock ADTModel.json file in the temporary mCIDE directory."""
    schema_file_path = mock_mcide_dir / "ADTModel.json"
    with open(schema_file_path, 'w') as f:
        json.dump(mock_adt_schema_content, f)
    return schema_file_path

# Removed duplicate import of Union, Any from here

@pytest.fixture
def patch_adt_schema_load(monkeypatch, mock_adt_schema_content):
    """Patches pyclif.utils.validator._load_spec to return the mock ADT schema content."""
    
    original_load_spec = pyclif.utils.validator._load_spec

    def mock_load_spec(table_name: str, spec_dir: Union[str, None] = None) -> dict[str, Any]: # Added return type hint for clarity
        if table_name.lower() == "adt":
            # Return a copy to prevent modification by the validator if it ever does that
            return mock_adt_schema_content.copy() 
        # For any other table, or if spec_dir is explicitly provided (which our ADT class doesn't do),
        # fall back to original behavior to not break other tests or functionalities.
        # However, for unit testing ADT, we expect spec_dir to be None.
        if spec_dir is not None:
             return original_load_spec(table_name, spec_dir)
        # This case should ideally not be hit if the mock is only for 'adt' table and adt class calls it correctly.
        # If other tables are tested and _load_spec is called without spec_dir, they might hit this.
        # For robustness in the mock, we might let original_load_spec handle it or raise a specific error.
        # The original _load_spec has a default for spec_dir, so calling it without should be fine.
        return original_load_spec(table_name, spec_dir) # Fallback to original if not 'adt' or if spec_dir is None for other tables

    monkeypatch.setattr(pyclif.utils.validator, '_load_spec', mock_load_spec)
    yield
    monkeypatch.setattr(pyclif.utils.validator, '_load_spec', original_load_spec)


# --- Data Fixtures ---
@pytest.fixture
def sample_valid_adt_data():
    """Create a valid ADT DataFrame for testing."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H002', 'H003'],
        'hospital_id': ['HOSP_A', 'HOSP_A', 'HOSP_B', 'HOSP_A'],
        'patient_id': ['P001', 'P001', 'P002', 'P003'],
        'in_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 14:00', '2023-01-05 09:00', '2023-01-10 12:00']),
        'out_dttm': pd.to_datetime(['2023-01-01 13:59', '2023-01-03 18:00', '2023-01-08 11:00', '2023-01-12 15:00']),
        'location_category': ['ED', 'ICU', 'WARD', 'ICU'],
        'hospital_type': ['ACADEMIC', 'ACADEMIC', 'COMMUNITY', 'ACADEMIC']
    })

@pytest.fixture
def sample_invalid_adt_data_schema():
    """Create an ADT DataFrame with schema violations."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        # Missing patient_id
        'in_dttm': ['2023-01-01 10:00', '2023-01-05 09:00'], # Invalid datetime format for direct use / wrong type
        'out_dttm': pd.to_datetime(['2023-01-01 13:59', '2023-01-08 11:00']),
        'location_category': ['ED', 'WARD']
        # Missing other required columns implicitly
    })

@pytest.fixture
def sample_invalid_adt_data_category():
    """Create an ADT DataFrame with invalid category values."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H002'],
        'patient_id': ['P001', 'P002'],
        'in_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-05 09:00']),
        'out_dttm': pd.to_datetime(['2023-01-01 13:59', '2023-01-08 11:00']),
        'location_category': ['INVALID_LOCATION', 'ICU'], # Invalid location
        'hospital_type': ['ACADEMIC', 'INVALID_HOSP_TYPE'] # Invalid hospital type
    })

@pytest.fixture
def mock_adt_file_path(tmp_path, sample_valid_adt_data):
    """Creates a mock ADT parquet file and returns its full path."""
    test_dir = tmp_path / "test_data_adt_file"
    test_dir.mkdir(exist_ok=True)
    file_path = test_dir / "clif_adt.parquet"
    sample_valid_adt_data.to_parquet(file_path)
    return str(file_path)

@pytest.fixture
def mock_adt_file_directory(tmp_path, sample_valid_adt_data):
    """Creates a mock ADT parquet file in a directory and returns the directory path."""
    test_dir = tmp_path / "test_data_adt_dir"
    test_dir.mkdir(exist_ok=True)
    file_path = test_dir / "clif_adt.parquet" # Filename load_data will look for
    sample_valid_adt_data.to_parquet(file_path)
    return str(test_dir)


# --- Tests for adt class --- 

# Initialization and Schema-dependent validation
@pytest.mark.usefixtures("patch_adt_schema_load")
def test_adt_init_with_valid_data(sample_valid_adt_data):
    """Test adt initialization with valid data and mocked schema."""
    adt_obj = adt(sample_valid_adt_data)
    assert adt_obj.df is not None
    assert adt_obj.isvalid() is True
    assert not adt_obj.errors

@pytest.mark.usefixtures("patch_adt_schema_load")
def test_adt_init_with_invalid_schema_data(sample_invalid_adt_data_schema):
    """Test adt initialization with schema-invalid data (missing columns, wrong types)."""
    adt_obj = adt(sample_invalid_adt_data_schema)
    assert adt_obj.df is not None
    assert adt_obj.isvalid() is False
    assert len(adt_obj.errors) > 0
    error_types = [e['type'] for e in adt_obj.errors]
    # Expect 'missing_columns' due to 'patient_id' missing from sample_invalid_adt_data_schema
    # Expect 'datatype_mismatch' for 'in_dttm' (string instead of datetime)
    assert 'missing_columns' in error_types
    assert 'datatype_mismatch' in error_types
    # Check specific missing column
    missing_cols_error = next((e for e in adt_obj.errors if e['type'] == 'missing_columns'), None)
    assert missing_cols_error is not None
    assert 'patient_id' in missing_cols_error['columns']
    # Check specific datatype mismatch
    datatype_error = next((e for e in adt_obj.errors if e['type'] == 'datatype_mismatch'), None)
    assert datatype_error is not None
    assert datatype_error['column'] == 'in_dttm'
    assert datatype_error['expected'] == 'DATETIME'

@pytest.mark.usefixtures("patch_adt_schema_load")
def test_adt_init_with_invalid_category_data(sample_invalid_adt_data_category):
    """Test adt initialization with invalid category data."""
    adt_obj = adt(sample_invalid_adt_data_category)
    assert adt_obj.df is not None
    assert adt_obj.isvalid() is False
    assert len(adt_obj.errors) > 0
    error_types = [e['type'] for e in adt_obj.errors]
    assert 'invalid_category' in error_types
    # Check specific invalid categories
    invalid_cat_errors = [e for e in adt_obj.errors if e['type'] == 'invalid_category']
    assert len(invalid_cat_errors) >= 1 # Could be one error per column or combined
    location_error = next((e for e in invalid_cat_errors if e['column'] == 'location_category'), None)
    assert location_error is not None
    assert 'INVALID_LOCATION' in location_error['values']
    
    # hospital_type is not required, so if it's all NaN or only invalid, it might not appear if not handled by validator
    # The current mock schema does not mark hospital_type as required, so invalid values are the primary check.
    hospital_type_error = next((e for e in invalid_cat_errors if e['column'] == 'hospital_type'), None)
    if hospital_type_error: # hospital_type might be optional and thus not error if all are NaN after bad values
        assert 'INVALID_HOSP_TYPE' in hospital_type_error['values']

@pytest.mark.usefixtures("patch_adt_schema_load")
def test_adt_init_without_data():
    """Test adt initialization without data."""
    adt_obj = adt()
    assert adt_obj.df is None
    assert adt_obj.isvalid() is True # No data, so no validation errors
    assert not adt_obj.errors

# from_file constructor
@pytest.mark.usefixtures("patch_adt_schema_load") # Schema load must be patched for validation during init
def test_adt_from_file(mock_adt_file_directory, sample_valid_adt_data):
    """Test loading ADT data from a parquet file using directory path."""
    # adt.from_file calls load_data("adt", table_path=mock_adt_file_directory, ...)
    # load_data then constructs the full path: os.path.join(table_path, f"clif_{table_name}.{table_format_type}")
    adt_obj = adt.from_file(table_path=mock_adt_file_directory, table_format_type="parquet")

    assert adt_obj.df is not None
    # Parquet read can sometimes alter dtypes (e.g. int64 vs int32) or index.
    # Reset index and check_dtype=False for robustness.
    pd.testing.assert_frame_equal(adt_obj.df.reset_index(drop=True), 
                                   sample_valid_adt_data.reset_index(drop=True), 
                                   check_dtype=False)
    assert adt_obj.isvalid() is True # Validation is called in init after load

@pytest.mark.usefixtures("patch_adt_schema_load") # Not strictly needed as it should fail before schema load
def test_adt_from_file_nonexistent(tmp_path):
    """Test loading ADT data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_adt_file.parquet")
    with pytest.raises(FileNotFoundError): # Based on load_data utility raising FileNotFoundError
        adt.from_file(non_existent_path, table_format_type="parquet")

# isvalid method
@pytest.mark.usefixtures("patch_adt_schema_load")
def test_adt_isvalid(sample_valid_adt_data, sample_invalid_adt_data_schema):
    """Test isvalid method."""
    valid_adt = adt(sample_valid_adt_data)
    assert valid_adt.isvalid() is True
    
    invalid_adt = adt(sample_invalid_adt_data_schema)
    assert invalid_adt.isvalid() is False 

# validate method output

# --- Helper Method Tests --- 

@pytest.mark.usefixtures("patch_adt_schema_load")
def test_get_location_categories(sample_valid_adt_data):
    """Test get_location_categories method."""
    adt_obj = adt(sample_valid_adt_data)
    categories = adt_obj.get_location_categories()
    assert isinstance(categories, list)
    assert set(categories) == {'ED', 'ICU', 'WARD'}

    # Test with empty DataFrame
    adt_empty = adt(pd.DataFrame(columns=sample_valid_adt_data.columns))
    assert adt_empty.get_location_categories() == []

    # Test with DataFrame missing 'location_category' column
    adt_no_col = adt(sample_valid_adt_data.drop(columns=['location_category']))
    assert adt_no_col.get_location_categories() == []

    # Test with DataFrame where 'location_category' is all NaN
    data_nan_loc = sample_valid_adt_data.copy()
    data_nan_loc['location_category'] = pd.NA
    adt_nan_loc = adt(data_nan_loc)
    assert adt_nan_loc.get_location_categories() == []
    
    # Test with no data
    adt_none = adt()
    assert adt_none.get_location_categories() == []

@pytest.mark.usefixtures("patch_adt_schema_load")
def test_get_hospital_types(sample_valid_adt_data):
    """Test get_hospital_types method."""
    adt_obj = adt(sample_valid_adt_data)
    types = adt_obj.get_hospital_types()
    assert isinstance(types, list)
    assert set(types) == {'ACADEMIC', 'COMMUNITY'}

    # Test with empty DataFrame
    adt_empty = adt(pd.DataFrame(columns=sample_valid_adt_data.columns))
    assert adt_empty.get_hospital_types() == []

    # Test with DataFrame missing 'hospital_type' column
    adt_no_col = adt(sample_valid_adt_data.drop(columns=['hospital_type']))
    assert adt_no_col.get_hospital_types() == []

    # Test with DataFrame where 'hospital_type' is all NaN
    data_nan_hosp = sample_valid_adt_data.copy()
    data_nan_hosp['hospital_type'] = pd.NA
    adt_nan_hosp = adt(data_nan_hosp)
    assert adt_nan_hosp.get_hospital_types() == []

    # Test with no data
    adt_none = adt()
    assert adt_none.get_hospital_types() == []

@pytest.mark.usefixtures("patch_adt_schema_load")
def test_filter_by_hospitalization(sample_valid_adt_data):
    """Test filter_by_hospitalization method."""
    adt_obj = adt(sample_valid_adt_data)
    
    # Filter for an existing hospitalization_id
    filtered_df = adt_obj.filter_by_hospitalization('H001')
    assert len(filtered_df) == 2
    assert all(filtered_df['hospitalization_id'] == 'H001')

    # Filter for a non-existent hospitalization_id
    filtered_df_nonexistent = adt_obj.filter_by_hospitalization('H999')
    assert filtered_df_nonexistent.empty

    # Test with empty DataFrame
    adt_empty = adt(pd.DataFrame(columns=sample_valid_adt_data.columns))
    assert adt_empty.filter_by_hospitalization('H001').empty

    # Test with no data
    adt_none = adt()
    assert adt_none.filter_by_hospitalization('H001').empty

@pytest.mark.usefixtures("patch_adt_schema_load")
def test_filter_by_location_category(sample_valid_adt_data):
    """Test filter_by_location_category method."""
    adt_obj = adt(sample_valid_adt_data)
    
    # Filter for an existing location_category
    filtered_df_icu = adt_obj.filter_by_location_category('ICU')
    assert len(filtered_df_icu) == 2
    assert all(filtered_df_icu['location_category'] == 'ICU')

    # Filter for a non-existent location_category
    filtered_df_nonexistent = adt_obj.filter_by_location_category('NONEXISTENT_LOC')
    assert filtered_df_nonexistent.empty

    # Test with empty DataFrame
    adt_empty = adt(pd.DataFrame(columns=sample_valid_adt_data.columns))
    assert adt_empty.filter_by_location_category('ICU').empty

    # Test with DataFrame missing 'location_category' column
    adt_no_col = adt(sample_valid_adt_data.drop(columns=['location_category']))
    assert adt_no_col.filter_by_location_category('ICU').empty

    # Test with no data
    adt_none = adt()
    assert adt_none.filter_by_location_category('ICU').empty

@pytest.mark.usefixtures("patch_adt_schema_load")
def test_filter_by_date_range(sample_valid_adt_data):
    """Test filter_by_date_range method."""
    adt_obj = adt(sample_valid_adt_data)
    
    # Define date range for 'in_dttm'
    start_date_in = datetime(2023, 1, 1, 12, 0, 0) # Includes H001 (2nd record), H002, H003
    end_date_in = datetime(2023, 1, 10, 15, 0, 0)
    filtered_df_in = adt_obj.filter_by_date_range(start_date_in, end_date_in, date_column='in_dttm')
    assert len(filtered_df_in) == 3 # H001 (14:00), H002 (09:00), H003 (12:00)
    assert 'H001' in filtered_df_in['hospitalization_id'].values
    assert 'H002' in filtered_df_in['hospitalization_id'].values
    assert 'H003' in filtered_df_in['hospitalization_id'].values
    assert not any(d < start_date_in for d in pd.to_datetime(filtered_df_in['in_dttm']))
    assert not any(d > end_date_in for d in pd.to_datetime(filtered_df_in['in_dttm']))

    # Define date range for 'out_dttm'
    start_date_out = datetime(2023, 1, 3, 0, 0, 0) # Includes H001 (2nd record), H002, H003
    end_date_out = datetime(2023, 1, 12, 23, 59, 59)
    filtered_df_out = adt_obj.filter_by_date_range(start_date_out, end_date_out, date_column='out_dttm')
    assert len(filtered_df_out) == 3 # H001 (01-03), H002 (01-08), H003 (01-12)
    assert 'H001' in filtered_df_out['hospitalization_id'].values
    assert 'H002' in filtered_df_out['hospitalization_id'].values
    assert 'H003' in filtered_df_out['hospitalization_id'].values
    assert not any(d < start_date_out for d in pd.to_datetime(filtered_df_out['out_dttm']))
    assert not any(d > end_date_out for d in pd.to_datetime(filtered_df_out['out_dttm']))

    # Test with a date range that yields no results
    filtered_df_none = adt_obj.filter_by_date_range(datetime(2024, 1, 1), datetime(2024, 1, 2))
    assert filtered_df_none.empty

    # Test with empty DataFrame
    adt_empty = adt(pd.DataFrame(columns=sample_valid_adt_data.columns))
    assert adt_empty.filter_by_date_range(start_date_in, end_date_in).empty

    # Test with DataFrame missing the date_column
    adt_no_col = adt(sample_valid_adt_data.drop(columns=['in_dttm']))
    assert adt_no_col.filter_by_date_range(start_date_in, end_date_in, date_column='in_dttm').empty

    # Test with no data
    adt_none = adt()
    assert adt_none.filter_by_date_range(start_date_in, end_date_in).empty

@pytest.mark.usefixtures("patch_adt_schema_load")
def test_get_summary_stats(sample_valid_adt_data):
    """Test get_summary_stats method."""
    adt_obj = adt(sample_valid_adt_data)
    stats = adt_obj.get_summary_stats()

    assert isinstance(stats, dict)
    assert stats['total_records'] == 4
    assert stats['unique_hospitalizations'] == 3
    assert stats['unique_hospitals'] == 2 # HOSP_A, HOSP_B
    assert stats['location_category_counts'] == {'ED': 1, 'ICU': 2, 'WARD': 1}
    assert stats['hospital_type_counts'] == {'ACADEMIC': 3, 'COMMUNITY': 1}
    assert stats['date_range']['earliest_in'] == pd.Timestamp('2023-01-01 10:00:00')
    assert stats['date_range']['latest_in'] == pd.Timestamp('2023-01-10 12:00:00')
    assert stats['date_range']['earliest_out'] == pd.Timestamp('2023-01-01 13:59:00')
    assert stats['date_range']['latest_out'] == pd.Timestamp('2023-01-12 15:00:00')

    # Test with empty DataFrame
    adt_empty = adt(pd.DataFrame(columns=sample_valid_adt_data.columns))
    stats_empty = adt_empty.get_summary_stats()
    assert stats_empty['total_records'] == 0
    assert stats_empty['unique_hospitalizations'] == 0
    assert stats_empty['unique_hospitals'] == 0
    assert stats_empty['location_category_counts'] == {}
    assert stats_empty['hospital_type_counts'] == {}
    assert pd.isna(stats_empty['date_range']['earliest_in']) # empty series.min() is NaT
    assert pd.isna(stats_empty['date_range']['latest_in'])
    assert pd.isna(stats_empty['date_range']['earliest_out'])
    assert pd.isna(stats_empty['date_range']['latest_out'])

    # Test with DataFrame missing some columns used in stats
    data_missing_cols = sample_valid_adt_data[['hospitalization_id', 'in_dttm']].copy()
    adt_missing_cols = adt(data_missing_cols)
    stats_missing = adt_missing_cols.get_summary_stats()
    assert stats_missing['total_records'] == 4
    assert stats_missing['unique_hospitalizations'] == 3
    assert stats_missing['unique_hospitals'] == 0 # hospital_id was dropped
    assert stats_missing['location_category_counts'] == {} # location_category was dropped
    assert stats_missing['hospital_type_counts'] == {} # hospital_type was dropped
    assert stats_missing['date_range']['earliest_in'] == pd.Timestamp('2023-01-01 10:00:00')
    assert stats_missing['date_range']['latest_in'] == pd.Timestamp('2023-01-10 12:00:00')
    assert stats_missing['date_range']['earliest_out'] is None # out_dttm was dropped
    assert stats_missing['date_range']['latest_out'] is None
    
    # Test with no data
    adt_none = adt()
    stats_none = adt_none.get_summary_stats()
    assert stats_none == {}

@pytest.mark.usefixtures("patch_adt_schema_load")
def test_adt_validate_output(sample_valid_adt_data, sample_invalid_adt_data_schema, capsys):
    """Test validate method output messages."""
    # Valid data - validation runs at init
    adt_obj_valid = adt(sample_valid_adt_data)
    captured = capsys.readouterr() 
    assert "Validation completed successfully." in captured.out

    # Invalid schema data - validation runs at init
    adt_obj_invalid = adt(sample_invalid_adt_data_schema)
    captured = capsys.readouterr()
    assert "Validation completed with" in captured.out
    assert "error(s)" in captured.out

    # No data - explicit call to validate needed to check its specific print for no data
    adt_obj_no_data = adt()
    adt_obj_no_data.validate() 
    captured = capsys.readouterr()
    assert "No dataframe to validate." in captured.out
 
