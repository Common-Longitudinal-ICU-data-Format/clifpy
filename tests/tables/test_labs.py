"""
Tests for the labs table module.
"""
import os
import pytest
import pandas as pd
import json
from datetime import datetime
from pyclif.tables.labs import labs

# --- Mock Schema --- 
@pytest.fixture
def mock_lab_schema_content():
    """Provides the content for a mock LabsModel.json using columns/permissible_values structure."""
    return {
        "columns": [
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True},
            {"name": "lab_order_dttm", "data_type": "DATETIME", "required": True},
            {"name": "lab_collect_dttm", "data_type": "DATETIME", "required": True},
            {"name": "lab_result_dttm", "data_type": "DATETIME", "required": True},
            {"name": "lab_order_category", "data_type": "VARCHAR", "required": False, "is_category_column": True, "permissible_values": ["CBC", "Coags", "BMP"]},
            {"name": "lab_category", "data_type": "VARCHAR", "required": True, "is_category_column": True, "permissible_values": ["hemoglobin", "inr", "glucose_serum"]},
            {"name": "lab_value", "data_type": "VARCHAR", "required": True},
            {"name": "lab_value_numeric", "data_type": "DOUBLE", "required": False},
            {"name": "reference_unit", "data_type": "VARCHAR", "required": False},
            {"name": "value_source_concept_id", "data_type": "VARCHAR", "required": False}
        ],
        "lab_reference_units": {
            "hemoglobin": ["g/dL"],
            "inr": ["INR"],
            "glucose_serum": ["mg/dL", "mmol/L"]
        }
    }

@pytest.fixture
def mock_mcide_dir(tmp_path):
    """Creates a temporary mCIDE directory."""
    mcide_path = tmp_path / "mCIDE"
    mcide_path.mkdir()
    return mcide_path

@pytest.fixture
def mock_labs_model_json(mock_mcide_dir, mock_lab_schema_content):
    """Creates a mock LabsModel.json file in the temporary mCIDE directory."""
    schema_file_path = mock_mcide_dir / "LabsModel.json"
    with open(schema_file_path, 'w') as f:
        json.dump(mock_lab_schema_content, f)
    return schema_file_path

@pytest.fixture
def patch_lab_schema_path(monkeypatch, mock_labs_model_json):
    """Patches the path to LabsModel.json for the labs class."""
    # The labs class looks for ../mCIDE/LabsModel.json relative to its own location.
    # We need to make labs.py think it's in a structure where this path points to our mock file.
    # For simplicity in testing, we'll mock os.path.join and os.path.dirname
    # to directly return the path to our mock_labs_model_json when _load_lab_schema is called.

    original_dirname = os.path.dirname
    original_join = os.path.join
    original_abspath = os.path.abspath

    def mock_dirname(path):
        if '__file__' in path: # Heuristic: assuming it's labs.py trying to find itself
            # This should point to a dummy 'tables' directory such that '../mCIDE' resolves correctly
            # relative to the mock_labs_model_json's parent (tmp_path / "mCIDE")
            # So, if mock_labs_model_json is in tmp_path/mCIDE/LabsModel.json
            # then labs.py should think it's in tmp_path/tables/labs.py
            return str(mock_labs_model_json.parent.parent / "tables")
        return original_dirname(path)

    def mock_abspath(path):
        if '__file__' in path:
            return str(mock_labs_model_json.parent.parent / "tables" / "dummy_labs.py")
        return original_abspath(path)

    def mock_join(*args):
        # If it's trying to construct the schema path from the mocked dirname
        if len(args) > 1 and args[1] == '..' and args[2] == 'mCIDE' and args[3] == 'LabsModel.json':
            return str(mock_labs_model_json)
        return original_join(*args)

    monkeypatch.setattr(os.path, 'dirname', mock_dirname)
    monkeypatch.setattr(os.path, 'abspath', mock_abspath)
    monkeypatch.setattr(os.path, 'join', mock_join)

# --- Data Fixtures --- 
@pytest.fixture
def sample_valid_labs_data():
    """Create a valid labs DataFrame for testing."""
    return pd.DataFrame({
        'patient_id': ['P001', 'P001', 'P002'],
        'encounter_id': ['E001', 'E001', 'E002'],
        'hospitalization_id': ['H001', 'H001', 'H002'],
        'lab_id': ['L001', 'L002', 'L003'],
        'lab_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 11:00', '2023-01-02 09:00']),
        'lab_collect_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 11:00', '2023-01-02 09:00']),
        'lab_order_dttm': pd.to_datetime(['2023-01-01 09:55', '2023-01-01 10:55', '2023-01-02 08:55']),
        'lab_result_dttm': pd.to_datetime(['2023-01-01 10:05', '2023-01-01 11:05', '2023-01-02 09:05']),
        'lab_name': ['Glucose', 'Hemoglobin', 'INR'],
        'lab_code': ['GLU', 'HGB', 'PTINR'],
        'lab_system_name': ['SNOMED', 'SNOMED', 'LOINC'],
        'lab_order_category': ['CBC', 'CBC', 'Coags'],
        'lab_category': ['glucose_serum', 'hemoglobin', 'inr'],
        'lab_value': ['100.0', '14.5', '1.1'],
        'lab_value_numeric': [100.0, 14.5, 1.1],
        'value_source_value': ['100.0', '14.5', '1.1'],
        'value_source_concept_id': ['0', '0', '0'],
        'reference_unit': ['mg/dL', 'g/dL', 'INR'],
        'reference_range_low': [70.0, 13.5, 0.9],
        'reference_range_high': [110.0, 17.5, 1.2],
        'lab_result_source_system': ['LIS', 'LIS', 'LIS']
    })

@pytest.fixture
def sample_invalid_labs_data_schema():
    """Create a labs DataFrame with schema violations."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        # Missing encounter_id
        'lab_id': ['L001'],
        'lab_dttm': ['2023-01-01 10:00'], # Invalid datetime format for direct use
        'lab_name': ['Glucose'],
        'lab_code': ['GLU'],
        'lab_system_name': ['SNOMED'],
        'lab_category': ['CHEMISTRY'],
        'lab_value': ['100.0'], # Invalid type for lab_value
        'reference_unit': ['mg/dL']
        # Missing other optional fields, but crucial ones are type/presence
    })

@pytest.fixture
def sample_invalid_labs_data_units():
    """Create a labs DataFrame with invalid reference units for known categories."""
    return pd.DataFrame({
        'patient_id': ['P001', 'P002'],
        'encounter_id': ['E001', 'E002'],
        'hospitalization_id': ['H001', 'H002'],
        'lab_id': ['L001', 'L002'],
        'lab_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-02 11:00']),
        'lab_collect_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-02 11:00']),
        'lab_order_dttm': pd.to_datetime(['2023-01-01 09:55', '2023-01-02 10:55']),
        'lab_result_dttm': pd.to_datetime(['2023-01-01 10:05', '2023-01-02 11:05']),
        'lab_name': ['Glucose', 'Hemoglobin'],
        'lab_code': ['GLU', 'HGB'],
        'lab_system_name': ['SNOMED', 'SNOMED'],
        'lab_order_category': ['CBC', 'CBC'],
        'lab_category': ['glucose_serum', 'hemoglobin'],
        'lab_value': ['100.0', '14.5'],
        'lab_value_numeric': [100.0, 14.5],
        'value_source_value': ['100.0', '14.5'],
        'value_source_concept_id': ['0', '0'],
        'reference_unit': ['invalid_unit_chem', 'invalid_unit_hema'], # Invalid units
        'reference_range_low': [70.0, 13.5],
        'reference_range_high': [110.0, 17.5],
        'lab_result_source_system': ['LIS', 'LIS']
    })

@pytest.fixture
def sample_labs_data_unknown_category():
    """Create a labs DataFrame with an unknown lab category."""
    return pd.DataFrame({
        'patient_id': ['P001'],
        'encounter_id': ['E001'],
        'hospitalization_id': ['H001'],
        'lab_id': ['L001'],
        'lab_dttm': pd.to_datetime(['2023-01-01 10:00']),
        'lab_collect_dttm': pd.to_datetime(['2023-01-01 10:00']),
        'lab_order_dttm': pd.to_datetime(['2023-01-01 09:55']),
        'lab_result_dttm': pd.to_datetime(['2023-01-01 10:05']),
        'lab_name': ['Special Test'],
        'lab_code': ['SPC'],
        'lab_system_name': ['LOCAL'],
        'lab_order_category': ['CBC'],
        'lab_category': ['unknown_lab_cat'],  # Not in permissible_values
        'lab_value': ['50.0'],
        'lab_value_numeric': [50.0],
        'value_source_value': ['50.0'],
        'value_source_concept_id': ['0'],
        'reference_unit': ['units'],
        'reference_range_low': [40.0],
        'reference_range_high': [60.0],
        'lab_result_source_system': ['LIS']
    })

@pytest.fixture
def mock_labs_file(tmp_path, sample_valid_labs_data):
    """Create a mock labs parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_labs.parquet"
    sample_valid_labs_data.to_parquet(file_path)
    return str(test_dir) # from_file expects directory path

# --- Tests for labs class --- 

# Initialization and Schema Loading
def test_labs_init_with_valid_data(patch_lab_schema_path, sample_valid_labs_data):
    """Test labs initialization with valid data and mocked schema."""
    lab_obj = labs(sample_valid_labs_data)
    assert lab_obj.df is not None
    if not lab_obj.isvalid():
        print("DEBUG lab_obj.errors:", lab_obj.errors)
    assert lab_obj.isvalid() is True
    assert not lab_obj.errors
    assert not lab_obj.unit_validation_errors
    assert "glucose_serum" in lab_obj.reference_units # Check schema loaded

def test_labs_init_with_invalid_schema_data(patch_lab_schema_path, sample_invalid_labs_data_schema):
    """Test labs initialization with schema-invalid data."""
    # sample_invalid_labs_data_schema is intentionally missing columns and has lab_dttm as string list
    lab_obj = labs(sample_invalid_labs_data_schema)
    assert lab_obj.df is not None
    # Validation is called in __init__. Calling again is okay for explicitness.
    lab_obj.validate()
    assert lab_obj.isvalid() is False
    assert len(lab_obj.errors) > 0
    error_types = [e['type'] for e in lab_obj.errors]
    # Expect 'missing_columns' due to the fixture design.
    # If schema specifies lab_dttm as datetime, 'invalid_type' might also appear if not auto-converted by pandas before validation.
    assert 'missing_columns' in error_types

def test_labs_init_with_invalid_unit_data(patch_lab_schema_path, sample_invalid_labs_data_units):
    """Test labs initialization with unit-invalid data."""
    lab_obj = labs(sample_invalid_labs_data_units)
    assert lab_obj.df is not None
    lab_obj.validate() # Validation populates unit_validation_errors
    assert lab_obj.isvalid() is False
    assert not lab_obj.errors # Schema might be fine
    assert len(lab_obj.unit_validation_errors) > 0
    assert any(e['error_type'] == 'invalid_reference_unit' for e in lab_obj.unit_validation_errors)

def test_labs_init_without_data(patch_lab_schema_path):
    """Test labs initialization without data."""
    lab_obj = labs()
    assert lab_obj.df is None
    assert lab_obj.isvalid() is True
    assert not lab_obj.errors
    assert not lab_obj.unit_validation_errors
    assert "glucose_serum" in lab_obj.reference_units # Schema should still load

def test_load_lab_schema_file_not_found(monkeypatch, capsys):
    """Test _load_lab_schema when LabsModel.json is not found."""
    # Mock os.path.join to raise FileNotFoundError for the schema path
    def mock_join_raise_fnf(*args):
        if 'LabsModel.json' in args[-1]:
            raise FileNotFoundError("Mocked File Not Found")
        return os.path.join(*args)
    monkeypatch.setattr(os.path, 'join', mock_join_raise_fnf)
    
    lab_obj = labs() # Init will call _load_lab_schema
    assert lab_obj._reference_units == {}
    captured = capsys.readouterr()
    assert "Warning: LabsModel.json not found" in captured.out

def test_load_lab_schema_json_decode_error(monkeypatch, tmp_path, capsys):
    """Test _load_lab_schema when LabsModel.json is malformed."""
    # Create a malformed JSON file
    mcide_dir = tmp_path / "mCIDE"
    mcide_dir.mkdir()
    malformed_schema_path = mcide_dir / "LabsModel.json"
    with open(malformed_schema_path, 'w') as f:
        f.write("this is not json")

    original_dirname = os.path.dirname
    original_join = os.path.join
    original_abspath = os.path.abspath

    def mock_dirname_local(path):
        if '__file__' in path:
            return str(malformed_schema_path.parent.parent / "tables")
        return original_dirname(path)

    def mock_abspath_local(path):
        if '__file__' in path:
            return str(malformed_schema_path.parent.parent / "tables" / "dummy_labs.py")
        return original_abspath(path)

    def mock_join_local(*args):
        if len(args) > 1 and args[1] == '..' and args[2] == 'mCIDE' and args[3] == 'LabsModel.json':
            return str(malformed_schema_path)
        return original_join(*args)

    monkeypatch.setattr(os.path, 'dirname', mock_dirname_local)
    monkeypatch.setattr(os.path, 'abspath', mock_abspath_local)
    monkeypatch.setattr(os.path, 'join', mock_join_local)

    lab_obj = labs()
    assert lab_obj._reference_units == {}
    captured = capsys.readouterr()
    assert "Warning: Invalid JSON in LabsModel.json" in captured.out

# from_file constructor
@pytest.mark.usefixtures("patch_lab_schema_path")
def test_labs_from_file(mock_labs_file, sample_valid_labs_data):
    """Test loading labs data from a parquet file."""
    lab_obj = labs.from_file(mock_labs_file, table_format_type="parquet")
    assert lab_obj.df is not None
    pd.testing.assert_frame_equal(lab_obj.df.reset_index(drop=True), sample_valid_labs_data.reset_index(drop=True), check_dtype=False)
    assert lab_obj.isvalid() is True # Validation is called in init

@pytest.mark.usefixtures("patch_lab_schema_path")
def test_labs_from_file_nonexistent(tmp_path):
    """Test loading labs data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        labs.from_file(non_existent_path, table_format_type="parquet")

# isvalid method
@pytest.mark.usefixtures("patch_lab_schema_path")
def test_labs_isvalid(sample_valid_labs_data, sample_invalid_labs_data_units):
    """Test isvalid method."""
    valid_lab = labs(sample_valid_labs_data)
    assert valid_lab.isvalid() is True
    
    invalid_lab = labs(sample_invalid_labs_data_units)
    # isvalid() reflects the state after the last validate() call, which happens at init
    assert invalid_lab.isvalid() is False 

# validate method
@pytest.mark.usefixtures("patch_lab_schema_path")
def test_labs_validate_output(sample_valid_labs_data, sample_invalid_labs_data_units, capsys):
    """Test validate method output messages."""
    # Valid data
    lab_obj_valid = labs(sample_valid_labs_data) # Validation runs at init
    # lab_obj_valid.validate() # Call again to capture output if init doesn't print
    captured = capsys.readouterr() # Capture output from init's validate call
    assert "Validation completed successfully." in captured.out

    # Invalid unit data
    lab_obj_invalid = labs(sample_invalid_labs_data_units) # Validation runs at init
    # lab_obj_invalid.validate() # Call again if needed
    captured = capsys.readouterr() # Capture output from init's validate call
    assert "Validation completed with" in captured.out
    assert "reference unit error(s)" in captured.out

    # No data
    lab_obj_no_data = labs()
    lab_obj_no_data.validate() # Explicit call as init with no data might not print this
    captured = capsys.readouterr()
    assert "No dataframe to validate." in captured.out

# validate_reference_units method
@pytest.mark.usefixtures("patch_lab_schema_path")
def test_validate_reference_units_valid(sample_valid_labs_data):
    """Test validate_reference_units with valid units."""
    lab_obj = labs(sample_valid_labs_data)
    # validate_reference_units is called during init's validate()
    assert not lab_obj.unit_validation_errors
    assert lab_obj.isvalid() is True

@pytest.mark.usefixtures("patch_lab_schema_path")
def test_validate_reference_units_invalid_unit(sample_invalid_labs_data_units):
    """Test validate_reference_units with invalid units."""
    lab_obj = labs(sample_invalid_labs_data_units)
    assert len(lab_obj.unit_validation_errors) > 0
    assert any(e['error_type'] == 'invalid_reference_unit' for e in lab_obj.unit_validation_errors)
    assert lab_obj.isvalid() is False

@pytest.mark.usefixtures("patch_lab_schema_path")
def test_validate_reference_units_unknown_category(patch_lab_schema_path, sample_labs_data_unknown_category):
    """Test reference unit validation with an unknown lab category."""
    lab_obj = labs(sample_labs_data_unknown_category)
    # Should error as unknown category
    assert any(e.get('error_type', '') == 'unknown_lab_category' or e.get('type', '') == 'unknown_lab_category' for e in lab_obj.unit_validation_errors)
    assert lab_obj.isvalid() is False

@pytest.mark.usefixtures("patch_lab_schema_path")
def test_validate_reference_units_missing_columns():
    """Test validate_reference_units with missing required columns."""
    data = pd.DataFrame({'patient_id': ['P001']}) # Missing lab_category, reference_unit
    lab_obj = labs(data)
    # Schema validation will likely fail first, but unit validation should also note missing columns
    lab_obj.validate() # Ensure unit validation part is explicitly triggered if schema fails early
    assert any(e['error_type'] == 'missing_columns' for e in lab_obj.unit_validation_errors)

# --- Helper Method Tests --- 

@pytest.mark.usefixtures("patch_lab_schema_path")
def test_get_reference_unit_summary(sample_valid_labs_data, sample_invalid_labs_data_units):
    """Test get_reference_unit_summary."""
    # Valid data
    lab_obj_valid = labs(sample_valid_labs_data)
    summary_valid = lab_obj_valid.get_reference_unit_summary()
    assert not summary_valid.empty
    assert 'glucose_serum' in summary_valid['lab_category'].values
    assert summary_valid[summary_valid['lab_category'] == 'glucose_serum']['validation_status'].iloc[0] == 'Valid'

    # Invalid unit data
    lab_obj_invalid = labs(sample_invalid_labs_data_units)
    summary_invalid = lab_obj_invalid.get_reference_unit_summary()
    assert not summary_invalid.empty
    # For glucose_serum, actual units are ['invalid_unit_chem'] which is not in expected ['mg/dL', 'mmol/L']
    glu_status = summary_invalid[summary_invalid['lab_category'] == 'glucose_serum']['validation_status'].iloc[0]
    assert glu_status == 'Invalid'

    # Empty data
    lab_obj_empty = labs()
    summary_empty = lab_obj_empty.get_reference_unit_summary()
    assert summary_empty.empty # or check columns based on implementation for no df

@pytest.mark.usefixtures("patch_lab_schema_path")
def test_get_unit_combinations_with_counts(sample_valid_labs_data, sample_invalid_labs_data_units):
    """Test get_unit_combinations_with_counts."""
    lab_obj = labs(sample_valid_labs_data)
    combinations = lab_obj.get_unit_combinations_with_counts()
    assert not combinations.empty
    assert 'count' in combinations.columns
    assert 'validation_status' in combinations.columns
    assert combinations[combinations['lab_category'] == 'glucose_serum']['validation_status'].iloc[0] == 'Valid'

    lab_obj_invalid = labs(sample_invalid_labs_data_units)
    combinations_invalid = lab_obj_invalid.get_unit_combinations_with_counts()
    assert combinations_invalid[combinations_invalid['lab_category'] == 'glucose_serum']['validation_status'].iloc[0] == 'Invalid unit'

@pytest.mark.usefixtures("patch_lab_schema_path")
def test_get_validation_summary_stats(sample_valid_labs_data, sample_invalid_labs_data_units):
    """Test get_validation_summary_stats."""
    # Valid data
    lab_obj_valid = labs(sample_valid_labs_data)
    stats_valid = lab_obj_valid.get_validation_summary_stats()
    assert stats_valid['total_combinations'] == 3
    assert stats_valid['valid_combinations'] == 3
    assert stats_valid['invalid_combinations'] == 0
    assert stats_valid['total_rows'] == 3
    assert stats_valid['valid_rows'] == 3
    assert stats_valid['row_validation_rate'] == 100.0

    # Invalid unit data
    lab_obj_invalid = labs(sample_invalid_labs_data_units)
    stats_invalid = lab_obj_invalid.get_validation_summary_stats()
    assert stats_invalid['total_combinations'] == 2
    assert stats_invalid['valid_combinations'] == 0
    assert stats_invalid['invalid_combinations'] == 2
    assert stats_invalid['total_rows'] == 2
    assert stats_invalid['valid_rows'] == 0
    assert stats_invalid['row_validation_rate'] == 0.0

    # Empty data
    lab_obj_empty = labs()
    stats_empty = lab_obj_empty.get_validation_summary_stats()
    assert stats_empty == {} # or specific empty state based on implementation

@pytest.mark.usefixtures("patch_lab_schema_path")
def test_labs_init_with_data_missing_unit_columns(patch_lab_schema_path):
    """Test labs initialization when lab_category or reference_unit columns are missing."""
    data_missing_cols = pd.DataFrame({
        'patient_id': ['P001'],
        'encounter_id': ['E001'],
        'lab_id': ['L001'],
        'lab_dttm': pd.to_datetime(['2023-01-01 10:00']),
        'lab_name': ['Glucose'],
        'lab_code': ['GLU'],
        'lab_system_name': ['SNOMED'],
        # 'lab_category': ['CHEMISTRY'], # Missing
        'lab_value': [100.0],
        # 'reference_unit': ['mg/dL'], # Missing
        'reference_range_low': [70.0],
        'reference_range_high': [110.0]
    })
    lab_obj = labs(data_missing_cols)
    lab_obj.validate() # Ensure validate is called
    assert lab_obj.isvalid() is False # Schema validation will fail
    assert any(e['type'] == 'missing_columns' for e in lab_obj.errors) # Schema error
    # Unit validation might also report missing columns for its specific check
    assert any(e.get('error_type') == 'missing_columns' for e in lab_obj.unit_validation_errors)
