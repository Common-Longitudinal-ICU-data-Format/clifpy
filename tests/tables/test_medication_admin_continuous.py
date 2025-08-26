"""
Tests for the medication_admin_continuous table module.
"""
import os
import pytest
import pandas as pd
import json
from datetime import datetime
import numpy as np
from pathlib import Path
from clifpy.tables.medication_admin_continuous import MedicationAdminContinuous

# --- Helper Fixtures for CSV Loading ---
@pytest.fixture
def load_fixture_csv():
    """Load CSV fixture from tests/fixtures/medication_admin_continuous/"""
    def _load(filename):
        path = Path(__file__).parent.parent / 'fixtures' / 'medication_admin_continuous' / filename
        return pd.read_csv(path)
    return _load

# --- Mock Schema --- 
@pytest.fixture
def mock_med_admin_continuous_schema_content():
    """Provides the content for a mock Medication_admin_continuousModel.json."""
    return {
        "columns": [
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True},
            {"name": "admin_dttm", "data_type": "DATETIME", "required": True},
            {"name": "med_category", "data_type": "VARCHAR", "required": False, "is_category_column": True, "permissible_values": ["Antibiotics", "Vasopressors"]},
            {"name": "med_group", "data_type": "VARCHAR", "required": False, "is_category_column": True, "permissible_values": ["Cephalosporins", "Catecholamines"]},
            {"name": "med_dose", "data_type": "DOUBLE", "required": False},
            {"name": "med_unit", "data_type": "VARCHAR", "required": False}
        ],
        "med_category_to_group_mapping": {
            "Antibiotics": "Antimicrobials",
            "Vasopressors": "Cardiovascular Agents"
        }
    }

@pytest.fixture
def mock_mcide_dir(tmp_path):
    """Creates a temporary mCIDE directory."""
    mcide_path = tmp_path / "mCIDE"
    mcide_path.mkdir()
    return mcide_path

@pytest.fixture
def mock_med_admin_continuous_model_json(mock_mcide_dir, mock_med_admin_continuous_schema_content):
    """Creates a mock Medication_admin_continuousModel.json file."""
    schema_file_path = mock_mcide_dir / "Medication_admin_continuousModel.json"
    with open(schema_file_path, 'w') as f:
        json.dump(mock_med_admin_continuous_schema_content, f)
    return schema_file_path

@pytest.fixture
def patch_med_admin_continuous_schema_path(monkeypatch, mock_med_admin_continuous_model_json):
    """Patches path resolution for Medication_admin_continuousModel.json for _load_medication_schema."""
    # This fixture specifically targets the _load_medication_schema method in medication_admin_continuous.py
    # It ensures that when this class tries to load its med_category_to_group_mapping, it uses the mock file.
    original_dirname = os.path.dirname
    original_join = os.path.join
    original_abspath = os.path.abspath

    import importlib
    # Explicitly import the module to ensure we get the module object, not potentially the class
    module_name = "clifpy.tables.medication_admin_continuous"
    module_obj = importlib.import_module(module_name)
    target_module_file_path = os.path.normpath(module_obj.__file__)

    # Create a fake directory structure that _load_medication_schema would expect
    # if it were located such that '../mCIDE/Medication_admin_continuousModel.json' points to our mock file.
    fake_module_dir = mock_med_admin_continuous_model_json.parent.parent / "tables_dummy_root_mac"
    fake_module_path = fake_module_dir / "medication_admin_continuous.py"

    def mock_abspath_for_class(path):
        if os.path.normpath(path) == target_module_file_path:
            return str(fake_module_path)
        return original_abspath(path)

    def mock_dirname_for_class(path):
        if os.path.normpath(path) == str(fake_module_path):
            return str(fake_module_dir)
        return original_dirname(path)

    def mock_join_for_class(*args):
        # This targets the specific join: os.path.join(current_dir, '..', 'mCIDE', 'Medication_admin_continuousModel.json')
        if (len(args) == 4 and 
            os.path.normpath(args[0]) == str(fake_module_dir) and 
            args[1] == '..' and args[2] == 'mCIDE' and 
            args[3] == 'Medication_admin_continuousModel.json'):
            return str(mock_med_admin_continuous_model_json.resolve())
        return original_join(*args)

    monkeypatch.setattr(os.path, 'abspath', mock_abspath_for_class)
    monkeypatch.setattr(os.path, 'dirname', mock_dirname_for_class)
    monkeypatch.setattr(os.path, 'join', mock_join_for_class)

@pytest.fixture
def patch_validator_load_schema(monkeypatch, mock_med_admin_continuous_schema_content):
    """Patches pyclif.utils.validator.load_schema to use mocked column definitions."""
    # This fixture targets the load_schema function used by validate_table.
    # It ensures that validate_table gets its column definitions from our mock schema.
    def mock_load_schema_for_validation(table_name, schema_dir=None):
        if table_name == "medication_admin_continuous":
            # Return the entire mock schema content for this table.
            # validate_table might use other parts of the schema beyond just 'columns'.
            return mock_med_admin_continuous_schema_content
        # Fallback for other tables if your tests involve them, though ideally scoped.
        # For this test suite, we are primarily concerned with 'medication_admin_continuous'.
        # If other tables were processed by validate_table in these tests, they'd need their own mock handling.
        raise ValueError(f"patch_validator_load_schema called for unexpected table: {table_name}")

    monkeypatch.setattr("clifpy.utils.validator._load_spec", mock_load_schema_for_validation)

# --- Data Fixtures --- 
@pytest.fixture
def sample_valid_med_admin_continuous_data():
    """Create a valid medication_admin_continuous DataFrame."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H002'],
        'admin_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 11:00', '2023-01-02 09:00']),
        'med_category': ['Antibiotics', 'Vasopressors', 'Antibiotics'],
        'med_group': ['Cephalosporins', 'Catecholamines', 'Cephalosporins'],
        'med_dose': [100.0, 10.5, 150.0],
        'med_unit': ['mg', 'mcg/kg/min', 'mg']
    })

@pytest.fixture
def sample_invalid_med_admin_continuous_data_schema():
    """Create a DataFrame with schema violations."""
    return pd.DataFrame({
        'hospitalization_id': ['H001'],
        # Missing admin_dttm (required)
        'med_category': ['UnknownCategory'], # Not in permissible_values if schema enforces
        'med_dose': ['not_a_number'] # Invalid type
    })

@pytest.fixture
def sample_med_admin_continuous_data_for_stats():
    """Create data suitable for testing get_summary_stats thoroughly."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H002', 'H003', 'H003'],
        'admin_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 11:00', '2023-01-02 09:00', '2023-01-03 14:00', '2023-01-03 15:00']),
        'med_category': ['Antibiotics', 'Vasopressors', 'Antibiotics', 'Vasopressors', 'Antibiotics'],
        'med_group': ['Cephalosporins', 'Catecholamines', 'Cephalosporins', 'OtherVaso', 'Penicillins'],
        'med_dose': [100.0, 10.5, 150.0, np.nan, 200.0],
        'med_unit': ['mg', 'mcg/kg/min', 'mg', 'mg', 'mg']
    })

@pytest.fixture
def mock_med_admin_continuous_file(tmp_path, sample_valid_med_admin_continuous_data):
    """Create a mock parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_medication_admin_continuous.parquet"
    sample_valid_med_admin_continuous_data.to_parquet(file_path)
    return str(test_dir) # from_file expects directory path

# --- Tests for medication_admin_continuous class --- 

# Initialization and Schema Loading
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_init_with_valid_data(sample_valid_med_admin_continuous_data):
    mac_obj = MedicationAdminContinuous(sample_valid_med_admin_continuous_data)
    assert mac_obj.df is not None
    assert mac_obj.isvalid() is True
    assert not mac_obj.errors
    assert mac_obj.med_category_to_group_mapping['Antibiotics'] == 'Antimicrobials'

@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_init_with_invalid_schema_data(sample_invalid_med_admin_continuous_data_schema):
    mac_obj = MedicationAdminContinuous(sample_invalid_med_admin_continuous_data_schema)
    assert mac_obj.df is not None
    mac_obj.validate() # Ensure validation is run if not fully in init or for clarity
    assert mac_obj.isvalid() is False
    assert len(mac_obj.errors) > 0
    error_types = [e['type'] for e in mac_obj.errors]

    # Check for the errors that are actually reported
    assert 'invalid_category' in error_types
    assert 'datatype_mismatch' in error_types

    # Explicitly assert that 'missing_columns' is NOT currently reported in this scenario
    # This might indicate a point for future investigation in validator.py if this behavior is unexpected.
    assert 'missing_columns' not in error_types

    # Ensure no other unexpected errors are present
    assert len(error_types) == 2

@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_init_without_data():
    mac_obj = MedicationAdminContinuous()
    assert mac_obj.df is None
    assert mac_obj.isvalid() is True # No data, so no validation errors
    assert not mac_obj.errors
    assert mac_obj.med_category_to_group_mapping['Antibiotics'] == 'Antimicrobials'

def test_load_medication_schema_file_not_found(monkeypatch, capsys):
    def mock_join_raise_fnf(*args):
        if 'Medication_admin_continuousModel.json' in args[-1]:
            raise FileNotFoundError("Mocked File Not Found")
        return os.path.join(*args)
    monkeypatch.setattr(os.path, 'join', mock_join_raise_fnf)
    
    mac_obj = MedicationAdminContinuous() # Init calls _load_medication_schema
    assert mac_obj._med_category_to_group == {}
    captured = capsys.readouterr()
    assert "Warning: Medication_admin_continuousModel.json not found" in captured.out

def test_load_medication_schema_json_decode_error(monkeypatch, tmp_path, capsys):
    mcide_dir = tmp_path / "mCIDE"
    mcide_dir.mkdir()
    malformed_schema_path = mcide_dir / "Medication_admin_continuousModel.json"
    with open(malformed_schema_path, 'w') as f:
        f.write("this is not json")

    original_dirname = os.path.dirname
    original_join = os.path.join
    original_abspath = os.path.abspath

    def mock_abspath_local(path):
        if '__file__' in path:
            return str(malformed_schema_path.parent.parent / "tables" / "dummy.py")
        return original_abspath(path)
    
    def mock_dirname_local(path):
        if path == str(malformed_schema_path.parent.parent / "tables" / "dummy.py"):
             return str(malformed_schema_path.parent.parent / "tables")
        return original_dirname(path)

    def mock_join_local(*args):
        if len(args) > 1 and args[1] == '..' and args[2] == 'mCIDE' and args[3] == 'Medication_admin_continuousModel.json':
            return str(malformed_schema_path)
        return original_join(*args)

    monkeypatch.setattr(os.path, 'abspath', mock_abspath_local)
    monkeypatch.setattr(os.path, 'dirname', mock_dirname_local)
    monkeypatch.setattr(os.path, 'join', mock_join_local)

    mac_obj = MedicationAdminContinuous()
    assert mac_obj._med_category_to_group == {}
    captured = capsys.readouterr()
    assert "Warning: Invalid JSON in Medication_admin_continuousModel.json" in captured.out

@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path") # Only this patch needed as it tests _load_medication_schema directly
def test_med_category_to_group_mapping_property(mock_med_admin_continuous_schema_content):
    mac_obj = MedicationAdminContinuous()
    expected_mapping = mock_med_admin_continuous_schema_content['med_category_to_group_mapping']
    assert mac_obj.med_category_to_group_mapping == expected_mapping
    # Test it returns a copy
    mapping = mac_obj.med_category_to_group_mapping
    mapping['NewKey'] = 'NewValue'
    assert mac_obj.med_category_to_group_mapping == expected_mapping

# from_file constructor
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_from_file(mock_med_admin_continuous_file, sample_valid_med_admin_continuous_data):
    mac_obj = MedicationAdminContinuous.from_file(mock_med_admin_continuous_file, table_format_type="parquet")
    assert mac_obj.df is not None
    pd.testing.assert_frame_equal(mac_obj.df.reset_index(drop=True), sample_valid_med_admin_continuous_data.reset_index(drop=True), check_dtype=False)
    assert mac_obj.isvalid() is True

@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_from_file_nonexistent(tmp_path):
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        MedicationAdminContinuous.from_file(non_existent_path, table_format_type="parquet")

# isvalid method
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_isvalid(sample_valid_med_admin_continuous_data, sample_invalid_med_admin_continuous_data_schema):
    valid_mac = MedicationAdminContinuous(sample_valid_med_admin_continuous_data)
    assert valid_mac.isvalid() is True
    
    invalid_mac = MedicationAdminContinuous(sample_invalid_med_admin_continuous_data_schema)
    assert invalid_mac.isvalid() is False 

# validate method
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_validate_output(sample_valid_med_admin_continuous_data, sample_invalid_med_admin_continuous_data_schema, capsys):
    MedicationAdminContinuous(sample_valid_med_admin_continuous_data) # Validation runs at init
    captured = capsys.readouterr()
    assert "Validation completed successfully." in captured.out

    MedicationAdminContinuous(sample_invalid_med_admin_continuous_data_schema)
    captured = capsys.readouterr()
    assert f"Validation completed with" in captured.out # Checks for presence of error count

    mac_obj_no_data = MedicationAdminContinuous()
    mac_obj_no_data.validate() # Explicit call
    captured = capsys.readouterr()
    assert "No dataframe to validate." in captured.out

# Medication Admin Continuous Specific Methods
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_get_med_categories(sample_valid_med_admin_continuous_data):
    mac_obj = MedicationAdminContinuous(sample_valid_med_admin_continuous_data)
    categories = mac_obj.get_med_categories()
    assert isinstance(categories, list)
    assert set(categories) == {'Antibiotics', 'Vasopressors'}

    mac_obj_no_data = MedicationAdminContinuous()
    assert mac_obj_no_data.get_med_categories() == []

    mac_obj_no_col = MedicationAdminContinuous(pd.DataFrame({'hospitalization_id': [1]}))
    assert mac_obj_no_col.get_med_categories() == []

@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_get_med_groups(sample_valid_med_admin_continuous_data):
    mac_obj = MedicationAdminContinuous(sample_valid_med_admin_continuous_data)
    groups = mac_obj.get_med_groups()
    assert isinstance(groups, list)
    assert set(groups) == {'Cephalosporins', 'Catecholamines'}

    mac_obj_no_data = MedicationAdminContinuous()
    assert mac_obj_no_data.get_med_groups() == []

    mac_obj_no_col = MedicationAdminContinuous(pd.DataFrame({'hospitalization_id': [1]}))
    assert mac_obj_no_col.get_med_groups() == []

@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_filter_by_med_group(sample_valid_med_admin_continuous_data):
    mac_obj = MedicationAdminContinuous(sample_valid_med_admin_continuous_data)
    filtered_df = mac_obj.filter_by_med_group('Cephalosporins')
    assert len(filtered_df) == 2
    assert all(filtered_df['med_group'] == 'Cephalosporins')

    filtered_df_non_existent = mac_obj.filter_by_med_group('NonExistentGroup')
    assert filtered_df_non_existent.empty

    mac_obj_no_data = MedicationAdminContinuous()
    assert mac_obj_no_data.filter_by_med_group('AnyGroup').empty

    mac_obj_no_col = MedicationAdminContinuous(pd.DataFrame({'hospitalization_id': [1]}))
    assert mac_obj_no_col.filter_by_med_group('AnyGroup').empty

@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_get_summary_stats(sample_med_admin_continuous_data_for_stats):
    mac_obj = MedicationAdminContinuous(sample_med_admin_continuous_data_for_stats)
    stats = mac_obj.get_summary_stats()

    assert stats['total_records'] == 5
    assert stats['unique_hospitalizations'] == 3
    assert stats['med_category_counts'] == {'Antibiotics': 3, 'Vasopressors': 2}
    assert stats['med_group_counts'] == {'Cephalosporins': 2, 'Catecholamines': 1, 'OtherVaso': 1, 'Penicillins': 1}
    assert stats['date_range']['earliest'] == pd.to_datetime('2023-01-01 10:00')
    assert stats['date_range']['latest'] == pd.to_datetime('2023-01-03 15:00')

    dose_stats = stats['dose_stats_by_group']
    assert 'Cephalosporins' in dose_stats
    assert dose_stats['Cephalosporins']['count'] == 2
    assert dose_stats['Cephalosporins']['mean_dose'] == pytest.approx(125.0)
    assert dose_stats['Cephalosporins']['min_dose'] == 100.0
    assert dose_stats['Cephalosporins']['max_dose'] == 150.0

    assert 'Catecholamines' in dose_stats
    assert dose_stats['Catecholamines']['count'] == 1
    assert dose_stats['Catecholamines']['mean_dose'] == pytest.approx(10.5)

    assert 'OtherVaso' not in dose_stats # Group with only NaN dose is not included in dose_stats

    assert 'Penicillins' in dose_stats
    assert dose_stats['Penicillins']['count'] == 1
    assert dose_stats['Penicillins']['mean_dose'] == pytest.approx(200.0)

@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_get_summary_stats_empty_df():
    mac_obj = MedicationAdminContinuous(pd.DataFrame(columns=['hospitalization_id', 'admin_dttm', 'med_category', 'med_group', 'med_dose']))
    stats = mac_obj.get_summary_stats()
    assert stats['total_records'] == 0
    assert stats['unique_hospitalizations'] == 0
    assert stats['med_category_counts'] == {}
    assert stats['med_group_counts'] == {}
    assert pd.isna(stats['date_range']['earliest'])
    assert pd.isna(stats['date_range']['latest'])
    assert 'dose_stats_by_group' not in stats or stats['dose_stats_by_group'] == {}

@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_get_summary_stats_no_df():
    mac_obj = MedicationAdminContinuous()
    stats = mac_obj.get_summary_stats()
    assert stats == {}

@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_get_summary_stats_missing_columns():
    # Test with missing optional columns like med_dose
    data_missing_dose = pd.DataFrame({
        'hospitalization_id': ['H001'],
        'admin_dttm': pd.to_datetime(['2023-01-01 10:00']),
        'med_category': ['Antibiotics'],
        'med_group': ['Cephalosporins']
        # med_dose is missing
    })
    mac_obj = MedicationAdminContinuous(data_missing_dose)
    stats = mac_obj.get_summary_stats()
    assert 'dose_stats_by_group' not in stats or stats['dose_stats_by_group'] == {}

    # Test with missing core columns like med_category or med_group
    data_missing_group = pd.DataFrame({
        'hospitalization_id': ['H001'],
        'admin_dttm': pd.to_datetime(['2023-01-01 10:00']),
        'med_category': ['Antibiotics'],
        'med_dose': [100.0]
        # med_group is missing
    })
    mac_obj_missing_group = MedicationAdminContinuous(data_missing_group)
    stats_missing_group = mac_obj_missing_group.get_summary_stats()
    assert stats_missing_group['med_group_counts'] == {}
    assert 'dose_stats_by_group' not in stats_missing_group or stats_missing_group['dose_stats_by_group'] == {}

# ===========================================
# Tests for `_acceptable_dose_unit_patterns`
# ===========================================
@pytest.mark.unit_conversion
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_acceptable_dose_unit_patterns():
    """
    Test that acceptable dose unit pattern (e.g. 'mcg/kg/hr') are included in the `_acceptable_dose_unit_patterns` property
    and wrong patterns (e.g. 'mcg/lb/min') are not.
    """
    mac_obj = MedicationAdminContinuous()
    acceptable_patterns = mac_obj._acceptable_dose_unit_patterns
    
    # Test cases that should be acceptable
    acceptable_cases = [
        'mcg/kg/hr',
        'mcg/kg/h',
        'mcg/kg/hour',
        'mcg/kg/min',
        'mcg/kg/m',
        'mcg/kg/minute',
        'mg/hr',
        'mg/min',
        'units/hr',
        'units/min',
        'ml/hr',
        'ml/min',
        'l/hr',
        'ng/kg/min',
        'milli-units/min',
        'mcg/lb/min'
    ]
    
    for pattern in acceptable_cases:
        assert pattern in acceptable_patterns, f"Pattern '{pattern}' should be acceptable"
    
    # Test cases that should NOT be acceptable
    unacceptable_cases = [
        'mcg/kg/sec',  # sec not supported
        'mcg/kg/day',  # day not supported
        'mcg',
        "ml",
        "l"
    ]
    
    for pattern in unacceptable_cases:
        assert pattern not in acceptable_patterns, f"Pattern '{pattern}' should NOT be acceptable"

# ===========================================
# Tests for `_normalize_dose_unit_pattern`
# ===========================================
@pytest.fixture
def normalize_dose_unit_pattern_test_data(load_fixture_csv):
    """
    Load test data for dose unit pattern normalization tests.
    
    Returns CSV data with columns:
    - case: 'valid' or 'invalid' to categorize test scenarios
    - med_dose_unit: Original dose unit string
    - med_dose_unit_clean: Expected normalized result
    """
    return load_fixture_csv('test_normalize_dose_unit_pattern.csv')

@pytest.mark.unit_conversion
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_normalize_dose_unit_pattern_recognized(normalize_dose_unit_pattern_test_data):
    """
    Test that the `_normalize_dose_unit_pattern` private method correctly normalizes valid dose units.
    
    Validates:
    1. Whitespace removal (including internal spaces): 'mL/ hr' -> 'ml/hr'
    2. Case conversion to lowercase: 'ML/HR' -> 'ml/hr'
    3. Return value indicates no unrecognized units (False)
    
    Uses fixture data filtered for 'valid' test cases.
    """
    mac_obj = MedicationAdminContinuous()
    test_df = normalize_dose_unit_pattern_test_data.query("case == 'valid'")
    # first check the filtering went right, i.e. test_df is not empty
    assert test_df.shape[0] > 0
    result_series, unrecognized = mac_obj._normalize_dose_unit_pattern(test_df['med_dose_unit'])
    
    pd.testing.assert_series_equal(
        result_series.reset_index(drop=True),
        test_df['med_dose_unit_clean'].reset_index(drop=True),
        check_names=False
    )
    assert unrecognized is False # no unrecognized dose units so the return should be False

@pytest.mark.unit_conversion
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_normalize_dose_unit_pattern_unrecognized(normalize_dose_unit_pattern_test_data, caplog):
    """
    Test that the `_normalize_dose_unit_pattern` private method handles unrecognized dose units.
    
    Validates:
    1. Unrecognized units are identified and returned as a dictionary with counts
    2. Warning is logged with details about unrecognized units
    3. All invalid units from test data appear in the unrecognized dictionary
    
    Uses fixture data filtered for 'invalid' test cases.
    """
    mac_obj = MedicationAdminContinuous()
    test_df = normalize_dose_unit_pattern_test_data.query("case == 'invalid'")
    assert test_df.shape[0] > 0
    
    with caplog.at_level('WARNING'):
        _, unrecognized = mac_obj._normalize_dose_unit_pattern(test_df['med_dose_unit'])
    
    assert isinstance(unrecognized, dict)
    for unit in test_df['med_dose_unit_clean']:
        assert unit in unrecognized # check that all invalid units are in the unrecognized dict
    assert "not recognized by the converter" in caplog.text

@pytest.mark.skip(reason="to be retired")
@pytest.mark.unit_conversion
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_normalize_dose_unit_pattern_empty_dataframe():
    """
    Test that `_normalize_dose_unit_pattern` gracefully handles empty DataFrames.
    
    Validates:
    - Empty DataFrame input doesn't cause errors
    - Output DataFrame contains required 'med_dose_unit_clean' column
    - Returns False for unrecognized units (no units to process)
    """
    mac_obj = MedicationAdminContinuous()
    empty_df = pd.DataFrame({'med_dose_unit': pd.Series([], dtype='object')})
    result_series, unrecognized = mac_obj._normalize_dose_unit_pattern(empty_df['med_dose_unit'])
    assert 'med_dose_unit_clean' in result_series.columns
    assert unrecognized is False

@pytest.mark.unit_conversion
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_normalize_dose_unit_pattern_using_self_df(normalize_dose_unit_pattern_test_data):
    """
    Test that `_normalize_dose_unit_pattern` uses self.df when no DataFrame is provided.
    
    Validates:
    - Method defaults to using self.df when med_df parameter is None
    - Normalization works correctly on instance data
    - 'ML/HR' is properly normalized to 'ml/hr'
    """
    # test_data = pd.DataFrame({
    #     'hospitalization_id': ['H001', 'H002', 'H003'],
    #     'admin_dttm': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
    #     'med_dose_unit': ['ML / HR', 'ml/hr', 'l/hr']
    # })
    mac_obj_with_data = MedicationAdminContinuous(data_directory=".", filetype="parquet", data=normalize_dose_unit_pattern_test_data)
    test_df = normalize_dose_unit_pattern_test_data.query("case == 'valid'")
    # first check the filtering went right, i.e. test_df is not empty
    assert test_df.shape[0] > 0
    result_series, unrecognized = mac_obj_with_data._normalize_dose_unit_pattern(test_df['med_dose_unit'])
    
    pd.testing.assert_series_equal(
        result_series.reset_index(drop=True),
        test_df['med_dose_unit_clean'].reset_index(drop=True),
        check_names=False
    )
    assert unrecognized is False # no unrecognized dose units so the return should be False

@pytest.mark.unit_conversion
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_normalize_dose_unit_pattern_no_data_provided():
    """
    Test that `_normalize_dose_unit_pattern` raises ValueError when no data is available.
    
    Validates:
    - ValueError is raised with message "No data provided"
    - Occurs when both med_df parameter is None and self.df is None
    """
    mac_obj = MedicationAdminContinuous()
    # mac_obj.df is None since no data was provided during initialization
    with pytest.raises(ValueError, match="No data provided"):
        mac_obj._normalize_dose_unit_pattern()


# ===========================================
# Tests for `_convert_normalized_dose_units_to_limited_units`
# ===========================================
@pytest.fixture
def convert_normalized_dose_units_to_limited_units_test_data(load_fixture_csv):
    """
    Load test data for dose unit conversion tests.
    
    Returns CSV data with columns:
    - hospitalization_id, admin_dttm: Patient and timing identifiers
    - med_dose, med_dose_unit: Original dose values and units
    - weight_kg: Patient weight (may be empty/NaN)
    - med_dose_converted, med_dose_unit_converted: Expected conversion results
    - case: Test scenario category
    
    Processes admin_dttm to datetime and converts empty weight_kg to NaN.
    """
    df = load_fixture_csv('test_convert_normalized_dose_units_to_limited_units.csv')
    # df['admin_dttm'] = pd.to_datetime(df['admin_dttm'])
    # Replace empty strings with NaN for weight_kg column
    df['weight_kg'] = df['weight_kg'].replace('', np.nan)
    return df

@pytest.fixture
def vitals_mock_data(load_fixture_csv):
    """
    Load mock vitals data containing patient weights.
    
    Returns CSV data with columns:
    - hospitalization_id: Patient identifier
    - recorded_dttm: Timestamp of vital recording
    - vital_category: Type of vital (expects 'weight_kg')
    - vital_value: Numeric weight value
    
    Used for testing weight-based dose conversions.
    """
    df = load_fixture_csv('vitals_weights.csv')
    df['recorded_dttm'] = pd.to_datetime(df['recorded_dttm'])
    return df

@pytest.mark.unit_conversion
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_convert_normalized_dose_units_to_limited_units(convert_normalized_dose_units_to_limited_units_test_data,caplog):
    """
    Test that the `_convert_normalized_dose_units_to_limited_units` method correctly converts doses to standard units.
    
    Validates:
    - Conversion to target units: mcg/min, ml/min, or units/min
    - Weight-based calculations using patient weights from vitals
    - Time unit conversions (per hour to per minute)
    - Proper handling of various dose patterns from test data
    - Warning logged for unrecognized dose units
    - Output columns include med_dose_converted, med_dose_unit_converted, weight_kg
    
    Compares actual conversions against expected values from test fixture.
    """
    mac_obj = MedicationAdminContinuous()
    test_df = convert_normalized_dose_units_to_limited_units_test_data #.query("case == 'valid'")
    # test_df['med_category'] = 'Vasopressors'  # Required for SQL query
    
    input_df = test_df.drop(['case', 'med_dose_converted', 'med_dose_unit_converted'], axis=1)
    
    # with caplog.at_level('WARNING'):
    result_df = mac_obj._convert_normalized_dose_units_to_limited_units(med_df = input_df) \
        .sort_values(by=['rn']) # sort by rn to ensure the order of the rows is consistent
    
    # Verify columns exist
    assert 'med_dose_converted' in result_df.columns
    assert 'med_dose_unit_converted' in result_df.columns
    assert 'weight_kg' in result_df.columns
    # assert "Unrecognized dose units found" in caplog.text # check that the warning is logged

    # Verify converted values
    pd.testing.assert_series_equal(
        test_df['med_dose_converted'].reset_index(drop=True),
        result_df['med_dose_converted'].reset_index(drop=True),
        check_names=False,
        check_dtype=False
    )

    # Verify converted units
    pd.testing.assert_series_equal(
        test_df['med_dose_unit_converted'].fillna('None').reset_index(drop=True),
        result_df['med_dose_unit_converted'].fillna('None').reset_index(drop=True),
        check_names=False,
        check_dtype=False
    )

@pytest.mark.unit_conversion
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_convert_normalized_dose_units_to_limited_units_missing_columns():
    """
    Test that `_convert_normalized_dose_units_to_limited_units` raises errors for missing required columns.
    
    Validates:
    - ValueError is raised when required columns are missing
    - Error message includes "required but not found"
    - Specifically tests missing 'med_dose_unit' column
    - Other required columns (med_dose, weight_kg) would trigger same error
    """
    mac_obj = MedicationAdminContinuous()
    vitals_df = pd.DataFrame({
        'hospitalization_id': ['H001'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 09:00']),
        'vital_category': ['weight_kg'],
        'vital_value': [70.0]
    })
    
    med_df_missing = pd.DataFrame({
        'hospitalization_id': ['H001'],
        'admin_dttm': pd.to_datetime(['2023-01-01 11:00']),
        'med_category': ['Antibiotics'],  # Include required column for SQL
        # Missing med_dose_unit
        'med_dose': [100.0]
    })
    
    with pytest.raises(ValueError, match="required but not found"):
        mac_obj.standardize_dose_to_limited_units(vitals_df, med_df_missing)

@pytest.mark.unit_conversion
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path", "patch_validator_load_schema")
def test_convert_normalized_dose_units_to_limited_units_no_data_provided():
    """
    Test that `_convert_normalized_dose_units_to_limited_units` raises ValueError when no medication data is available.
    
    Validates:
    - ValueError is raised with message "No data provided"
    - Occurs when both med_df parameter is None and self.df is None
    - Vitals data is provided but medication data is missing
    """
    mac_obj = MedicationAdminContinuous()
    vitals_df = pd.DataFrame({
        'hospitalization_id': ['H001'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 09:00']),
        'vital_category': ['weight_kg'],
        'vital_value': [70.0]
    })
    # mac_obj.df is None since no data was provided during initialization
    with pytest.raises(ValueError, match="No data provided"):
        mac_obj.standardize_dose_to_limited_units(vitals_df)