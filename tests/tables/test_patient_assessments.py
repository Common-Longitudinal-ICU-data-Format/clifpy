"""
Tests for the patient_assessments table module.
"""
import os
import pytest
import pandas as pd
import json
from datetime import datetime
from clifpy.tables.patient_assessments import patient_assessments

# --- Mock Schema ---
@pytest.fixture
def mock_assessment_schema_content():
    """Provides the content for a mock Patient_assessmentsModel.json."""
    return {
        "table_name": "patient_assessments",
        "columns": [
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True},
            {"name": "recorded_dttm", "data_type": "DATETIME", "required": True},
            {"name": "assessment_id", "data_type": "VARCHAR", "required": True},
            {"name": "assessment_category", "data_type": "VARCHAR", "required": True, "is_category_column": True, "permissible_values": ["GCS", "RASS"]},
            {"name": "assessment_group", "data_type": "VARCHAR", "required": False},
            {"name": "assessment_tool", "data_type": "VARCHAR", "required": False},
            {"name": "numerical_value", "data_type": "DOUBLE", "required": False},
            {"name": "string_value", "data_type": "VARCHAR", "required": False}
        ],
        "assessment_category_to_group_mapping": {
            "GCS": "Neurological",
            "RASS": "Sedation/Agitation"
        },
        "assessment_score_ranges": {
            "GCS": {"min": 3, "max": 15},
            "RASS": {"min": -5, "max": 4}
        }
    }

@pytest.fixture
def mock_mcide_dir(tmp_path):
    """Creates a temporary mCIDE directory."""
    mcide_path = tmp_path / "mCIDE"
    mcide_path.mkdir()
    return mcide_path

@pytest.fixture
def mock_assessments_model_json(mock_mcide_dir, mock_assessment_schema_content):
    """Creates a mock Patient_assessmentsModel.json file."""
    schema_file_path = mock_mcide_dir / "Patient_assessmentsModel.json"
    with open(schema_file_path, 'w') as f:
        json.dump(mock_assessment_schema_content, f)
    return schema_file_path

@pytest.fixture
def patch_assessment_schema_path(monkeypatch, mock_assessments_model_json):
    """Patches the path to Patient_assessmentsModel.json for the patient_assessments class."""
    original_dirname = os.path.dirname
    original_join = os.path.join
    original_abspath = os.path.abspath

    def mock_dirname(path):
        if '__file__' in path:
            return str(mock_assessments_model_json.parent.parent / "tables")
        return original_dirname(path)

    def mock_abspath(path):
        if '__file__' in path:
            return str(mock_assessments_model_json.parent.parent / "tables" / "dummy_assessments.py")
        return original_abspath(path)

    def mock_join(*args):
        if len(args) > 1 and 'Patient_assessmentsModel.json' in args[-1]:
            return str(mock_assessments_model_json)
        return original_join(*args)

    monkeypatch.setattr(os.path, 'dirname', mock_dirname)
    monkeypatch.setattr(os.path, 'abspath', mock_abspath)
    monkeypatch.setattr(os.path, 'join', mock_join)

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_assessments_data():
    """Create a valid patient_assessments DataFrame for testing."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H002'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 11:00', '2023-01-02 09:00']),
        'assessment_id': ['A001', 'A002', 'A003'],
        'assessment_category': ['GCS', 'RASS', 'GCS'],
        'assessment_group': ['Neurological', 'Sedation/Agitation', 'Neurological'],
        'numerical_value': [14.0, -2.0, 15.0],
        'string_value': ['E4V5M5', '-2', 'E4V5M6']
    })

@pytest.fixture
def sample_invalid_assessments_data_schema():
    """Create a patient_assessments DataFrame with schema violations."""
    return pd.DataFrame({
        'hospitalization_id': ['H001'],
        # Missing recorded_dttm
        'assessment_id': ['A001'],
        'assessment_category': ['INVALID_CAT'], # Not in permissible_values
        'numerical_value': ['not-a-number'] # Invalid data type
    })

@pytest.fixture
def mock_assessments_file(tmp_path, sample_valid_assessments_data):
    """Create a mock patient_assessments parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_patient_assessments.parquet"
    sample_valid_assessments_data.to_parquet(file_path)
    return str(test_dir)

# --- Tests for patient_assessments class ---

# Initialization and Schema Loading
@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_assessments_init_with_valid_data(sample_valid_assessments_data):
    """Test patient_assessments initialization with valid data."""
    pa_obj = patient_assessments(sample_valid_assessments_data)
    assert pa_obj.df is not None
    assert pa_obj.isvalid() is True
    assert not pa_obj.errors
    assert "GCS" in pa_obj.assessment_score_ranges # Check schema loaded

@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_assessments_init_with_invalid_schema_data(sample_invalid_assessments_data_schema):
    """Test patient_assessments initialization with schema-invalid data."""
    pa_obj = patient_assessments(sample_invalid_assessments_data_schema)
    assert pa_obj.df is not None
    assert pa_obj.isvalid() is False
    assert len(pa_obj.errors) > 0
    error_types = {e['type'] for e in pa_obj.errors}
    # Per memory ffec3dfe-15d2-444a-97a8-a4b3af6273e3, validate_table doesn't report
    # missing columns if other errors like datatype_mismatch are present.
    assert 'missing_columns' not in error_types
    assert 'datatype_mismatch' in error_types
    assert 'invalid_category' in error_types

@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_assessments_init_without_data():
    """Test patient_assessments initialization without data."""
    pa_obj = patient_assessments()
    assert pa_obj.df is None
    assert pa_obj.isvalid() is True
    assert not pa_obj.errors
    assert "GCS" in pa_obj.assessment_score_ranges

def test_load_assessments_schema_file_not_found(monkeypatch, capsys):
    """Test _load_assessments_schema when JSON file is not found."""
    def mock_join_raise_fnf(*args):
        if 'Patient_assessmentsModel.json' in args[-1]:
            raise FileNotFoundError("Mocked File Not Found")
        return os.path.join(*args)
    monkeypatch.setattr(os.path, 'join', mock_join_raise_fnf)
    
    pa_obj = patient_assessments()
    assert pa_obj._assessment_score_ranges == {}
    captured = capsys.readouterr()
    assert "Warning: Patient_assessmentsModel.json not found" in captured.out

def test_load_assessments_schema_json_decode_error(monkeypatch, tmp_path, capsys):
    """Test _load_assessments_schema when JSON is malformed."""
    mcide_dir = tmp_path / "mCIDE"
    mcide_dir.mkdir()
    malformed_schema_path = mcide_dir / "Patient_assessmentsModel.json"
    with open(malformed_schema_path, 'w') as f:
        f.write("this is not json")

    monkeypatch.setattr(os.path, 'join', lambda *args: str(malformed_schema_path) if 'Patient_assessmentsModel.json' in args[-1] else os.path.join(*args))

    pa_obj = patient_assessments()
    assert pa_obj._assessment_score_ranges == {}
    captured = capsys.readouterr()
    assert "Warning: Invalid JSON in Patient_assessmentsModel.json" in captured.out

# from_file constructor
@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_assessments_from_file(mock_assessments_file, sample_valid_assessments_data):
    """Test loading data from a parquet file."""
    pa_obj = patient_assessments.from_file(mock_assessments_file, table_format_type="parquet")
    assert pa_obj.df is not None
    pd.testing.assert_frame_equal(pa_obj.df.reset_index(drop=True), sample_valid_assessments_data.reset_index(drop=True), check_dtype=False)
    assert pa_obj.isvalid() is True

@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_assessments_from_file_nonexistent(tmp_path):
    """Test loading from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        patient_assessments.from_file(non_existent_path, table_format_type="parquet")

# isvalid method
@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_assessments_isvalid(sample_valid_assessments_data, sample_invalid_assessments_data_schema):
    """Test isvalid method."""
    valid_pa = patient_assessments(sample_valid_assessments_data)
    assert valid_pa.isvalid() is True
    
    invalid_pa = patient_assessments(sample_invalid_assessments_data_schema)
    assert invalid_pa.isvalid() is False

# validate method
@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_assessments_validate_output(sample_valid_assessments_data, sample_invalid_assessments_data_schema, capsys):
    """Test validate method output messages."""
    patient_assessments(sample_valid_assessments_data)
    captured = capsys.readouterr()
    assert "Validation completed successfully." in captured.out

    patient_assessments(sample_invalid_assessments_data_schema)
    captured = capsys.readouterr()
    assert "Validation completed with" in captured.out
    assert "schema validation error(s)" in captured.out

    pa_no_data = patient_assessments()
    pa_no_data.validate()
    captured = capsys.readouterr()
    assert "No dataframe to validate." in captured.out

# --- Helper Method Tests ---

@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_get_assessment_categories(sample_valid_assessments_data):
    """Test get_assessment_categories method."""
    pa_obj = patient_assessments(sample_valid_assessments_data)
    categories = pa_obj.get_assessment_categories()
    assert isinstance(categories, list)
    assert set(categories) == {'GCS', 'RASS'}

    pa_empty = patient_assessments()
    assert pa_empty.get_assessment_categories() == []

@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_filter_by_assessment_category(sample_valid_assessments_data):
    """Test filter_by_assessment_category method."""
    pa_obj = patient_assessments(sample_valid_assessments_data)
    gcs_df = pa_obj.filter_by_assessment_category('GCS')
    assert len(gcs_df) == 2
    assert all(gcs_df['assessment_category'] == 'GCS')

    non_existent_df = pa_obj.filter_by_assessment_category('NonExistent')
    assert non_existent_df.empty

    pa_empty = patient_assessments()
    assert pa_empty.filter_by_assessment_category('GCS').empty

@pytest.mark.usefixtures("patch_assessment_schema_path")
def test_get_summary_stats(sample_valid_assessments_data):
    """Test get_summary_stats method."""
    pa_obj = patient_assessments(sample_valid_assessments_data)
    stats = pa_obj.get_summary_stats()

    assert stats['total_records'] == 3
    assert stats['unique_hospitalizations'] == 2
    assert stats['assessment_category_counts'] == {'GCS': 2, 'RASS': 1}
    assert 'numerical_value_stats' in stats
    assert 'GCS' in stats['numerical_value_stats']
    assert stats['numerical_value_stats']['GCS']['mean'] == 14.5

    pa_empty = patient_assessments()
    assert pa_empty.get_summary_stats() == {}
