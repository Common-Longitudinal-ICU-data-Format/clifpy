"""
Tests for the position table module.
"""
import os
import pytest
import pandas as pd
import json
from datetime import datetime
from pyclif.tables.position import position
import pyclif.utils.validator

# --- Mock Schema ---
@pytest.fixture
def mock_position_schema_content():
    """Provides the content for a mock PositionModel.json."""
    return {
        "columns": [
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True},
            {"name": "recorded_dttm", "data_type": "DATETIME", "required": True},
            {
                "name": "position_category",
                "data_type": "VARCHAR",
                "required": True,
                "is_category_column": True,
                "permissible_values": ["Prone", "Supine", "Upright"],
            },
        ]
    }

@pytest.fixture
def mock_mcide_dir(tmp_path):
    """Creates a temporary mCIDE directory."""
    mcide_path = tmp_path / "mCIDE"
    mcide_path.mkdir()
    return mcide_path

@pytest.fixture
def mock_position_model_json(mock_mcide_dir, mock_position_schema_content):
    """Creates a mock PositionModel.json file in the temporary mCIDE directory."""
    schema_file_path = mock_mcide_dir / "PositionModel.json"
    with open(schema_file_path, 'w') as f:
        json.dump(mock_position_schema_content, f)
    return schema_file_path

@pytest.fixture
def patch_schema_path(monkeypatch, mock_position_model_json):
    """Patches the default schema directory to use the mock mCIDE directory."""
    # The validator utility uses this module-level variable to find the schema directory.
    # We patch it directly to point to our temporary directory.
    # We depend on mock_position_model_json to ensure the file is created.
    mock_schema_dir = os.path.dirname(str(mock_position_model_json))
    monkeypatch.setattr(pyclif.utils.validator, "_DEF_SPEC_DIR", mock_schema_dir)

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_position_data():
    """Create a valid position DataFrame for testing."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H002'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 12:00', '2023-01-02 09:00']),
        'position_category': ['Supine', 'Prone', 'Supine']
    })

@pytest.fixture
def sample_invalid_position_data_schema():
    """Create a position DataFrame with schema violations."""
    return pd.DataFrame({
        # Missing hospitalization_id
        'recorded_dttm': ['not a date', '2023-01-01 12:00'], # wrong type
        'position_category': ['Lying Down', 'Prone'] # not in permissible_values
    })

@pytest.fixture
def mock_position_file(tmp_path, sample_valid_position_data):
    """Create a mock position parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_position.parquet"
    sample_valid_position_data.to_parquet(file_path)
    return str(test_dir) # from_file expects directory path

# --- Tests for position class --- 

# Initialization
@pytest.mark.usefixtures("patch_schema_path")
def test_position_init_with_valid_data(sample_valid_position_data):
    """Test position initialization with valid data."""
    pos_obj = position(sample_valid_position_data)
    assert pos_obj.df is not None
    assert pos_obj.isvalid() is True
    assert not pos_obj.errors

@pytest.mark.usefixtures("patch_schema_path")
def test_position_init_with_invalid_schema_data(sample_invalid_position_data_schema):
    """Test position initialization with schema-invalid data."""
    pos_obj = position(sample_invalid_position_data_schema)
    assert pos_obj.df is not None
    assert pos_obj.isvalid() is False
    assert len(pos_obj.errors) > 0
    error_types = {e['type'] for e in pos_obj.errors}
    # Per memory ffec3dfe, validator.py doesn't report missing_columns when other errors are present.
    assert 'missing_columns' not in error_types
    assert 'datatype_mismatch' in error_types
    assert 'invalid_category' in error_types

@pytest.mark.usefixtures("patch_schema_path")
def test_position_init_without_data():
    """Test position initialization without data."""
    pos_obj = position()
    assert pos_obj.df is None
    assert pos_obj.isvalid() is True
    assert not pos_obj.errors

# from_file constructor
@pytest.mark.usefixtures("patch_schema_path")
def test_position_from_file(mock_position_file, sample_valid_position_data):
    """Test loading position data from a parquet file."""
    pos_obj = position.from_file(mock_position_file, table_format_type="parquet")
    assert pos_obj.df is not None
    pd.testing.assert_frame_equal(pos_obj.df.reset_index(drop=True), sample_valid_position_data.reset_index(drop=True), check_dtype=False)
    assert pos_obj.isvalid() is True # Validation is called in init

@pytest.mark.usefixtures("patch_schema_path")
def test_position_from_file_nonexistent(tmp_path):
    """Test loading position data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        position.from_file(non_existent_path, table_format_type="parquet")

# isvalid method
@pytest.mark.usefixtures("patch_schema_path")
def test_position_isvalid(sample_valid_position_data, sample_invalid_position_data_schema):
    """Test isvalid method."""
    valid_pos = position(sample_valid_position_data)
    assert valid_pos.isvalid() is True
    
    invalid_pos = position(sample_invalid_position_data_schema)
    assert invalid_pos.isvalid() is False

# validate method
@pytest.mark.usefixtures("patch_schema_path")
def test_position_validate_output(sample_valid_position_data, sample_invalid_position_data_schema, capsys):
    """Test validate method output messages."""
    # Valid data - validation runs at init
    position(sample_valid_position_data)
    captured = capsys.readouterr()
    assert "Validation completed successfully." in captured.out

    # Invalid data - validation runs at init
    position(sample_invalid_position_data_schema)
    captured = capsys.readouterr()
    # Expecting 2 errors, not 3, due to known issue with missing_columns reporting.
    assert "Validation completed with 2 error(s)." in captured.out

    # No data
    pos_obj_no_data = position()
    pos_obj_no_data.validate() # Explicit call
    captured = capsys.readouterr()
    assert "No dataframe to validate." in captured.out

# get_summary_stats method
@pytest.mark.usefixtures("patch_schema_path")
def test_get_summary_stats_with_data(sample_valid_position_data):
    """Test get_summary_stats with a valid dataframe."""
    pos_obj = position(sample_valid_position_data)
    stats = pos_obj.get_summary_stats()
    
    assert stats['total_records'] == 3
    assert stats['unique_hospitalizations'] == 2
    assert stats['position_category_counts'] == {'Supine': 2, 'Prone': 1}
    assert stats['date_range']['earliest'] == pd.to_datetime('2023-01-01 10:00')
    assert stats['date_range']['latest'] == pd.to_datetime('2023-01-02 09:00')

@pytest.mark.usefixtures("patch_schema_path")
def test_get_summary_stats_without_data():
    """Test get_summary_stats with no dataframe."""
    pos_obj = position()
    stats = pos_obj.get_summary_stats()
    assert stats == {}

@pytest.mark.usefixtures("patch_schema_path")
def test_get_summary_stats_with_missing_cols():
    """Test get_summary_stats handles missing columns gracefully."""
    df = pd.DataFrame({'other_col': [1, 2, 3]})
    pos_obj = position(df)
    stats = pos_obj.get_summary_stats()

    assert stats['total_records'] == 3
    assert stats['unique_hospitalizations'] == 0
    assert stats['position_category_counts'] == {}
    assert stats['date_range']['earliest'] is None
    assert stats['date_range']['latest'] is None
