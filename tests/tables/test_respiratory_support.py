"""
Tests for the respiratory_support table module.
"""
import os
import pytest
import pandas as pd
import json
from datetime import datetime
from pyclif.tables.respiratory_support import respiratory_support

# --- Mock Schema ---
@pytest.fixture
def mock_rs_schema_content():
    """Provides the content for a mock Respiratory_supportModel.json."""
    return {
        "columns": [
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True},
            {"name": "recorded_dttm", "data_type": "DATETIME", "required": True},
            {"name": "device_category", "data_type": "VARCHAR", "is_category_column": True, "permissible_values": ["imv", "nippv", "room air", "trach collar"]},
            {"name": "device_name", "data_type": "VARCHAR"},
            {"name": "mode_category", "data_type": "VARCHAR", "is_category_column": True, "permissible_values": ["assist control-volume control", "simv", "pressure control", "pressure support/cpap"]},
            {"name": "mode_name", "data_type": "VARCHAR"},
            {"name": "tracheostomy", "data_type": "INTEGER"},
            {"name": "fio2_set", "data_type": "DOUBLE"},
            {"name": "lpm_set", "data_type": "DOUBLE"},
            {"name": "peep_set", "data_type": "DOUBLE"},
            {"name": "tidal_volume_set", "data_type": "DOUBLE"},
            {"name": "resp_rate_set", "data_type": "DOUBLE"},
            {"name": "resp_rate_obs", "data_type": "DOUBLE"},
            {"name": "pressure_support_set", "data_type": "DOUBLE"},
            {"name": "peak_inspiratory_pressure_set", "data_type": "DOUBLE"}
        ],
        "required_columns": ["hospitalization_id", "recorded_dttm"]
    }

@pytest.fixture
def mock_mcide_dir(tmp_path):
    """Creates a temporary mCIDE directory."""
    mcide_path = tmp_path / "mCIDE"
    mcide_path.mkdir()
    return mcide_path

@pytest.fixture
def mock_rs_model_json(mock_mcide_dir, mock_rs_schema_content):
    """Creates a mock Respiratory_supportModel.json file."""
    schema_file_path = mock_mcide_dir / "Respiratory_supportModel.json"
    with open(schema_file_path, 'w') as f:
        json.dump(mock_rs_schema_content, f)
    return schema_file_path

@pytest.fixture
def patch_rs_schema_path(monkeypatch, mock_rs_model_json):
    """Patches the path for the validator to find the mock schema."""
    # The validator looks for ../mCIDE/<ModelName>.json relative to its own location.
    # We patch the _load_spec function in the validator to use our temp dir.
    from pyclif.utils import validator
    monkeypatch.setattr(validator, '_DEF_SPEC_DIR', str(mock_rs_model_json.parent))

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_rs_data():
    """Create a valid respiratory_support DataFrame for testing."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001'], # Ensure string type
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 11:00']),
        'device_category': ['imv', 'imv'],
        'device_name': ['ventilator1', 'ventilator1'],
        'mode_category': ['assist control-volume control', 'assist control-volume control'],
        'fio2_set': [0.5, 0.6]
    }).astype({'hospitalization_id': 'str'})

@pytest.fixture
def sample_invalid_rs_data_schema():
    """Create a respiratory_support DataFrame with schema violations."""
    return pd.DataFrame({
        'hospitalization_id': ['H001'],
        # Missing recorded_dttm
        'device_category': ['invalid_device'], # Invalid category
        'fio2_set': ['not_a_number'] # Invalid data type
    })

@pytest.fixture
def mock_rs_file(tmp_path, sample_valid_rs_data):
    """Create a mock respiratory_support parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_respiratory_support.parquet"
    sample_valid_rs_data.to_parquet(file_path)
    return str(test_dir) # from_file expects directory path

# --- Tests for respiratory_support class ---

# Initialization and Schema Loading
@pytest.mark.usefixtures("patch_rs_schema_path")
def test_rs_init_with_valid_data(sample_valid_rs_data):
    """Test initialization with valid data."""
    rs_obj = respiratory_support(sample_valid_rs_data)
    assert rs_obj.df is not None
    assert rs_obj.isvalid() is True
    assert not rs_obj.errors

@pytest.mark.usefixtures("patch_rs_schema_path")
def test_rs_init_with_invalid_schema_data(sample_invalid_rs_data_schema):
    """Test initialization with schema-invalid data."""
    rs_obj = respiratory_support(sample_invalid_rs_data_schema)
    assert rs_obj.isvalid() is False
    assert len(rs_obj.errors) > 0
    error_types = {e['type'] for e in rs_obj.errors}
    assert 'missing_columns' in error_types
    assert 'invalid_category' in error_types
    assert 'datatype_mismatch' in error_types

@pytest.mark.usefixtures("patch_rs_schema_path")
def test_rs_init_without_data():
    """Test initialization without data."""
    rs_obj = respiratory_support()
    assert rs_obj.df is None
    assert rs_obj.isvalid() is True # No data, so no errors

# from_file constructor
@pytest.mark.usefixtures("patch_rs_schema_path")
def test_rs_from_file(mock_rs_file, sample_valid_rs_data):
    """Test loading data from a parquet file."""
    rs_obj = respiratory_support.from_file(mock_rs_file, table_format_type="parquet")
    assert rs_obj.df is not None
    pd.testing.assert_frame_equal(rs_obj.df.reset_index(drop=True), sample_valid_rs_data.reset_index(drop=True), check_dtype=False)
    assert rs_obj.isvalid() is True

@pytest.mark.usefixtures("patch_rs_schema_path")
def test_rs_from_file_nonexistent(tmp_path):
    """Test loading from a nonexistent file."""
    with pytest.raises(FileNotFoundError):
        respiratory_support.from_file(str(tmp_path / "nonexistent.parquet"))

# isvalid and validate methods
@pytest.mark.usefixtures("patch_rs_schema_path")
def test_rs_isvalid_and_validate(sample_valid_rs_data, sample_invalid_rs_data_schema, capsys):
    """Test isvalid and validate methods and their output."""
    # Valid
    valid_obj = respiratory_support(sample_valid_rs_data)
    captured = capsys.readouterr()
    assert valid_obj.isvalid() is True
    assert "Validation completed successfully." in captured.out

    # Invalid
    invalid_obj = respiratory_support(sample_invalid_rs_data_schema)
    captured = capsys.readouterr()
    assert invalid_obj.isvalid() is False
    assert "Validation completed with" in captured.out

    # No data
    no_data_obj = respiratory_support()
    no_data_obj.validate()
    captured = capsys.readouterr()
    assert "No dataframe to validate." in captured.out

# --- Waterfall Method Tests ---

@pytest.fixture
def waterfall_input_data():
    """Create a DataFrame for testing the waterfall method."""
    return pd.DataFrame({
        'hospitalization_id': ['H001', 'H001', 'H001', 'H001'],
        'recorded_dttm': pd.to_datetime(['2023-01-01 10:00', '2023-01-01 12:00', '2023-01-01 14:00', '2023-01-01 15:00']).tz_localize('UTC'),
        'device_category': ['imv', None, 'room air', 'imv'],
        'device_name': ['ventilator1', 'ventilator1', None, 'ventilator2'],
        'mode_category': ['assist control-volume control', None, None, 'simv'],
        'mode_name': ['AC/VC', None, None, 'SIMV-VC'],
        'fio2_set': [50.0, None, None, 60.0], # Note: FiO2 > 1 to test scaling
        'peep_set': [5.0, 6.0, None, 7.0],
        'resp_rate_set': [12.0, 14.0, None, 16.0],
        'tidal_volume_set': [500.0, 510.0, None, 520.0],
        'peak_inspiratory_pressure_set': [20.0, 22.0, None, 25.0],
        'pressure_support_set': [10.0, 12.0, None, 15.0],
        'tracheostomy': [None, None, None, None],      # Added missing column
        'lpm_set': [None, None, None, None],           # Added missing column
        'resp_rate_obs': [None, None, None, None]      # Added missing column
    })

@pytest.mark.usefixtures("patch_rs_schema_path")
def test_waterfall_processing(waterfall_input_data):
    """Test the full waterfall processing logic."""
    rs_obj = respiratory_support(waterfall_input_data)
    processed_df = rs_obj.waterfall(verbose=False)

    # Check for hourly scaffold rows
    assert 'is_scaffold' in processed_df.columns
    assert processed_df['is_scaffold'].any()

    # Check FiO2 scaling (50.0 -> 0.5, 60.0 -> 0.6)
    assert processed_df['fio2_set'].max() <= 1.0

    # Check hierarchical ID generation
    for col in ['device_cat_id', 'device_id', 'mode_cat_id', 'mode_name_id']:
        assert col in processed_df.columns

    # Check filling logic (forward/backward fill within groups)
    # Example: Check if None in fio2_set at 12:00 is filled
    original_none_mask = waterfall_input_data['recorded_dttm'] == pd.to_datetime('2023-01-01 12:00').tz_localize('UTC')
    original_none_index = waterfall_input_data[original_none_mask].index[0]
    
    # Find the corresponding row in the processed_df (might not be at the same index)
    processed_row = processed_df[processed_df['recorded_dttm'] == pd.to_datetime('2023-01-01 12:00').tz_localize('UTC')]
    assert not processed_row.empty
    # The value should be filled from the 10:00 entry (0.5)
    assert pd.notna(processed_row['fio2_set'].iloc[0])
    assert processed_row['fio2_set'].iloc[0] == 0.5 

    # Check room air default FiO2
    room_air_row = processed_df[processed_df['device_category'] == 'room air']
    assert not room_air_row.empty
    # The value should be filled to 0.21
    assert room_air_row['fio2_set'].iloc[0] == 0.21

@pytest.mark.usefixtures("patch_rs_schema_path")
def test_waterfall_no_data():
    """Test waterfall with no data."""
    rs_obj = respiratory_support()
    with pytest.raises(ValueError, match="No data available"):
        rs_obj.waterfall()
