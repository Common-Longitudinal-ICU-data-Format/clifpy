"""
Tests for the microbiology_culture table module.
"""
import os
import pytest
import pandas as pd
import json
from datetime import datetime
from clifpy.tables.microbiology_culture import MicrobiologyCulture

# --- Data Fixtures ---
@pytest.fixture
def sample_valid_microbiology_data():
    """Create a valid microbiology culture DataFrame for testing."""
    return pd.DataFrame({
        'patient_id': ['12345', '12345', '67890'],
        'hospitalization_id': ['HOSP12345', 'HOSP12345', 'HOSP67890'],
        'organism_id': ['ORG001', 'ORG002', 'ORG003'],
        'order_dttm': pd.to_datetime(['2025-06-05 08:15:00+00:00', '2025-06-05 08:15:00+00:00', '2025-06-10 14:10:00+00:00']),
        'collect_dttm': pd.to_datetime(['2025-06-05 08:45:00+00:00', '2025-06-05 08:45:00+00:00', '2025-06-10 14:35:00+00:00']),
        'result_dttm': pd.to_datetime(['2025-06-06 12:00:00+00:00', '2025-06-06 12:00:00+00:00', '2025-06-11 09:20:00+00:00']),
        'fluid_name': ['AFB/FUNGAL BLOOD CULTURE', 'AFB/FUNGAL BLOOD CULTURE', 'BRAIN BIOPSY CULTURE'],
        'fluid_category': ['Blood/Buffy Coat', 'Blood/Buffy Coat', 'Brain'],
        'method_name': ['Blood culture', 'Blood culture', 'Tissue culture'],
        'method_category': ['culture', 'culture', 'culture'],
        'organism_name': ['Acinetobacter baumanii', 'Candida albicans', 'Aspergillus fumigatus'],
        'organism_category': ['acinetobacter_baumanii', 'candida_albicans', 'aspergillus_fumigatus'],
        'organism_group': ['acinetobacter (baumanii, calcoaceticus, lwoffi, other species)', 'candida albicans', 'asperguillus fumigatus'],
        'lab_loinc_code': ['', '', '']
    })

@pytest.fixture
def sample_invalid_microbiology_data_schema():
    """Create a microbiology DataFrame with schema violations."""
    return pd.DataFrame({
        'patient_id': ['12345'],
        'hospitalization_id': ['HOSP12345'],
        'organism_id': [123],  # Invalid data type for organism_id, schema expects VARCHAR
        'order_dttm': pd.to_datetime(['2025-06-05 08:15:00+00:00']),
        'collect_dttm': pd.to_datetime(['2025-06-05 08:45:00+00:00']),
        'result_dttm': pd.to_datetime(['2025-06-06 12:00:00+00:00']),
        'fluid_category': ['Blood/Buffy Coat'],
        'method_category': ['culture'],
        'organism_category': ['acinetobacter_baumanii']
    })

@pytest.fixture
def sample_invalid_microbiology_data_organisms():
    """Create a microbiology DataFrame with unknown organism categories."""
    return pd.DataFrame({
        'patient_id': ['12345', '67890'],
        'hospitalization_id': ['HOSP12345', 'HOSP67890'],
        'organism_id': ['ORG001', 'ORG002'],
        'order_dttm': pd.to_datetime(['2025-06-05 08:15:00+00:00', '2025-06-10 14:10:00+00:00']),
        'collect_dttm': pd.to_datetime(['2025-06-05 08:45:00+00:00', '2025-06-10 14:35:00+00:00']),
        'result_dttm': pd.to_datetime(['2025-06-06 12:00:00+00:00', '2025-06-11 09:20:00+00:00']),
        'fluid_category': ['Blood/Buffy Coat', 'Brain'],
        'method_category': ['culture', 'unknown_method'],  # unknown method
        'organism_category': ['unknown_organism', 'aspergillus_fumigatus'],  # unknown organism
        'organism_group': ['unknown_group', 'asperguillus fumigatus']
    })

@pytest.fixture
def sample_microbiology_data_group_mismatch():
    """Create a microbiology DataFrame with organism group mismatches."""
    return pd.DataFrame({
        'patient_id': ['12345'],
        'hospitalization_id': ['HOSP12345'],
        'organism_id': ['ORG001'],
        'order_dttm': pd.to_datetime(['2025-06-05 08:15:00+00:00']),
        'collect_dttm': pd.to_datetime(['2025-06-05 08:45:00+00:00']),
        'result_dttm': pd.to_datetime(['2025-06-06 12:00:00+00:00']),
        'fluid_category': ['Blood/Buffy Coat'],
        'method_category': ['culture'],
        'organism_category': ['acinetobacter_baumanii'],
        'organism_group': ['wrong_group']  # Should be 'acinetobacter (baumanii, calcoaceticus, lwoffi, other species)'
    })

@pytest.fixture
def mock_microbiology_file(tmp_path, sample_valid_microbiology_data):
    """Create a mock microbiology parquet file for testing."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    file_path = test_dir / "clif_microbiology_culture.parquet"
    sample_valid_microbiology_data.to_parquet(file_path)
    return str(test_dir) # from_file expects directory path

# --- Mock Schema ---
@pytest.fixture
def mock_microbiology_schema_content():
    """Provides the content for a mock microbiology_cultureModel.json."""
    return {
        "columns": [
            {"name": "patient_id", "data_type": "VARCHAR", "required": True},
            {"name": "hospitalization_id", "data_type": "VARCHAR", "required": True},
            {"name": "organism_id", "data_type": "VARCHAR", "required": True},
            {"name": "order_dttm", "data_type": "DATETIME", "required": True},
            {"name": "collect_dttm", "data_type": "DATETIME", "required": True},
            {"name": "result_dttm", "data_type": "DATETIME", "required": True},
            {"name": "fluid_name", "data_type": "VARCHAR", "required": False},
            {"name": "fluid_category", "data_type": "VARCHAR", "required": True, "is_category_column": True, "permissible_values": ["Blood/Buffy Coat", "Brain", "Respiratory", "Urine"]},
            {"name": "method_name", "data_type": "VARCHAR", "required": False},
            {"name": "method_category", "data_type": "VARCHAR", "required": True, "is_category_column": True, "permissible_values": ["culture", "gram stain", "smear"]},
            {"name": "organism_name", "data_type": "VARCHAR", "required": False},
            {"name": "organism_category", "data_type": "VARCHAR", "required": True, "is_category_column": True, "permissible_values": ["acinetobacter_baumanii", "candida_albicans", "aspergillus_fumigatus", "escherichia_coli"]},
            {"name": "organism_group", "data_type": "VARCHAR", "required": False},
            {"name": "lab_loinc_code", "data_type": "VARCHAR", "required": False}
        ],
        "organism_categories": {
            "acinetobacter_baumanii": "Acinetobacter baumanii",
            "candida_albicans": "Candida albicans",
            "aspergillus_fumigatus": "Aspergillus fumigatus",
            "escherichia_coli": "Escherichia coli"
        },
        "fluid_categories": {
            "Blood/Buffy Coat": "Blood or buffy coat specimen",
            "Brain": "Brain tissue specimen",
            "Respiratory": "Respiratory tract specimen",
            "Urine": "Urine specimen"
        },
        "method_categories": {
            "culture": "Microbial culture",
            "gram stain": "Gram staining",
            "smear": "Direct smear examination"
        },
        "organism_groups": {
            "acinetobacter_baumanii": "acinetobacter (baumanii, calcoaceticus, lwoffi, other species)",
            "candida_albicans": "candida albicans",
            "aspergillus_fumigatus": "asperguillus fumigatus",
            "escherichia_coli": "escherichia coli"
        }
    }

@pytest.fixture
def mock_mcide_dir(tmp_path):
    """Creates a temporary mCIDE directory."""
    mcide_path = tmp_path / "mCIDE"
    mcide_path.mkdir()
    return mcide_path

@pytest.fixture
def mock_microbiology_model_json(mock_mcide_dir, mock_microbiology_schema_content):
    """Creates a mock microbiology_cultureModel.json file in the temporary mCIDE directory."""
    schema_file_path = mock_mcide_dir / "microbiology_cultureModel.json"
    with open(schema_file_path, 'w') as f:
        json.dump(mock_microbiology_schema_content, f)
    return schema_file_path

@pytest.fixture
def patch_microbiology_schema_path(monkeypatch, mock_microbiology_model_json):
    """Patches the path to microbiology_cultureModel.json for the microbiology_culture class."""
    original_dirname = os.path.dirname
    original_join = os.path.join
    original_abspath = os.path.abspath

    def mock_dirname(path):
        if '__file__' in path: 
            return str(mock_microbiology_model_json.parent.parent / "tables")
        return original_dirname(path)

    def mock_abspath(path):
        if '__file__' in path:
            return str(mock_microbiology_model_json.parent.parent / "tables" / "dummy_microbiology_culture.py")
        return original_abspath(path)

    def mock_join(*args):
        if len(args) > 1 and args[1] == '..' and args[2] == 'mCIDE' and args[3] == 'microbiology_cultureModel.json':
            return str(mock_microbiology_model_json)
        return original_join(*args)

    monkeypatch.setattr(os.path, 'dirname', mock_dirname)
    monkeypatch.setattr(os.path, 'abspath', mock_abspath)
    monkeypatch.setattr(os.path, 'join', mock_join)

# --- Tests for microbiology_culture class --- 

# Initialization and Schema Loading
@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_microbiology_culture_init_with_valid_data(sample_valid_microbiology_data, mock_microbiology_schema_content):
    """Test microbiology culture initialization with valid data and mocked schema."""
    micro_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)
    assert micro_obj.df is not None
    # Validate is called in __init__
    if not micro_obj.isvalid():
        print("DEBUG micro_obj.errors:", micro_obj.errors)
        print("DEBUG micro_obj.organism_validation_errors:", micro_obj.organism_validation_errors)
    assert micro_obj.isvalid() is True
    assert not micro_obj.errors
    assert not micro_obj.organism_validation_errors
    assert "acinetobacter_baumanii" in micro_obj.organism_categories # Check schema loaded
    assert "Blood/Buffy Coat" in micro_obj.fluid_categories

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_microbiology_culture_init_with_invalid_schema_data(sample_invalid_microbiology_data_schema):
    """Test microbiology culture initialization with schema-invalid data."""
    micro_obj = MicrobiologyCulture(data=sample_invalid_microbiology_data_schema)
    assert micro_obj.df is not None
    # Validation is called in __init__. 
    assert micro_obj.isvalid() is False
    assert len(micro_obj.errors) > 0
    error_types = [e['type'] for e in micro_obj.errors]
    # validate_table (used by _validate_schema) reports 'datatype_mismatch'
    assert 'missing_columns' in error_types or 'datatype_mismatch' in error_types

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_microbiology_culture_init_with_invalid_organism_data(sample_invalid_microbiology_data_organisms):
    """Test microbiology culture initialization with unknown organism data."""
    micro_obj = MicrobiologyCulture(data=sample_invalid_microbiology_data_organisms)
    assert micro_obj.df is not None
    # Validation is called in __init__ which calls validate_organism_categories.
    assert micro_obj.isvalid() is False
    assert len(micro_obj.organism_validation_errors) > 0
    assert any(e['error_type'] == 'unknown_organism_category' for e in micro_obj.organism_validation_errors)
    assert any(e['error_type'] == 'unknown_method_category' for e in micro_obj.organism_validation_errors)

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_microbiology_culture_init_with_group_mismatch(sample_microbiology_data_group_mismatch):
    """Test microbiology culture initialization with organism group mismatch."""
    micro_obj = MicrobiologyCulture(data=sample_microbiology_data_group_mismatch)
    assert micro_obj.df is not None
    # Validation is called in __init__ which calls validate_organism_categories.
    assert micro_obj.isvalid() is False
    assert len(micro_obj.organism_validation_errors) > 0
    assert any(e['error_type'] == 'organism_group_mismatch' for e in micro_obj.organism_validation_errors)

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_microbiology_culture_init_without_data():
    """Test microbiology culture initialization without data."""
    micro_obj = MicrobiologyCulture()
    assert micro_obj.df is None
    assert micro_obj.isvalid() is True # No data, so no data errors
    assert not micro_obj.errors
    assert not micro_obj.organism_validation_errors
    assert "acinetobacter_baumanii" in micro_obj.organism_categories # Schema should still load

def test_load_microbiology_schema_file_not_found(monkeypatch, capsys):
    """Test _load_microbiology_schema when microbiology_cultureModel.json is not found."""
    def mock_join_raise_fnf(*args):
        if 'microbiology_cultureModel.json' in args[-1]:
            raise FileNotFoundError("Mocked File Not Found")
        return os.path.join(*args)
    monkeypatch.setattr(os.path, 'join', mock_join_raise_fnf)
    
    micro_obj = MicrobiologyCulture() # Init will call _load_microbiology_schema
    assert micro_obj._organism_categories == {}
    assert micro_obj._fluid_categories == {}
    assert micro_obj._method_categories == {}
    assert micro_obj._organism_groups == {}
    captured = capsys.readouterr()
    assert "Warning: microbiology_cultureModel.json not found" in captured.out

# from_file constructor
@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_microbiology_culture_from_file(mock_microbiology_file, sample_valid_microbiology_data):
    """Test loading microbiology culture data from a parquet file."""
    micro_obj = MicrobiologyCulture.from_file(mock_microbiology_file, table_format_type="parquet")
    assert micro_obj.df is not None
    # Standardize DataFrames before comparison (e.g. reset index, sort)
    expected_df = sample_valid_microbiology_data.reset_index(drop=True)
    loaded_df = micro_obj.df.reset_index(drop=True)
    pd.testing.assert_frame_equal(loaded_df, expected_df, check_dtype=False)
    assert micro_obj.isvalid() is True # Validation is called in init

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_microbiology_culture_from_file_nonexistent(tmp_path):
    """Test loading microbiology culture data from a nonexistent file."""
    non_existent_path = str(tmp_path / "nonexistent_dir")
    with pytest.raises(FileNotFoundError):
        MicrobiologyCulture.from_file(non_existent_path, table_format_type="parquet")

# isvalid method
@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_microbiology_culture_isvalid(sample_valid_microbiology_data, sample_invalid_microbiology_data_organisms):
    """Test isvalid method."""
    valid_micro = MicrobiologyCulture(data=sample_valid_microbiology_data)
    assert valid_micro.isvalid() is True
    
    invalid_micro = MicrobiologyCulture(data=sample_invalid_microbiology_data_organisms)
    # isvalid() reflects the state after the last validate() call, which happens at init
    assert invalid_micro.isvalid() is False 

# validate method
@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_microbiology_culture_validate_output(sample_valid_microbiology_data, sample_invalid_microbiology_data_organisms, capsys):
    """Test validate method output messages."""
    # Valid data - validation runs at init
    MicrobiologyCulture(data=sample_valid_microbiology_data) 
    captured = capsys.readouterr() 
    assert "Validation completed successfully." in captured.out

    # Invalid organism data - validation runs at init
    MicrobiologyCulture(data=sample_invalid_microbiology_data_organisms) 
    captured = capsys.readouterr()
    assert "Validation completed with" in captured.out
    assert "error(s)" in captured.out

    # No data
    micro_obj_no_data = MicrobiologyCulture()
    micro_obj_no_data.validate() # Explicit call as init with no data might not print this
    captured = capsys.readouterr()
    assert "No dataframe to validate." in captured.out

# Schema Properties Access
@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_microbiology_culture_properties_access(mock_microbiology_schema_content):
    """Test access to organism_categories, fluid_categories, method_categories, and organism_groups properties."""
    micro_obj = MicrobiologyCulture()
    
    # Test organism_categories
    organisms = micro_obj.organism_categories
    assert organisms == mock_microbiology_schema_content['organism_categories']
    assert id(organisms) != id(micro_obj._organism_categories) # Ensure it's a copy
    # Modify the returned dict and check original is not affected
    organisms['new_key'] = 'new_value'
    assert 'new_key' not in micro_obj.organism_categories

    # Test fluid_categories
    fluids = micro_obj.fluid_categories
    assert fluids == mock_microbiology_schema_content['fluid_categories']
    assert id(fluids) != id(micro_obj._fluid_categories) # Ensure it's a copy

    # Test method_categories
    methods = micro_obj.method_categories
    assert methods == mock_microbiology_schema_content['method_categories']
    assert id(methods) != id(micro_obj._method_categories) # Ensure it's a copy

    # Test organism_groups
    groups = micro_obj.organism_groups
    assert groups == mock_microbiology_schema_content['organism_groups']
    assert id(groups) != id(micro_obj._organism_groups) # Ensure it's a copy

    # Test properties when schema is not loaded
    micro_obj_no_schema = MicrobiologyCulture()
    micro_obj_no_schema._organism_categories = None
    micro_obj_no_schema._fluid_categories = None
    micro_obj_no_schema._method_categories = None
    micro_obj_no_schema._organism_groups = None
    assert micro_obj_no_schema.organism_categories == {}
    assert micro_obj_no_schema.fluid_categories == {}
    assert micro_obj_no_schema.method_categories == {}
    assert micro_obj_no_schema.organism_groups == {}

# validate_organism_categories method
@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_validate_organism_categories_valid(sample_valid_microbiology_data):
    """Test validate_organism_categories with valid data."""
    micro_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)
    # validate_organism_categories is called during init's validate()
    assert not micro_obj.organism_validation_errors
    assert micro_obj.isvalid() is True

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_validate_organism_categories_unknown_organisms(sample_invalid_microbiology_data_organisms):
    """Test validate_organism_categories with unknown organism categories."""
    micro_obj = MicrobiologyCulture(data=sample_invalid_microbiology_data_organisms)
    assert len(micro_obj.organism_validation_errors) > 0
    assert any(e['error_type'] == 'unknown_organism_category' for e in micro_obj.organism_validation_errors)
    assert any(e['error_type'] == 'unknown_method_category' for e in micro_obj.organism_validation_errors)
    
    # Check specific error details
    unknown_organism_error = next((e for e in micro_obj.organism_validation_errors if e['error_type'] == 'unknown_organism_category'), None)
    assert unknown_organism_error is not None
    assert unknown_organism_error['organism_category'] == 'unknown_organism'
    assert micro_obj.isvalid() is False

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_validate_organism_categories_group_mismatch(sample_microbiology_data_group_mismatch):
    """Test validate_organism_categories with organism group mismatch."""
    micro_obj = MicrobiologyCulture(data=sample_microbiology_data_group_mismatch)
    assert len(micro_obj.organism_validation_errors) > 0
    assert any(e['error_type'] == 'organism_group_mismatch' for e in micro_obj.organism_validation_errors)
    
    group_mismatch_error = next((e for e in micro_obj.organism_validation_errors if e['error_type'] == 'organism_group_mismatch'), None)
    assert group_mismatch_error is not None
    assert group_mismatch_error['organism_category'] == 'acinetobacter_baumanii'
    assert group_mismatch_error['expected_group'] == 'acinetobacter (baumanii, calcoaceticus, lwoffi, other species)'
    assert group_mismatch_error['actual_group'] == 'wrong_group'
    assert micro_obj.isvalid() is False

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_validate_organism_categories_missing_columns():
    """Test validate_organism_categories with missing required columns."""
    # Missing organism_category
    data_missing_organism = pd.DataFrame({
        'patient_id': ['12345'],
        'hospitalization_id': ['HOSP12345'],
        'organism_id': ['ORG001']
    })
    micro_obj_missing_organism = MicrobiologyCulture(data=data_missing_organism)
    # Schema validation will likely also report missing 'organism_category'
    # Here we explicitly check the organism_validation_errors
    assert any(e['error_type'] == 'missing_columns_for_organism_validation' for e in micro_obj_missing_organism.organism_validation_errors)
    assert 'organism_category' in micro_obj_missing_organism.organism_validation_errors[0]['message']

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_validate_organism_categories_no_data_or_schema(capsys):
    """Test validate_organism_categories with no data or no schema."""
    # No DataFrame
    micro_obj_no_df = MicrobiologyCulture()
    micro_obj_no_df.validate_organism_categories() # Explicitly call
    assert not micro_obj_no_df.organism_validation_errors

    # Empty DataFrame
    micro_obj_empty_df = MicrobiologyCulture(data=pd.DataFrame(columns=['patient_id', 'hospitalization_id', 'organism_category']))
    micro_obj_empty_df.validate_organism_categories()
    assert not micro_obj_empty_df.organism_validation_errors

    # No organism_categories in schema
    micro_obj_no_schema_organisms = MicrobiologyCulture(data=pd.DataFrame({'organism_category': ['test_organism']}))
    micro_obj_no_schema_organisms._organism_categories = {} # Manually clear categories after init
    micro_obj_no_schema_organisms.validate_organism_categories()
    assert not micro_obj_no_schema_organisms.organism_validation_errors # Should not attempt validation if no categories defined

# --- Helper Method Tests ---

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_filter_by_organism_category(sample_valid_microbiology_data):
    """Test filter_by_organism_category method."""
    micro_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)
    
    # Existing organism_category
    filtered_df_acin = micro_obj.filter_by_organism_category('acinetobacter_baumanii')
    assert len(filtered_df_acin) == 1
    assert filtered_df_acin['organism_category'].iloc[0] == 'acinetobacter_baumanii'

    # Non-existing organism_category
    filtered_df_unknown = micro_obj.filter_by_organism_category('unknown_organism')
    assert filtered_df_unknown.empty

    # No data
    micro_obj_no_data = MicrobiologyCulture()
    assert micro_obj_no_data.filter_by_organism_category('acinetobacter_baumanii').empty

    # Data missing organism_category column
    data_no_organism = sample_valid_microbiology_data.drop(columns=['organism_category'])
    micro_obj_no_organism = MicrobiologyCulture(data=data_no_organism)
    assert micro_obj_no_organism.filter_by_organism_category('acinetobacter_baumanii').empty

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_filter_by_fluid_category(sample_valid_microbiology_data):
    """Test filter_by_fluid_category method."""
    micro_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)

    # Existing fluid_category
    filtered_df_blood = micro_obj.filter_by_fluid_category('Blood/Buffy Coat')
    assert len(filtered_df_blood) == 2
    assert all(filtered_df_blood['fluid_category'] == 'Blood/Buffy Coat')

    # Non-existing fluid_category
    filtered_df_unknown = micro_obj.filter_by_fluid_category('Unknown_Fluid')
    assert filtered_df_unknown.empty

    # No data
    micro_obj_no_data = MicrobiologyCulture()
    assert micro_obj_no_data.filter_by_fluid_category('Blood/Buffy Coat').empty

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_filter_by_method_category(sample_valid_microbiology_data):
    """Test filter_by_method_category method."""
    micro_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)

    # Existing method_category
    filtered_df_culture = micro_obj.filter_by_method_category('culture')
    assert len(filtered_df_culture) == 3
    assert all(filtered_df_culture['method_category'] == 'culture')

    # Non-existing method_category
    filtered_df_unknown = micro_obj.filter_by_method_category('unknown_method')
    assert filtered_df_unknown.empty

    # No data
    micro_obj_no_data = MicrobiologyCulture()
    assert micro_obj_no_data.filter_by_method_category('culture').empty

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_filter_by_organism_group(sample_valid_microbiology_data):
    """Test filter_by_organism_group method."""
    micro_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)

    # Existing organism_group
    filtered_df_candida = micro_obj.filter_by_organism_group('candida albicans')
    assert len(filtered_df_candida) == 1
    assert filtered_df_candida['organism_group'].iloc[0] == 'candida albicans'

    # Non-existing organism_group
    filtered_df_unknown = micro_obj.filter_by_organism_group('unknown_group')
    assert filtered_df_unknown.empty

    # No data
    micro_obj_no_data = MicrobiologyCulture()
    assert micro_obj_no_data.filter_by_organism_group('candida albicans').empty

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_get_organism_summary_stats(sample_valid_microbiology_data):
    """Test get_organism_summary_stats method."""
    # With data
    micro_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)
    stats = micro_obj.get_organism_summary_stats()
    assert not stats.empty
    assert 'unique_patients' in stats.columns
    assert 'unique_hospitalizations' in stats.columns
    assert 'total_cultures' in stats.columns
    assert stats.loc['acinetobacter_baumanii', 'total_cultures'] == 1

    # No data
    micro_obj_no_data = MicrobiologyCulture()
    stats_no_data = micro_obj_no_data.get_organism_summary_stats()
    assert stats_no_data.empty

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_get_fluid_summary_stats(sample_valid_microbiology_data):
    """Test get_fluid_summary_stats method."""
    # With data
    micro_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)
    stats = micro_obj.get_fluid_summary_stats()
    assert not stats.empty
    assert 'unique_patients' in stats.columns
    assert 'unique_hospitalizations' in stats.columns
    assert 'total_cultures' in stats.columns
    assert 'unique_organisms' in stats.columns
    assert stats.loc['Blood/Buffy Coat', 'total_cultures'] == 2

    # No data
    micro_obj_no_data = MicrobiologyCulture()
    stats_no_data = micro_obj_no_data.get_fluid_summary_stats()
    assert stats_no_data.empty

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_get_time_to_result_stats(sample_valid_microbiology_data):
    """Test get_time_to_result_stats method."""
    # With data
    micro_obj = MicrobiologyCulture(data=sample_valid_microbiology_data)
    stats = micro_obj.get_time_to_result_stats()
    assert not stats.empty
    assert 'mean' in stats.columns
    assert 'count' in stats.columns
    
    # Check that time calculation is reasonable (should be around 27-18 hours)
    assert all(stats['mean'] > 10)  # At least 10 hours
    assert all(stats['mean'] < 30)  # Less than 30 hours

    # No data
    micro_obj_no_data = MicrobiologyCulture()
    stats_no_data = micro_obj_no_data.get_time_to_result_stats()
    assert stats_no_data.empty

    # Data missing required columns
    data_no_times = sample_valid_microbiology_data.drop(columns=['collect_dttm', 'result_dttm'])
    micro_obj_no_times = MicrobiologyCulture(data=data_no_times)
    stats_no_times = micro_obj_no_times.get_time_to_result_stats()
    assert stats_no_times.empty

@pytest.mark.usefixtures("patch_microbiology_schema_path")
def test_get_organism_validation_report(sample_invalid_microbiology_data_organisms, sample_valid_microbiology_data):
    """Test get_organism_validation_report method."""
    # With organism errors
    micro_obj_errors = MicrobiologyCulture(data=sample_invalid_microbiology_data_organisms)
    report_errors = micro_obj_errors.get_organism_validation_report()
    assert isinstance(report_errors, pd.DataFrame)
    assert not report_errors.empty
    assert 'unknown_organism_category' in report_errors['error_type'].tolist()
    assert 'unknown_method_category' in report_errors['error_type'].tolist()

    # No organism errors
    micro_obj_no_errors = MicrobiologyCulture(data=sample_valid_microbiology_data)
    report_no_errors = micro_obj_no_errors.get_organism_validation_report()
    assert isinstance(report_no_errors, pd.DataFrame)
    assert report_no_errors.empty # Should be empty when no errors
    expected_cols = ['error_type', 'organism_category', 'affected_rows', 'message']
    if not report_no_errors.empty:
        assert all(col in report_no_errors.columns for col in expected_cols)
