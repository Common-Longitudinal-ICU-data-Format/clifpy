"""Tests for the unit_converter module.

This module contains comprehensive tests for all functions in the
clifpy.utils.unit_converter module, including unit cleaning, conversion,
and standardization functionality.
"""
import pytest
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
from clifpy.utils.unit_converter import (
    _clean_dose_unit_formats,
    _clean_dose_unit_names,
    _detect_and_classify_clean_dose_units,
    ACCEPTABLE_RATE_UNITS,
    _convert_clean_dose_units_to_base_units,
    standardize_dose_to_base_units,
    _convert_base_units_to_preferred_units,
    convert_dose_units_by_med_category
)

# --- Helper Fixtures for CSV Loading ---
@pytest.fixture
def load_fixture_csv():
    """Load CSV fixture from tests/fixtures/unit_converter/.

    Returns
    -------
    callable
        Function that loads CSV files from the fixture directory.
    """
    def _load(filename) -> pd.DataFrame:
        path = Path(__file__).parent.parent / 'fixtures' / 'unit_converter' / filename
        # pd.read_csv will auto read any empty string like '' as np.nan, so need to change it back to ''
        df = pd.read_csv(path) # .replace(np.nan, '')
        return df
    return _load


# --- Data Fixtures ---
@pytest.fixture
def _clean_dose_unit_names_test_data(load_fixture_csv):
    """Load test data for dose unit name normalization tests.

    Returns
    -------
    pd.DataFrame
        CSV data with columns:

        - _clean_format_unit: Input dose unit string (already format cleaned)
        - _clean_name_unit: Expected cleaned result
    """
    return load_fixture_csv('test__clean_dose_unit_names.csv')

# ===========================================
# Tests for `_clean_dose_unit_formats`
# ===========================================
@pytest.fixture
def clean_dose_unit_formats_test_data(load_fixture_csv):
    """Load test data for dose unit pattern normalization tests.

    Returns
    -------
    pd.DataFrame
        CSV data with columns:

        - case: 'valid' or 'invalid' to categorize test scenarios
        - med_dose_unit: Original dose unit string
        - _clean_format_unit: Expected cleaned result
    """
    df: pd.DataFrame = load_fixture_csv('test__clean_dose_unit_formats.csv')
    # pd.read_csv will auto read any empty string like '' as np.nan, so need to change it back to ''
    df.replace(np.nan, None, inplace=True)
    return df

@pytest.mark.unit_conversion
def test_clean_dose_unit_formats(clean_dose_unit_formats_test_data):
    """Test the _clean_dose_unit_formats function for proper formatting cleaning.

    Validates that the function correctly:

    1. Removes all whitespace (including internal spaces): 'mL / hr' -> 'ml/hr'
    2. Converts to lowercase: 'MCG/KG/MIN' -> 'mcg/kg/min'
    3. Handles edge cases like leading/trailing spaces: ' Mg/Hr ' -> 'mg/hr'

    Uses comprehensive test data from test__clean_dose_unit_formats.csv
    covering both valid and invalid unit patterns.

    Parameters
    ----------
    clean_dose_unit_formats_test_data : pd.DataFrame
        Test fixture containing unit format test cases.
    """
    test_df: pd.DataFrame = clean_dose_unit_formats_test_data
    # first check the filtering went right, i.e. test_df is not empty
    result_series = _clean_dose_unit_formats(test_df['med_dose_unit'])
    
    pd.testing.assert_series_equal(
        result_series.reset_index(drop=True), # actual
        test_df['_clean_format_unit'].reset_index(drop=True), # expected
        check_names=False
    )

# ===========================================
# Tests for `_clean_dose_unit_names`
# ===========================================
def test_clean_dose_unit_names(_clean_dose_unit_names_test_data):
    """Test the _clean_dose_unit_names function for unit name standardization.

    Validates comprehensive unit name cleaning including:

    - Time units: 'hour', 'h' -> '/hr'; 'minute', 'm' -> '/min'
    - Volume units: 'liter', 'liters', 'litre', 'litres' -> 'l'
    - Unit units: 'units', 'unit' -> 'u'
    - Milli prefix: 'milli-units', 'milliunits' -> 'mu'
    - Special characters: 'µg', 'ug' -> 'mcg'
    - Mass units: 'gram' -> 'g'

    Uses comprehensive fixture data from test__clean_dose_unit_names.csv
    containing real-world unit variations.

    Parameters
    ----------
    _clean_dose_unit_names_test_data : pd.DataFrame
        Test fixture containing unit name cleaning test cases.
    """
    test_df = _clean_dose_unit_names_test_data.dropna()

    # Apply the function
    result_series = _clean_dose_unit_names(test_df['_clean_format_unit'])
    
    # Validate results
    pd.testing.assert_series_equal(
        result_series.reset_index(drop=True),
        test_df['_clean_name_unit'].reset_index(drop=True),
        check_names=False
    )

def test_acceptable_rate_units():
    """Test the acceptable_rate_units function and ACCEPTABLE_RATE_UNITS constant.

    Validates that the function correctly generates all valid combinations
    of rate units by checking both positive cases (should be accepted)
    and negative cases (should be rejected).
    """
    positive_cases = ['ml/hr', 'ml/min', 'l/hr', 'l/min', 'u/hr', 'u/min']
    negative_cases = ['ml', 'l', 'u', 'mcg', 'mg', 'ng', 'units/min']
    
    for unit in positive_cases:
        assert unit in ACCEPTABLE_RATE_UNITS
    
    for unit in negative_cases:
        assert unit not in ACCEPTABLE_RATE_UNITS
 
def test_detect_and_classify_clean_dose_units():
    """Test the _detect_and_classify_clean_dose_units classification function.

    Validates that the function correctly categorizes clean units into:

    - Rate units: recognized rate patterns (e.g., ml/hr, mcg/kg/min)
    - Amount units: recognized amount patterns (e.g., ml, mcg, u)
    - Unrecognized units: anything else including nulls and invalid patterns

    Also verifies correct counting of each unit type including duplicates.
    """
    test_series = pd.Series([
        # rate units
        'u/min', 
        'ml/hr', 'ml/hr', 
        'mcg/kg/hr', 'mcg/kg/hr', 'mcg/kg/hr',
        # amount units
        'ml',
        'mu', 'mu',
        'g', 'g', 'g',
        # unrecognized units
        'units/min', 
        'kg', 'kg', 
        pd.NA, pd.NA, 
        None, None, None,
        "", "", "", "",
        np.nan, np.nan, np.nan, np.nan, np.nan
    ])
    
    expected_dict = {
        'rate_units': {
            'u/min': 1, 'ml/hr': 2, 'mcg/kg/hr': 3
        },
        'amount_units': {
            'ml': 1, 'mu': 2, 'g': 3
        },
        'unrecognized_units': {
            'units/min': 1, 'kg': 2, pd.NA: 2, None: 3, '': 4, np.nan: 5
        }
    }
    
    result_dict = _detect_and_classify_clean_dose_units(test_series)
    
    assert result_dict == expected_dict
    

# ===========================================
# Tests for `_convert_clean_dose_units_to_base_units`
# ===========================================
@pytest.fixture
def convert_clean_dose_units_to_base_units_test_data(load_fixture_csv):
    """Load test data for dose unit conversion tests.

    Returns
    -------
    pd.DataFrame
        CSV data with columns:

        - hospitalization_id, admin_dttm: Patient and timing identifiers
        - med_dose, med_dose_unit: Original dose values and units
        - weight_kg: Patient weight (may be empty/NaN)
        - _base_dose, _base_unit: Expected conversion results
        - case: Test scenario category

        Processes admin_dttm to datetime and converts empty weight_kg to NaN.
    """
    df: pd.DataFrame = load_fixture_csv('test__convert_clean_dose_units_to_base_units.csv')
    # df['admin_dttm'] = pd.to_datetime(df['admin_dttm'])
    # Replace empty strings with NaN for weight_kg column
    df['weight_kg'] = df['weight_kg'].replace('', np.nan)
    return df

@pytest.fixture
def unit_converter_test_data(load_fixture_csv):
    """Load test data for standardize_dose_to_base_units tests.

    Provides comprehensive test data for validating the complete unit
    standardization pipeline from raw units to base standard units.

    Returns
    -------
    pd.DataFrame
        Test data with columns:

        - rn: Row number for ordering
        - med_dose: Original dose values
        - med_dose_unit: Original unit strings (various formats)
        - _clean_unit: Expected cleaned unit
        - _unit_class: Expected classification ('rate', 'amount', 'unrecognized')
        - _base_dose: Expected converted dose value
        - _base_unit: Expected base unit
        - weight_kg: Patient weight (may be NaN)

    Notes
    -----
    Filters out rows where unit_class is NaN to focus on valid test cases.
    Replaces empty strings in weight_kg with NaN for proper null handling.
    """
    df = load_fixture_csv('test_unit_converter - standardize_dose_to_base_units.csv').dropna(subset=['_unit_class'])
    # df['admin_dttm'] = pd.to_datetime(df['admin_dttm'])
    # Replace empty strings with NaN for weight_kg column
    df['weight_kg'] = df['weight_kg'].replace('', np.nan)
    return df

@pytest.fixture
def convert_dose_units_by_med_category_test_data(load_fixture_csv):
    """Load test data for convert_dose_units_by_med_category tests.

    Provides test data for validating medication-specific unit conversions
    including both valid and invalid conversion scenarios.

    Returns
    -------
    pd.DataFrame
        Test data with columns:

        - rn: Row number for test ordering
        - case: 'valid' or 'invalid' test scenario
        - med_category: Medication category (e.g., 'propofol', 'fentanyl')
        - med_dose: Original dose value
        - med_dose_unit: Original unit string
        - _clean_unit: Cleaned unit
        - _unit_class: Unit classification
        - _unit_subclass: Unit subclassification ('mass', 'volume', 'unit')
        - _base_dose: Dose in base units
        - _base_unit: Base unit string
        - weight_kg: Patient weight (may be NaN)
        - _preferred_unit: Target preferred unit
        - _unit_class_preferred: Preferred unit classification
        - _unit_subclass_preferred: Preferred unit subclassification
        - med_dose_converted: Expected final dose value
        - med_dose_unit_converted: Expected final unit
        - _convert_status: Expected conversion status message
        - note: Test case description

    Notes
    -----
    Includes test cases for:

    - Successful conversions between compatible units
    - Failed conversions (cross-class, cross-subclass)
    - Unrecognized units
    - Missing/empty units
    - Weight-based conversions
    """
    df = load_fixture_csv('test_unit_converter - convert_dose_units_by_med_category.csv').dropna(subset=['_unit_class'])
    # df['admin_dttm'] = pd.to_datetime(df['admin_dttm'])
    # Replace empty strings with NaN for weight_kg column
    df['weight_kg'] = df['weight_kg'].replace('', np.nan)
    return df

@pytest.mark.unit_conversion
def test_convert_clean_dose_units_to_base_units(unit_converter_test_data, caplog):
    """Test the core _convert_clean_dose_units_to_base_units conversion function.

    This comprehensive test validates the DuckDB-based conversion logic that
    transforms clean units to standard base units.

    Validates conversion to standard target units, weight-based dose calculations,
    time unit conversions, volume conversions, mass conversions, unit conversions,
    proper handling of unrecognized units, and presence of all required output columns.

    Uses comprehensive test data from test_unit_converter.csv with
    pre-calculated expected values for validation.

    Parameters
    ----------
    unit_converter_test_data : pd.DataFrame
        Test fixture containing conversion test cases.
    caplog : pytest.LogCaptureFixture
        Pytest fixture for capturing log messages.
    """
    test_df: pd.DataFrame = unit_converter_test_data 
    # test_df = pd.read_csv('../../tests/fixtures/unit_converter/test__convert_clean_dose_units_to_base_units.csv')

    input_df = test_df.filter(items=['rn','med_dose', '_clean_unit', 'weight_kg'])
    
    # with caplog.at_level('WARNING'):
    result_df = _convert_clean_dose_units_to_base_units(med_df = input_df) \
        .sort_values(by=['rn']) # sort by rn to ensure the order of the rows is consistent
    
    # Verify columns exist
    assert '_base_dose' in result_df.columns
    assert '_base_unit' in result_df.columns
    assert 'weight_kg' in result_df.columns
    # assert "Unrecognized dose units found" in caplog.text # check that the warning is logged

    # Verify base values
    pd.testing.assert_series_equal(
        result_df['_base_dose'].reset_index(drop=True), # actual
        test_df['_base_dose'].reset_index(drop=True), # expected
        check_names=False,
        # check_dtype=False
    )

    # Verify base units
    pd.testing.assert_series_equal(
        # TODO: may consider adding a check NA test and avoid using fillna('None') here
        result_df['_base_unit'].fillna('None').reset_index(drop=True),
        test_df['_base_unit'].fillna('None').reset_index(drop=True),
        check_names=False,
        # check_dtype=False
    )
    
    # Verify unit class
    pd.testing.assert_series_equal(
        result_df['_unit_class'].reset_index(drop=True),
        test_df['_unit_class'].reset_index(drop=True),
        check_names=False,
        # check_dtype=False
    )

@pytest.mark.unit_conversion
def test_standardize_dose_to_base_units(unit_converter_test_data, caplog):
    """Test the main public API function standardize_dose_to_base_units.

    This is the primary integration test for the complete unit standardization
    pipeline, testing the end-to-end conversion process from raw unit strings
    to standardized base units.

    Validates complete pipeline execution including format cleaning, name cleaning,
    and unit conversion. Ensures both output DataFrames are correctly generated
    and all intermediate and final columns are present.

    Uses the same test data as test_convert_clean_dose_units_to_base_units
    but starts with raw, non-cleaned unit strings to test the full pipeline.

    Parameters
    ----------
    unit_converter_test_data : pd.DataFrame
        Test fixture containing conversion test cases.
    caplog : pytest.LogCaptureFixture
        Pytest fixture for capturing log messages.

    Notes
    -----
    This test assumes weight_kg is already present in the input DataFrame
    (not testing vitals join functionality).
    """
    test_df: pd.DataFrame = unit_converter_test_data 
    # test_df = pd.read_csv('../../tests/fixtures/unit_converter/test__convert_clean_dose_units_to_base_units.csv')

    input_df = test_df.filter(items=['rn','med_dose', 'med_dose_unit', 'weight_kg'])
    
    # with caplog.at_level('WARNING'):
    base_df, counts_df = standardize_dose_to_base_units(med_df = input_df)
    base_df.sort_values(by=['rn'], inplace=True) # sort by rn to ensure the order of the rows is consistent
    
    # Verify columns exist
    assert '_clean_unit' in base_df.columns
    assert '_base_dose' in base_df.columns
    assert '_base_unit' in base_df.columns
    assert 'weight_kg' in base_df.columns

    # Verify base values
    pd.testing.assert_series_equal(
        base_df['_base_dose'].reset_index(drop=True), # actual
        test_df['_base_dose'].reset_index(drop=True), # expected
        check_names=False,
        #check_dtype=False
    )

    # Verify base units
    pd.testing.assert_series_equal(
        base_df['_base_unit'].fillna('None').reset_index(drop=True),
        test_df['_base_unit'].fillna('None').reset_index(drop=True),
        check_names=False,
        # check_dtype=False
    )

# @pytest.mark.skip
def test__convert_base_units_to_preferred_units_inverse(unit_converter_test_data, caplog):
    """Test the _convert_base_units_to_preferred_units conversion function.

    Tests the inverse process of converting from original to base, i.e. converting from
    _base_dose and _base_unit to med_dose and _clean_unit respectively.

    Parameters
    ----------
    unit_converter_test_data : pd.DataFrame
        Test fixture containing conversion test cases.
    caplog : pytest.LogCaptureFixture
        Pytest fixture for capturing log messages.

    Notes
    -----
    This test assumes the presence of _clean_unit column.
    """
    test_df: pd.DataFrame = unit_converter_test_data # .query("case == 'valid'")
    assert len(test_df) > 0
    q = """
    SELECT rn
        , _base_dose
        , _base_unit
        , _clean_unit as _preferred_unit
        , weight_kg
    FROM test_df
    """
    input_df = duckdb.sql(q).to_df()
    
    result_df = _convert_base_units_to_preferred_units(med_df = input_df, override=True)
    result_df.sort_values(by=['rn'], inplace=True) # sort by rn to ensure the order of the rows is consistent

    # Verify output columns exist
    assert 'med_dose_unit_converted' in result_df.columns
    assert 'med_dose_converted' in result_df.columns

    # Verify preferred values
    pd.testing.assert_series_equal(
        result_df['med_dose_converted'].reset_index(drop=True), # actual
        test_df['med_dose'].reset_index(drop=True), # expected
        check_names=False,
        # check_dtype=False
    ) 

def test__convert_base_units_to_preferred_units_new(convert_dose_units_by_med_category_test_data, caplog):
    """Test the _convert_base_units_to_preferred_units conversion function.

    Parameters
    ----------
    convert_dose_units_by_med_category_test_data : pd.DataFrame
        Test fixture containing conversion test cases by medication category.
    caplog : pytest.LogCaptureFixture
        Pytest fixture for capturing log messages.

    Notes
    -----
    This test assumes the presence of _preferred_unit column.
    """
    test_df: pd.DataFrame = convert_dose_units_by_med_category_test_data.query("case == 'valid'")
    assert len(test_df) > 0
    q = """
    SELECT rn
        , _base_dose
        , _base_unit
        , _preferred_unit
        , weight_kg
    FROM test_df
    """
    input_df = duckdb.sql(q).to_df()
    
    result_df = _convert_base_units_to_preferred_units(med_df = input_df, override=True)
    result_df.sort_values(by=['rn'], inplace=True) # sort by rn to ensure the order of the rows is consistent

    # Verify columns exist
    assert 'med_dose_unit_converted' in result_df.columns
    assert 'med_dose_converted' in result_df.columns

    # Verify converted dose values
    pd.testing.assert_series_equal(
        result_df['med_dose_converted'].reset_index(drop=True), # actual
        test_df['med_dose_converted'].reset_index(drop=True), # expected
        check_names=False,
        check_exact=False,
        rtol=1e-3,
        atol=1e-5
    ) 
    
    # Verify converted units
    pd.testing.assert_series_equal(
        result_df['med_dose_unit_converted'].reset_index(drop=True), # actual
        test_df['med_dose_unit_converted'].reset_index(drop=True), # expected
        check_names=False,
        check_exact=False
    )

def test_convert_dose_units_by_med_category(convert_dose_units_by_med_category_test_data, caplog):
    """Test the public API function convert_dose_units_by_med_category.

    Tests the complete medication-specific unit conversion pipeline using
    a dictionary of preferred units for each medication category.

    Parameters
    ----------
    convert_dose_units_by_med_category_test_data : pd.DataFrame
        Test fixture containing conversion test cases by medication category.
    caplog : pytest.LogCaptureFixture
        Pytest fixture for capturing log messages.
    """
    test_df: pd.DataFrame = convert_dose_units_by_med_category_test_data
    
    input_df = test_df.filter(items=['rn','med_category', 'med_dose', 'med_dose_unit', 'weight_kg'])
    
    preferred_units = {
        'propofol': 'mcg/kg/min',
        'midazolam': 'mg/hr',
        'fentanyl': 'mcg/hr',
        'insulin': 'u/hr',
        'norepinephrine': 'ng/kg/min',
        'dextrose': 'g',
        'heparin': 'l/hr',
        'bivalirudin': 'ml/hr',
        'oxytocin': 'mu',
        'lactated_ringers_solution': 'ml',
        'liothyronine': 'u/hr',
        'zidovudine': 'iu/hr'
        }
    
    result_df, _ = convert_dose_units_by_med_category(med_df = input_df, preferred_units = preferred_units, override=True)
    result_df.sort_values(by=['rn'], inplace=True) # sort by rn to ensure the order of the rows is consistent
    
    # check _convert_status
    pd.testing.assert_series_equal(
        result_df['_convert_status'].reset_index(drop=True), # actual
        test_df['_convert_status'].reset_index(drop=True), # expected
        check_names=False,
        # check_dtype=False
    )
    
    # check med_dose_preferred
    pd.testing.assert_series_equal(
        result_df['med_dose_converted'].reset_index(drop=True), # actual
        test_df['med_dose_converted'].reset_index(drop=True), # expected
        check_names=False,
        # check_dtype=False
        rtol=1e-3,
        atol=1e-5
    )
    
    # check med_dose_unit_preferred
    pd.testing.assert_series_equal(
        result_df['med_dose_unit_converted'].reset_index(drop=True), # actual
        test_df['med_dose_unit_converted'].reset_index(drop=True), # expected
        check_names=False,
        # check_dtype=False
    )
    

# TODO scenarios to test:
# - [x] preferred_units not supported (not in the acceptable set)
# - [x] cannot convert from rate to amount
# - [x] cannot convert from mass (mcg) to volume (ml)
# - [ ] med_category not in the dataset
# - [ ] test the error message when no override