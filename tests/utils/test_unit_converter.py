"""
Tests for the unit_converter module.
"""
import pytest
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
from clifpy.utils.unit_converter import (
    _normalize_dose_unit_formats,
    _normalize_dose_unit_names,
    _detect_and_classify_normalized_dose_units,
    ACCEPTABLE_RATE_UNITS,
    _convert_normalized_dose_units_to_limited_units,
    standardize_dose_to_limited_units,
    _convert_limited_units_to_preferred_units,
    convert_dose_units_by_med_category
)

# --- Helper Fixtures for CSV Loading ---
@pytest.fixture
def load_fixture_csv():
    """Load CSV fixture from tests/fixtures/unit_converter/"""
    def _load(filename) -> pd.DataFrame:
        path = Path(__file__).parent.parent / 'fixtures' / 'unit_converter' / filename
        # pd.read_csv will auto read any empty string like '' as np.nan, so need to change it back to ''
        df = pd.read_csv(path) # .replace(np.nan, '') 
        return df
    return _load


# --- Data Fixtures ---
@pytest.fixture
def _normalize_dose_unit_names_test_data(load_fixture_csv):
    """
    Load test data for dose unit name normalization tests.
    
    Returns CSV data with columns:
    - med_dose_unit_format_normalized: Input dose unit string (already format normalized)
    - med_dose_unit_name_normalized: Expected normalized result
    """
    return load_fixture_csv('test_normalize_dose_unit_names.csv')

# ===========================================
# Tests for `_normalize_dose_unit_formats`
# ===========================================
@pytest.fixture
def normalize_dose_unit_formats_test_data(load_fixture_csv):
    """
    Load test data for dose unit pattern normalization tests.
    
    Returns CSV data with columns:
    - case: 'valid' or 'invalid' to categorize test scenarios
    - med_dose_unit: Original dose unit string
    - med_dose_unit_format_normalized: Expected normalized result
    """
    df: pd.DataFrame =load_fixture_csv('test_normalize_dose_unit_formats.csv')
    # pd.read_csv will auto read any empty string like '' as np.nan, so need to change it back to ''
    df.replace(np.nan, None, inplace=True) 
    return df

@pytest.mark.unit_conversion
def test_normalize_dose_unit_formats(normalize_dose_unit_formats_test_data):
    """
    Test the _normalize_dose_unit_formats function for proper formatting normalization.
    
    Validates that the function correctly:
    1. Removes all whitespace (including internal spaces): 'mL / hr' -> 'ml/hr'
    2. Converts to lowercase: 'MCG/KG/MIN' -> 'mcg/kg/min'
    3. Handles edge cases like leading/trailing spaces: ' Mg/Hr ' -> 'mg/hr'
    
    Uses comprehensive test data from test_normalize_dose_unit_formats.csv
    covering both valid and invalid unit patterns.
    
    Test Coverage
    -------------
    - Various spacing patterns
    - Mixed case inputs
    - Special characters preservation
    - Empty and null handling
    """
    test_df: pd.DataFrame = normalize_dose_unit_formats_test_data
    # first check the filtering went right, i.e. test_df is not empty
    result_series = _normalize_dose_unit_formats(test_df['med_dose_unit'])
    
    pd.testing.assert_series_equal(
        result_series.reset_index(drop=True), # actual
        test_df['med_dose_unit_format_normalized'].reset_index(drop=True), # expected
        check_names=False
    )

# ===========================================
# Tests for `_normalize_dose_unit_names`
# ===========================================
def test_normalize_dose_unit_names(_normalize_dose_unit_names_test_data):
    """
    Test the _normalize_dose_unit_names function for unit name standardization.
    
    Validates comprehensive unit name normalization including:
    - Time units: 'hour', 'h' -> '/hr'; 'minute', 'm' -> '/min'
    - Volume units: 'liter', 'liters', 'litre', 'litres' -> 'l'
    - Unit units: 'units', 'unit' -> 'u'
    - Milli prefix: 'milli-units', 'milliunits' -> 'mu'
    - Special characters: 'µg', 'ug' -> 'mcg'
    - Mass units: 'gram' -> 'g'
    
    Uses comprehensive fixture data from test_normalize_dose_unit_names.csv
    containing real-world unit variations.
    
    Test Coverage
    -------------
    - All regex pattern replacements in UNIT_NAMING_VARIANTS
    - Preservation of weight qualifiers (/kg, /lb)
    - Handling of compound units
    - Edge cases and malformed inputs
    """
    test_df = _normalize_dose_unit_names_test_data.dropna()

    # Apply the function
    result_series = _normalize_dose_unit_names(test_df['med_dose_unit_format_normalized'])
    
    # Validate results
    pd.testing.assert_series_equal(
        result_series.reset_index(drop=True),
        test_df['med_dose_unit_name_normalized'].reset_index(drop=True),
        check_names=False
    )

def test_acceptable_rate_units():
    """
    Test the acceptable_rate_units function and ACCEPTABLE_RATE_UNITS constant.
    
    Validates that the function correctly generates all valid combinations
    of rate units by checking both positive cases (should be accepted)
    and negative cases (should be rejected).
    
    Test Coverage
    -------------
    - Basic rate units (ml/hr, u/min, etc.)
    - Weight-based rate units (mcg/kg/hr, ml/lb/min)
    - Amount units that should NOT be rate units
    - Invalid unit patterns
    """
    positive_cases = ['ml/hr', 'ml/min', 'l/hr', 'l/min', 'u/hr', 'u/min']
    negative_cases = ['ml', 'l', 'u', 'mcg', 'mg', 'ng', 'units/min']
    
    for unit in positive_cases:
        assert unit in ACCEPTABLE_RATE_UNITS
    
    for unit in negative_cases:
        assert unit not in ACCEPTABLE_RATE_UNITS
 
def test_detect_and_classify_normalized_dose_units():
    """
    Test the _detect_and_classify_normalized_dose_units classification function.
    
    Validates that the function correctly categorizes normalized units into:
    - Rate units: recognized rate patterns (e.g., ml/hr, mcg/kg/min)
    - Amount units: recognized amount patterns (e.g., ml, mcg, u)
    - Unrecognized units: anything else including nulls and invalid patterns
    
    Also verifies correct counting of each unit type including duplicates.
    
    Test Coverage
    -------------
    - All major unit categories
    - Duplicate handling and counting
    - Various null representations (None, pd.NA, np.nan, empty string)
    - Unrecognized but valid-looking patterns
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
    
    result_dict = _detect_and_classify_normalized_dose_units(test_series)
    
    assert result_dict == expected_dict
    

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
    - med_dose_limited, med_dose_unit_limited: Expected conversion results
    - case: Test scenario category
    
    Processes admin_dttm to datetime and converts empty weight_kg to NaN.
    """
    df: pd.DataFrame = load_fixture_csv('test_convert_normalized_dose_units_to_limited_units.csv')
    # df['admin_dttm'] = pd.to_datetime(df['admin_dttm'])
    # Replace empty strings with NaN for weight_kg column
    df['weight_kg'] = df['weight_kg'].replace('', np.nan)
    return df

@pytest.fixture
def unit_converter_test_data(load_fixture_csv):
    """
    Load test data for standardize_dose_to_limited_units tests.
    
    Provides comprehensive test data for validating the complete unit
    standardization pipeline from raw units to limited standard units.
    
    Returns
    -------
    pd.DataFrame
        Test data with columns:
        - rn: Row number for ordering
        - med_dose: Original dose values
        - med_dose_unit: Original unit strings (various formats)
        - med_dose_unit_normalized: Expected normalized unit
        - unit_class: Expected classification ('rate', 'amount', 'unrecognized')
        - med_dose_limited: Expected converted dose value
        - med_dose_unit_limited: Expected limited unit
        - weight_kg: Patient weight (may be NaN)
        
    Notes
    -----
    Filters out rows where unit_class is NaN to focus on valid test cases.
    Replaces empty strings in weight_kg with NaN for proper null handling.
    """
    df = load_fixture_csv('test_unit_converter - standardize_dose_to_limited_units.csv').dropna(subset=['unit_class'])
    # df['admin_dttm'] = pd.to_datetime(df['admin_dttm'])
    # Replace empty strings with NaN for weight_kg column
    df['weight_kg'] = df['weight_kg'].replace('', np.nan)
    return df

@pytest.fixture
def convert_dose_units_by_med_category_test_data(load_fixture_csv):
    """
    Load test data for convert_dose_units_by_med_category tests.
    
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
        - med_dose_unit_normalized: Normalized unit
        - unit_class: Unit classification
        - unit_subclass: Unit subclassification ('mass', 'volume', 'unit')
        - med_dose_limited: Dose in limited units
        - med_dose_unit_limited: Limited unit string
        - weight_kg: Patient weight (may be NaN)
        - med_dose_unit_preferred: Target preferred unit
        - unit_class_preferred: Preferred unit classification
        - unit_subclass_preferred: Preferred unit subclassification
        - med_dose_converted: Expected final dose value
        - med_dose_unit_converted: Expected final unit
        - convert_status: Expected conversion status message
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
    df = load_fixture_csv('test_unit_converter - convert_dose_units_by_med_category.csv').dropna(subset=['unit_class'])
    # df['admin_dttm'] = pd.to_datetime(df['admin_dttm'])
    # Replace empty strings with NaN for weight_kg column
    df['weight_kg'] = df['weight_kg'].replace('', np.nan)
    return df

@pytest.mark.unit_conversion
def test_convert_normalized_dose_units_to_limited_units(unit_converter_test_data, caplog):
    """
    Test the core _convert_normalized_dose_units_to_limited_units conversion function.
    
    This comprehensive test validates the DuckDB-based conversion logic that
    transforms normalized units to standard limited units.
    
    Validates
    ---------
    - Conversion to standard target units:
      * Rate units: mcg/min, ml/min, u/min
      * Amount units: mcg, ml, u
    - Weight-based dose calculations (using weight_kg column)
    - Time unit conversions (per hour to per minute, factor of 1/60)
    - Volume conversions (L to mL, factor of 1000)
    - Mass conversions (mg to mcg: 1000, ng to mcg: 1/1000, g to mcg: 1000000)
    - Unit conversions (milli-units to units, factor of 1/1000)
    - Proper handling of unrecognized units (NULL in limited columns)
    - All required output columns are present
    
    Uses comprehensive test data from test_unit_converter.csv with
    pre-calculated expected values for validation.
    
    Test Coverage
    -------------
    - All conversion factor combinations
    - Weight-based dosing with /kg and /lb
    - Edge cases (missing weights, zero doses)
    - Unrecognized unit handling
    """
    test_df: pd.DataFrame = unit_converter_test_data 
    # test_df = pd.read_csv('../../tests/fixtures/unit_converter/test_convert_normalized_dose_units_to_limited_units.csv')

    input_df = test_df.filter(items=['rn','med_dose', 'med_dose_unit_normalized', 'weight_kg'])
    
    # with caplog.at_level('WARNING'):
    result_df = _convert_normalized_dose_units_to_limited_units(med_df = input_df) \
        .sort_values(by=['rn']) # sort by rn to ensure the order of the rows is consistent
    
    # Verify columns exist
    assert 'med_dose_limited' in result_df.columns
    assert 'med_dose_unit_limited' in result_df.columns
    assert 'weight_kg' in result_df.columns
    # assert "Unrecognized dose units found" in caplog.text # check that the warning is logged

    # Verify limited values
    pd.testing.assert_series_equal(
        result_df['med_dose_limited'].reset_index(drop=True), # actual
        test_df['med_dose_limited'].reset_index(drop=True), # expected
        check_names=False,
        # check_dtype=False
    )

    # Verify limited units
    pd.testing.assert_series_equal(
        # TODO: may consider adding a check NA test and avoid using fillna('None') here
        result_df['med_dose_unit_limited'].fillna('None').reset_index(drop=True),
        test_df['med_dose_unit_limited'].fillna('None').reset_index(drop=True),
        check_names=False,
        # check_dtype=False
    )
    
    # Verify unit class
    pd.testing.assert_series_equal(
        result_df['unit_class'].reset_index(drop=True),
        test_df['unit_class'].reset_index(drop=True),
        check_names=False,
        # check_dtype=False
    )

@pytest.mark.unit_conversion
def test_standardize_dose_to_limited_units(unit_converter_test_data, caplog):
    """
    Test the main public API function standardize_dose_to_limited_units.
    
    This is the primary integration test for the complete unit standardization
    pipeline, testing the end-to-end conversion process from raw unit strings
    to standardized limited units.
    
    Validates
    ---------
    - Complete pipeline execution:
      1. Format normalization (spaces, case)
      2. Name normalization (variants to standard)
      3. Unit conversion (to limited set)
    - Both output DataFrames are correctly generated:
      1. limited medication DataFrame with all columns
      2. Summary counts table (via _create_unit_conversion_counts_table)
    - All intermediate and final columns are present
    - Conversion accuracy matches expected values
    
    Uses the same test data as test_convert_normalized_dose_units_to_limited_units
    but starts with raw, non-normalized unit strings to test the full pipeline.
    
    Test Coverage
    -------------
    - Integration of all conversion steps
    - Preservation of original data columns
    - Addition of all conversion-related columns
    - Accuracy of final limited values and units
    
    Notes
    -----
    This test assumes weight_kg is already present in the input DataFrame
    (not testing vitals join functionality).
    """
    test_df: pd.DataFrame = unit_converter_test_data 
    # test_df = pd.read_csv('../../tests/fixtures/unit_converter/test_convert_normalized_dose_units_to_limited_units.csv')

    input_df = test_df.filter(items=['rn','med_dose', 'med_dose_unit', 'weight_kg'])
    
    # with caplog.at_level('WARNING'):
    limited_df, counts_df = standardize_dose_to_limited_units(med_df = input_df)
    limited_df.sort_values(by=['rn'], inplace=True) # sort by rn to ensure the order of the rows is consistent
    
    # Verify columns exist
    assert 'med_dose_unit_normalized' in limited_df.columns
    assert 'med_dose_limited' in limited_df.columns
    assert 'med_dose_unit_limited' in limited_df.columns
    assert 'weight_kg' in limited_df.columns

    # Verify limited values
    pd.testing.assert_series_equal(
        limited_df['med_dose_limited'].reset_index(drop=True), # actual
        test_df['med_dose_limited'].reset_index(drop=True), # expected
        check_names=False,
        #check_dtype=False
    )

    # Verify limited units
    pd.testing.assert_series_equal(
        limited_df['med_dose_unit_limited'].fillna('None').reset_index(drop=True),
        test_df['med_dose_unit_limited'].fillna('None').reset_index(drop=True),
        check_names=False,
        # check_dtype=False
    )

# @pytest.mark.skip
def test__convert_limited_units_to_preferred_units_inverse(unit_converter_test_data, caplog):
    """
    Test the _convert_limited_units_to_preferred_units conversion function.
    
    is equivalent to the inverse process of converting from original to limited, i.e. converting from 
    med_dose_limited and med_dose_unit_limited to med_dose and med_dose_unit_normalized respectively
    
    this test assumes the presense of med_dose_unit_normalized column
    """
    test_df: pd.DataFrame = unit_converter_test_data # .query("case == 'valid'")
    assert len(test_df) > 0
    q = """
    SELECT rn
        , med_dose_limited
        , med_dose_unit_limited
        , med_dose_unit_normalized as med_dose_unit_preferred
        , weight_kg
    FROM test_df
    """
    input_df = duckdb.sql(q).to_df()
    
    result_df = _convert_limited_units_to_preferred_units(med_df = input_df, override=True)
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

def test__convert_limited_units_to_preferred_units_new(convert_dose_units_by_med_category_test_data, caplog):
    """
    Test the _convert_limited_units_to_preferred_units conversion function.
    
    this test assumes the presense of med_dose_unit_preferred column
    """
    test_df: pd.DataFrame = convert_dose_units_by_med_category_test_data.query("case == 'valid'")
    assert len(test_df) > 0
    q = """
    SELECT rn
        , med_dose_limited
        , med_dose_unit_limited
        , med_dose_unit_preferred
        , weight_kg
    FROM test_df
    """
    input_df = duckdb.sql(q).to_df()
    
    result_df = _convert_limited_units_to_preferred_units(med_df = input_df, override=True)
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
    '''
    test the public API function convert_dose_units_by_med_category which takes in a dict
    '''
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
    
    # check convert_status
    pd.testing.assert_series_equal(
        result_df['convert_status'].reset_index(drop=True), # actual
        test_df['convert_status'].reset_index(drop=True), # expected
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
    

"""
TODO scenarios to test:
- [x] preferred_units not supported (not in the acceptable set)
- [x] cannot convert from rate to amount
- [x] cannot convert from mass (mcg) to volume (ml)
- [ ] med_category not in the dataset
- [ ] test the error message when no override
"""