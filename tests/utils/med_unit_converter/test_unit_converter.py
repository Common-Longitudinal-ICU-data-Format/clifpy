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
    ACCEPTABLE_RATE_UNITS,
    _convert_clean_units_to_base_units,
    standardize_dose_to_base_units,
    _convert_base_units_to_preferred_units,
    convert_dose_units_by_med_category,
    _clean_dose_unit_formats_duckdb,
    _clean_dose_unit_names_duckdb
)

# --- Helper Fixtures for CSV Loading ---
@pytest.fixture
def load_fixture_csv():
    """Load CSV fixture co-located in tests/utils/med_unit_converter/.

    Returns
    -------
    callable
        Function that loads CSV files from the fixture directory.
    """
    def _load(filename) -> pd.DataFrame:
        path = Path(__file__).parent / filename
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
    return load_fixture_csv('_clean_dose_unit_names_test_data.csv')

# ===========================================
# Tests for `_clean_dose_unit_formats`
# ===========================================
@pytest.fixture
def _clean_dose_unit_formats_test_data(load_fixture_csv):
    """Load test data for dose unit pattern normalization tests.

    Returns
    -------
    pd.DataFrame
        CSV data with columns:

        - case: 'valid' or 'invalid' to categorize test scenarios
        - med_dose_unit: Original dose unit string
        - _clean_format_unit: Expected cleaned result
    """
    df: pd.DataFrame = load_fixture_csv('_clean_dose_unit_formats_test_data.csv')
    # pd.read_csv will auto read any empty string like '' as np.nan, so need to change it back to ''
    df.replace(np.nan, None, inplace=True)
    return df

@pytest.mark.skip
def test__clean_dose_unit_formats(_clean_dose_unit_formats_test_data):
    """Test the _clean_dose_unit_formats function for proper formatting cleaning.

    Validates that the function correctly:

    1. Removes all whitespace (including internal spaces): 'mL / hr' -> 'ml/hr'
    2. Converts to lowercase: 'MCG/KG/MIN' -> 'mcg/kg/min'
    3. Handles edge cases like leading/trailing spaces: ' Mg/Hr ' -> 'mg/hr'

    Uses comprehensive test data from _clean_dose_unit_formats_test_data.csv
    covering both valid and invalid unit patterns.

    Parameters
    ----------
    clean_dose_unit_formats_test_data : pd.DataFrame
        Test fixture containing unit format test cases.
    """
    test_df: pd.DataFrame = _clean_dose_unit_formats_test_data
    # first check the filtering went right, i.e. test_df is not empty
    result_series = _clean_dose_unit_formats(test_df['med_dose_unit'])
    
    pd.testing.assert_series_equal(
        result_series.reset_index(drop=True), # actual
        test_df['_clean_format_unit'].reset_index(drop=True), # expected
        check_names=False
    )

def test__clean_dose_unit_formats_duckdb(_clean_dose_unit_formats_test_data):
    """Test the _clean_dose_unit_formats function for proper formatting cleaning.
    """
    test_df: pd.DataFrame = _clean_dose_unit_formats_test_data
    result_df = _clean_dose_unit_formats_duckdb(test_df).to_df()
    
    pd.testing.assert_series_equal(
        result_df['_clean_unit'].reset_index(drop=True), # actual
        test_df['_clean_format_unit'].reset_index(drop=True), # expected
        check_names=False
    )

# ===========================================
# Tests for `_clean_dose_unit_names`
# ===========================================
@pytest.mark.skip
def test__clean_dose_unit_names(_clean_dose_unit_names_test_data):
    """Test the _clean_dose_unit_names function for unit name standardization.

    Validates comprehensive unit name cleaning including:

    - Time units: 'hour', 'h' -> '/hr'; 'minute', 'm' -> '/min'
    - Volume units: 'liter', 'liters', 'litre', 'litres' -> 'l'
    - Unit units: 'units', 'unit' -> 'u'
    - Milli prefix: 'milli-units', 'milliunits' -> 'mu'
    - Special characters: 'µg', 'ug' -> 'mcg'
    - Mass units: 'gram' -> 'g'

    Uses comprehensive fixture data from _clean_dose_unit_names_test_data.csv
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

def test__clean_dose_unit_names_duckdb(_clean_dose_unit_names_test_data):
    """Test the _clean_dose_unit_names function for unit name standardization.
    """
    test_df = _clean_dose_unit_names_test_data.dropna()
    result_df = _clean_dose_unit_names_duckdb(test_df).to_df()
    
    pd.testing.assert_series_equal(
        result_df['_clean_unit'].reset_index(drop=True), # actual
        test_df['_clean_name_unit'].reset_index(drop=True), # expected
        check_names=False
    )

def test__acceptable_rate_units():
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
 
# ===========================================
# Tests for `_convert_clean_units_to_base_units`
# ===========================================
@pytest.fixture
def _convert_clean_units_to_base_units_test_data(load_fixture_csv):
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
    df: pd.DataFrame = load_fixture_csv('_convert_clean_units_to_base_units_test_data.csv')
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
def test__convert_clean_units_to_base_units(unit_converter_test_data, caplog):
    """Test the core _convert_clean_units_to_base_units conversion function.

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

    input_df = test_df.filter(items=['rn','med_dose', '_clean_unit', 'weight_kg'])
    
    # with caplog.at_level('WARNING'):
    result_df = _convert_clean_units_to_base_units(med_df = input_df) \
        .to_df() \
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

    Uses the same test data as test_convert_clean_units_to_base_units
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

    input_df = test_df.filter(items=['rn','med_dose', 'med_dose_unit', 'weight_kg'])
    
    # with caplog.at_level('WARNING'):
    base_df, _ = standardize_dose_to_base_units(med_df = input_df)
    base_df = base_df.to_df()
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
    
    result_df = _convert_base_units_to_preferred_units(med_df = input_df, override=True).to_df()
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
    
    result_df = _convert_base_units_to_preferred_units(med_df = input_df, override=True).to_df()
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
        'propofol_lb': 'mcg/lb/min',
        'propofol_unweighted': 'mcg/min',
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


def test_convert_dose_units_by_med_category_return_rel(convert_dose_units_by_med_category_test_data):
    """Test that return_rel=True returns DuckDB PyRelation instead of DataFrame.

    Validates that:

    1. Both return values are DuckDBPyRelation instances
    2. The data is equivalent when materialized to DataFrame
    3. Expected columns are present in the result
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

    result_rel, counts_rel = convert_dose_units_by_med_category(
        med_df=input_df,
        preferred_units=preferred_units,
        override=True,
        return_rel=True
    )

    # Verify return types are DuckDB PyRelation
    assert isinstance(result_rel, duckdb.DuckDBPyRelation)
    assert isinstance(counts_rel, duckdb.DuckDBPyRelation)

    # Verify data is equivalent when materialized
    result_df = result_rel.to_df()
    assert '_convert_status' in result_df.columns
    assert 'med_dose_converted' in result_df.columns
    assert 'med_dose_unit_converted' in result_df.columns

    # Verify counts can be materialized
    counts_df = counts_rel.to_df()
    assert 'count' in counts_df.columns


# TODO scenarios to test:
# - [x] preferred_units not supported (not in the acceptable set)
# - [x] cannot convert from rate to amount
# - [x] cannot convert from mass (mcg) to volume (ml)
# - [x] med_category not in the dataset (test_anti_join_validation_extra_med_category)
# - [x] test the error message when no override (test_anti_join_validation_*)


# ===========================================
# Eleven-test plan: weight-aware redesign closeout
# (mirrors §6c of tests_update_memo.md)
# ===========================================

# Each test below builds its own minimal in-test fixture (small pd.DataFrame
# literals) so the existing CSV-based fixtures stay untouched. This is the
# §6c-required coverage for behaviors that today are smoke-only or absent.

KG_PER_LB_LITERAL = 2.20462  # mirrors clifpy.utils.unit_converter.KG_PER_LB


# --- Critical (no current pytest coverage) ----------------------------------

def test_fallback_on_earliest_recovers_lagged_charting():
    """Test #1 of §6c: when the earliest charted weight is *after* med admin,
    `fallback_on_earliest=True` recovers it; default leaves the row failing.

    Verifies both the failure-path status string and the success-path
    `_weight_source='earliest_fallback'` provenance value.
    """
    med_df = pd.DataFrame({
        'rn': [0],
        'hospitalization_id': ['H_LAGGED'],
        'admin_dttm': [pd.Timestamp('2024-01-01 09:00')],
        'med_category': ['propofol_unweighted'],
        'med_dose': [10.0],
        'med_dose_unit': ['mcg/kg/min'],
    })
    vitals_df = pd.DataFrame({
        'hospitalization_id': ['H_LAGGED'],
        'recorded_dttm': [pd.Timestamp('2024-01-01 09:30')],
        'vital_category': ['weight_kg'],
        'vital_value': [72.0],
    })
    preferred_units = {'propofol_unweighted': 'mcg/min'}

    # --- default fallback_on_earliest=False -> should fail with weight-missing ---
    result_no_fb, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        vitals_df=vitals_df,
        preferred_units=preferred_units,
        override=True,
        show_intermediate=True,
    )
    assert result_no_fb['_convert_status'].iloc[0] == (
        'cannot convert weighted to unweighted: weight_kg is missing'
    )
    assert pd.isna(result_no_fb['_weight_source'].iloc[0])

    # --- fallback_on_earliest=True -> should succeed using the lagged 09:30 row ---
    result_fb, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        vitals_df=vitals_df,
        preferred_units=preferred_units,
        override=True,
        show_intermediate=True,
        fallback_on_earliest=True,
    )
    assert result_fb['_convert_status'].iloc[0] == 'success'
    assert result_fb['_weight_source'].iloc[0] == 'earliest_fallback'
    # 10 mcg/kg/min × 72 kg = 720 mcg/min
    assert result_fb['med_dose_converted'].iloc[0] == pytest.approx(720.0, rel=1e-6)
    assert result_fb['med_dose_unit_converted'].iloc[0] == 'mcg/min'


def test_temp_table_cleanup_after_call():
    """Test #2 of §6c: pandas-input -> the orchestrator promotes med_df into a
    `_med_unit_input` temp table (boundary 2a). When `return_rel=False`,
    the `finally` block must drop every `_med_unit_*` table on exit.
    """
    # Sanity-check: drop any leftover _med_unit_* tables before the test.
    for (tname,) in duckdb.sql("SHOW TABLES").fetchall():
        if tname.startswith('_med_unit_'):
            duckdb.execute(f"DROP TABLE IF EXISTS {tname}")

    med_df = pd.DataFrame({
        'rn': [0],
        'med_category': ['propofol'],
        'med_dose': [10.0],
        'med_dose_unit': ['mcg/kg/min'],
        'weight_kg': [70.0],
    })

    convert_dose_units_by_med_category(
        med_df=med_df,
        preferred_units={'propofol': 'mcg/kg/min'},
        override=True,
        return_rel=False,
    )

    leftover = [
        t for (t,) in duckdb.sql("SHOW TABLES").fetchall()
        if t.startswith('_med_unit_')
    ]
    assert leftover == [], f"Expected no _med_unit_* tables, found: {leftover}"


def test_identity_short_circuit_bit_exact():
    """Test #3 of §6c: with non-integer `weight_kg` and identical input/preferred
    units, the identity short-circuit must return `med_dose_converted == med_dose`
    exactly (no float drift from the 6-case factor multiplication).
    """
    weight = 73.4836
    dose = 12.345678901234567
    med_df = pd.DataFrame({
        'rn': [0],
        'med_category': ['propofol'],
        'med_dose': [dose],
        'med_dose_unit': ['mcg/kg/min'],
        'weight_kg': [weight],
    })

    result, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        preferred_units={'propofol': 'mcg/kg/min'},
        override=True,
    )
    # Bit-exact equality, not approx — short-circuit means no multiplication.
    assert result['med_dose_converted'].iloc[0] == dose
    assert result['med_dose_unit_converted'].iloc[0] == 'mcg/kg/min'
    assert result['_convert_status'].iloc[0] == 'success'


# --- Important (smoke-only today) -------------------------------------------

def test_needs_wt_full_matrix():
    """Test #4 of §6c: every {/kg, /lb, ''} × {/kg, /lb, ''} input/preferred
    combination, asserting `_needs_wt` matches the XOR truth table.

    Note: `/lb` source rows are normalized to `/kg` in stage 1, so
    `_base_wt` is always in {'/kg', ''}. The XOR is computed on `_base_wt`,
    so a `/lb → /kg` row has `_needs_wt = 0` (both ends weighted-equivalent).
    """
    rows = [
        # (med_category, input_unit, preferred_unit, expected_needs_wt)
        ('m_kg_kg', 'mcg/kg/min', 'mcg/kg/min', 0),
        ('m_kg_lb', 'mcg/kg/min', 'mcg/lb/min', 0),
        ('m_kg_un', 'mcg/kg/min', 'mcg/min',    1),
        ('m_lb_kg', 'mcg/lb/min', 'mcg/kg/min', 0),
        ('m_lb_lb', 'mcg/lb/min', 'mcg/lb/min', 0),
        ('m_lb_un', 'mcg/lb/min', 'mcg/min',    1),
        ('m_un_kg', 'mcg/min',    'mcg/kg/min', 1),
        ('m_un_lb', 'mcg/min',    'mcg/lb/min', 1),
        ('m_un_un', 'mcg/min',    'mcg/min',    0),
    ]
    med_df = pd.DataFrame({
        'rn': list(range(len(rows))),
        'med_category': [r[0] for r in rows],
        'med_dose': [10.0] * len(rows),
        'med_dose_unit': [r[1] for r in rows],
        'weight_kg': [70.0] * len(rows),  # plenty for needs-wt rows
    })
    preferred_units = {r[0]: r[2] for r in rows}

    result, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        preferred_units=preferred_units,
        override=True,
        show_intermediate=True,
    )
    result = result.sort_values('rn').reset_index(drop=True)
    expected = pd.Series([r[3] for r in rows], name='_needs_wt')
    pd.testing.assert_series_equal(
        result['_needs_wt'].astype(int).reset_index(drop=True),
        expected,
        check_names=False,
    )


def test_kg_lb_constant_factor_no_weight():
    """Test #5 of §6c: explicit kg<->lb conversion with `weight_kg=NULL`.

    Both directions must succeed without consulting patient weight, using
    the `KG_PER_LB` constant. Covers stage-1 application (lb->kg) and
    stage-2 application (kg->lb).
    """
    med_df = pd.DataFrame({
        'rn': [0, 1],
        'med_category': ['kg_to_lb', 'lb_to_kg'],
        'med_dose': [100.0, 100.0],
        'med_dose_unit': ['mcg/kg/min', 'mcg/lb/hr'],
        'weight_kg': [np.nan, np.nan],
    })
    preferred_units = {
        'kg_to_lb': 'mcg/lb/min',  # factor 1/KG_PER_LB applied in stage 2
        'lb_to_kg': 'mcg/kg/min',  # factor KG_PER_LB applied in stage 1; stage 2 sees identity
    }

    result, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        preferred_units=preferred_units,
        override=True,
    )
    result = result.sort_values('rn').reset_index(drop=True)
    assert (result['_convert_status'] == 'success').all()

    # kg->lb: 100 / 2.20462 mcg/lb/min
    assert result.loc[0, 'med_dose_converted'] == pytest.approx(
        100.0 / KG_PER_LB_LITERAL, rel=1e-9
    )
    assert result.loc[0, 'med_dose_unit_converted'] == 'mcg/lb/min'

    # lb/hr -> kg/min: stage 1 collapses lb->kg via *KG_PER_LB and hr->min via /60.
    # 100 mcg/lb/hr  ==  (100 * KG_PER_LB / 60) mcg/kg/min
    assert result.loc[1, 'med_dose_converted'] == pytest.approx(
        100.0 * KG_PER_LB_LITERAL / 60.0, rel=1e-9
    )
    assert result.loc[1, 'med_dose_unit_converted'] == 'mcg/kg/min'


def test_user_prefilled_weight_kg_honored():
    """Test #6 of §6c: when `weight_kg` is in `med_df`, the orchestrator must
    skip the vitals lookup entirely. NULLs in the supplied column stay NULL,
    and only `_needs_wt=1 AND weight_kg IS NULL` rows fail.
    """
    med_df = pd.DataFrame({
        'rn': [0, 1, 2, 3],
        'med_category': ['propofol_unweighted'] * 4,
        'med_dose': [10.0, 10.0, 10.0, 10.0],
        'med_dose_unit': ['mcg/kg/min', 'mcg/kg/min', 'mcg/kg/min', 'mcg/kg/min'],
        'weight_kg': [70.0, np.nan, 80.0, np.nan],
    })

    # vitals_df=None must NOT trigger an error: user-prefilled weight wins.
    result, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        vitals_df=None,
        preferred_units={'propofol_unweighted': 'mcg/min'},
        override=True,
    )
    result = result.sort_values('rn').reset_index(drop=True)

    # Rows 0, 2: weight present -> success.
    assert result.loc[0, '_convert_status'] == 'success'
    assert result.loc[0, 'med_dose_converted'] == pytest.approx(700.0)
    assert result.loc[2, '_convert_status'] == 'success'
    assert result.loc[2, 'med_dose_converted'] == pytest.approx(800.0)

    # Rows 1, 3: weight NULL -> weighted->unweighted failure (NULLs preserved).
    assert result.loc[1, '_convert_status'] == (
        'cannot convert weighted to unweighted: weight_kg is missing'
    )
    assert result.loc[3, '_convert_status'] == (
        'cannot convert weighted to unweighted: weight_kg is missing'
    )


def test_lazy_weight_join_skipped():
    """Test #7 of §6c: when no row needs weight, the `fetchone()` short-circuit
    must skip the vitals join entirely (vitals_df=None must NOT raise).
    """
    med_df = pd.DataFrame({
        'rn': [0, 1, 2],
        'med_category': ['kg_kg', 'kg_lb', 'amount_id'],
        'med_dose': [10.0, 10.0, 5.0],
        'med_dose_unit': ['mcg/kg/min', 'mcg/kg/min', 'mg'],
    })
    preferred_units = {
        'kg_kg': 'mcg/kg/min',
        'kg_lb': 'mcg/lb/min',
        'amount_id': 'mg',
    }

    # No 'weight_kg' column AND vitals_df=None -> must succeed because no row
    # crosses the weighted/unweighted boundary.
    result, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        vitals_df=None,
        preferred_units=preferred_units,
        override=True,
    )
    assert (result['_convert_status'] == 'success').all()


def test_anti_join_validation_unacceptable_preferred():
    """Test #8 of §6c: an unacceptable preferred unit must trigger ValueError
    (default) or a warning (`override=True`) via the ANTI JOIN validator
    inside `_convert_base_units_to_preferred_units`.
    """
    med_df = pd.DataFrame({
        'rn': [0],
        'med_category': ['zidovudine'],
        'med_dose': [10.0],
        'med_dose_unit': ['mg'],
        'weight_kg': [70.0],
    })
    preferred_units = {'zidovudine': 'iu/hr'}  # 'iu' is unrecognized

    with pytest.raises(ValueError):
        convert_dose_units_by_med_category(
            med_df=med_df,
            preferred_units=preferred_units,
            override=False,
        )

    # override=True -> warning, not raise. Still produces output.
    result, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        preferred_units=preferred_units,
        override=True,
    )
    assert len(result) == 1


def test_anti_join_validation_extra_med_category():
    """Test #9 of §6c: a `med_category` in `preferred_units` that is not in
    `med_df` must trigger ValueError (default) or warning (`override=True`)
    via the ANTI JOIN validator at the orchestrator entry point.
    """
    med_df = pd.DataFrame({
        'rn': [0],
        'med_category': ['propofol'],
        'med_dose': [10.0],
        'med_dose_unit': ['mcg/kg/min'],
        'weight_kg': [70.0],
    })
    preferred_units = {
        'propofol': 'mcg/kg/min',
        'no_such_med': 'mg/hr',  # not present in med_df
    }

    with pytest.raises(ValueError):
        convert_dose_units_by_med_category(
            med_df=med_df,
            preferred_units=preferred_units,
            override=False,
        )

    # override=True -> warning, not raise.
    result, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        preferred_units=preferred_units,
        override=True,
    )
    assert len(result) == 1


def test_weight_source_provenance():
    """Test #10 of §6c: three scenarios in one fixture exercise every value
    of `_weight_source`: 'asof', 'earliest_fallback', and NULL.
    """
    # Three hospitalizations:
    # H_ASOF:     vitals before admin -> 'asof'
    # H_LAGGED:   vitals after admin only -> 'earliest_fallback' (with fallback)
    # H_NONE:     no vitals at all -> NULL
    med_df = pd.DataFrame({
        'rn': [0, 1, 2],
        'hospitalization_id': ['H_ASOF', 'H_LAGGED', 'H_NONE'],
        'admin_dttm': [
            pd.Timestamp('2024-01-01 10:00'),
            pd.Timestamp('2024-01-01 09:00'),
            pd.Timestamp('2024-01-01 12:00'),
        ],
        'med_category': ['propofol_unweighted'] * 3,
        'med_dose': [10.0, 10.0, 10.0],
        'med_dose_unit': ['mcg/kg/min'] * 3,
    })
    vitals_df = pd.DataFrame({
        'hospitalization_id': ['H_ASOF', 'H_LAGGED'],
        'recorded_dttm': [
            pd.Timestamp('2024-01-01 08:00'),  # before H_ASOF admin
            pd.Timestamp('2024-01-01 09:30'),  # after H_LAGGED admin
        ],
        'vital_category': ['weight_kg', 'weight_kg'],
        'vital_value': [70.0, 72.0],
    })

    result, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        vitals_df=vitals_df,
        preferred_units={'propofol_unweighted': 'mcg/min'},
        override=True,
        show_intermediate=True,
        fallback_on_earliest=True,
    )
    # Index by hospitalization_id for stable lookups.
    by_id = result.set_index('hospitalization_id')
    assert by_id.loc['H_ASOF', '_weight_source'] == 'asof'
    assert by_id.loc['H_LAGGED', '_weight_source'] == 'earliest_fallback'
    assert pd.isna(by_id.loc['H_NONE', '_weight_source'])


# --- Lower priority ---------------------------------------------------------

def test_show_intermediate_surfaces_qa_columns():
    """Test #11 of §6c: `show_intermediate=True` must expose stage-2 multiplier
    columns (and the planning columns `_needs_wt`, `_base_wt`, `_pref_wt`).
    Default mode hides all of them.
    """
    med_df = pd.DataFrame({
        'rn': [0],
        'med_category': ['propofol'],
        'med_dose': [10.0],
        'med_dose_unit': ['mcg/kg/min'],
        'weight_kg': [70.0],
    })
    preferred_units = {'propofol': 'mcg/kg/hr'}

    # Default (show_intermediate=False) -> QA columns must be absent.
    default_result, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        preferred_units=preferred_units,
        override=True,
    )
    hidden_cols = {
        '_amount_multiplier_preferred',
        '_time_multiplier_preferred',
        '_weight_multiplier_preferred',
        '_needs_wt',
        '_base_wt',
        '_pref_wt',
        '_base_unit',
        '_preferred_unit',
        '_weight_source',
    }
    assert hidden_cols.isdisjoint(default_result.columns), (
        f"Default mode leaked QA columns: "
        f"{hidden_cols & set(default_result.columns)}"
    )

    # show_intermediate=True -> QA columns must be present.
    qa_result, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        preferred_units=preferred_units,
        override=True,
        show_intermediate=True,
    )
    expected_visible = {
        '_amount_multiplier_preferred',
        '_time_multiplier_preferred',
        '_weight_multiplier_preferred',
        '_needs_wt',
        '_base_wt',
        '_pref_wt',
    }
    missing = expected_visible - set(qa_result.columns)
    assert not missing, f"show_intermediate=True missing QA columns: {missing}"