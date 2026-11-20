"""Tests for the unit_converter module.

This module contains comprehensive tests for all functions in the
clifpy.utils.unit_converter module, including unit cleaning, conversion,
and standardization functionality.
"""
import pytest
import polars as pl
import duckdb
from datetime import datetime
from pathlib import Path
from polars.testing import assert_series_equal
from clifpy.utils.unit_converter import (
    ACCEPTABLE_RATE_UNITS,
    _convert_clean_units_to_base_units,
    standardize_dose_to_base_units,
    _convert_base_units_to_preferred_units,
    convert_dose_units_by_med_category,
    _clean_dose_unit_formats_duckdb,
    _clean_dose_unit_names_duckdb
)
from clifpy.utils.io import load_data


def _spelled(units: pl.Series) -> pl.Series:
    """`units` as the public outputs show them: the internal unit-family
    tokens spelled out (`u/min` -> `units/min`, `mu` -> `milli-units`), a
    token rewritten only when a `/` or the end follows it. The fixtures keep
    the internal tokens because the stage-internal tests read them as input."""
    for token, name in (('mnu', 'million-units'), ('mu', 'milli-units'), ('u', 'units')):
        units = units.str.replace(rf'^{token}(/|$)', f'{name}$1')
    return units

# --- Helper Fixtures for CSV Loading ---
@pytest.fixture
def load_fixture_csv():
    """Load CSV fixture co-located in tests/utils/med_unit_converter/.

    Returns
    -------
    callable
        Function that loads a CSV fixture as a polars DataFrame, read the way
        these tests always read it: an empty field is missing, and an integer
        column holding a missing value is Float64.
    """
    def _load(filename) -> pl.DataFrame:
        path = Path(__file__).parent / filename
        df = pl.read_csv(path, infer_schema_length=None, null_values=[''])
        return df.with_columns(
            pl.col(name).cast(pl.Float64)
            for name, dtype in df.schema.items()
            if dtype.is_integer() and df[name].null_count() > 0
        )
    return _load


# --- Data Fixtures ---
@pytest.fixture
def _clean_dose_unit_names_test_data(load_fixture_csv):
    """Load test data for dose unit name normalization tests.

    Returns
    -------
    pl.DataFrame
        CSV data with columns:

        - _clean_format_unit: Input dose unit string (already format cleaned)
        - _clean_name_unit: Expected cleaned result
    """
    return load_fixture_csv('_clean_dose_unit_names_test_data.csv')

# ===========================================
# Tests for `_clean_dose_unit_formats_duckdb`
# ===========================================
@pytest.fixture
def _clean_dose_unit_formats_test_data(load_fixture_csv):
    """Load test data for dose unit pattern normalization tests.

    Returns
    -------
    pl.DataFrame
        CSV data with columns:

        - case: 'valid' or 'invalid' to categorize test scenarios
        - med_dose_unit: Original dose unit string
        - _clean_format_unit: Expected cleaned result
    """
    df: pl.DataFrame = load_fixture_csv('_clean_dose_unit_formats_test_data.csv')
    return df


def test__clean_dose_unit_formats_duckdb(_clean_dose_unit_formats_test_data):
    """Test _clean_dose_unit_formats_duckdb for proper formatting cleaning."""
    test_df: pl.DataFrame = _clean_dose_unit_formats_test_data.with_row_index('rn')
    result_df = _clean_dose_unit_formats_duckdb(test_df).pl().sort('rn')
    
    assert_series_equal(
        result_df['_clean_unit'], # actual
        test_df['_clean_format_unit'], # expected
        check_names=False
    )

# ===========================================
# Tests for `_clean_dose_unit_names_duckdb`
# ===========================================

def test__clean_dose_unit_names_duckdb(_clean_dose_unit_names_test_data):
    """Test _clean_dose_unit_names_duckdb for unit name standardization."""
    test_df = _clean_dose_unit_names_test_data.drop_nulls().with_row_index('rn')
    result_df = _clean_dose_unit_names_duckdb(test_df).pl().sort('rn')
    
    assert_series_equal(
        result_df['_clean_unit'], # actual
        test_df['_clean_name_unit'], # expected
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
    pl.DataFrame
        CSV data with columns:

        - hospitalization_id, admin_dttm: Patient and timing identifiers
        - med_dose, med_dose_unit: Original dose values and units
        - weight_kg: Patient weight (may be empty/NaN)
        - _base_dose, _base_unit: Expected conversion results
        - case: Test scenario category

        Processes admin_dttm to datetime and converts empty weight_kg to NaN.
    """
    df: pl.DataFrame = load_fixture_csv('_convert_clean_units_to_base_units_test_data.csv')
    return df

@pytest.fixture
def unit_converter_test_data(load_fixture_csv):
    """Load test data for standardize_dose_to_base_units tests.

    Provides comprehensive test data for validating the complete unit
    standardization pipeline from raw units to base standard units.

    Returns
    -------
    pl.DataFrame
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
    df = load_fixture_csv('test_unit_converter - standardize_dose_to_base_units.csv').drop_nulls('_unit_class')
    return df

@pytest.fixture
def convert_dose_units_by_med_category_test_data(load_fixture_csv):
    """Load test data for convert_dose_units_by_med_category tests.

    Provides test data for validating medication-specific unit conversions
    including both valid and invalid conversion scenarios.

    Returns
    -------
    pl.DataFrame
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
    df = load_fixture_csv('test_unit_converter - convert_dose_units_by_med_category.csv').drop_nulls('_unit_class')
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
    unit_converter_test_data : pl.DataFrame
        Test fixture containing conversion test cases.
    caplog : pytest.LogCaptureFixture
        Pytest fixture for capturing log messages.
    """
    test_df: pl.DataFrame = unit_converter_test_data 

    input_df = test_df.select(['rn','med_dose', '_clean_unit', 'weight_kg'])
    
    # with caplog.at_level('WARNING'):
    result_df = _convert_clean_units_to_base_units(med_df = input_df) \
        .pl() \
        .sort('rn') # sort by rn to ensure the order of the rows is consistent
    
    # Verify columns exist
    assert '_base_dose' in result_df.columns
    assert '_base_unit' in result_df.columns
    assert 'weight_kg' in result_df.columns
    # assert "Unrecognized dose units found" in caplog.text # check that the warning is logged

    # Verify base values
    assert_series_equal(
        result_df['_base_dose'], # actual
        test_df['_base_dose'], # expected
        check_names=False,
        # check_dtype=False
    )

    # Verify base units
    assert_series_equal(
        # TODO: may consider adding a check NA test and avoid using fillna('None') here
        result_df['_base_unit'].fill_null('None'),
        test_df['_base_unit'].fill_null('None'),
        check_names=False,
        # check_dtype=False
    )
    
    # Verify unit class
    assert_series_equal(
        result_df['_unit_class'],
        test_df['_unit_class'],
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
    unit_converter_test_data : pl.DataFrame
        Test fixture containing conversion test cases.
    caplog : pytest.LogCaptureFixture
        Pytest fixture for capturing log messages.

    Notes
    -----
    This test assumes weight_kg is already present in the input DataFrame
    (not testing vitals join functionality).
    """
    test_df: pl.DataFrame = unit_converter_test_data 

    input_df = test_df.select(['rn','med_dose', 'med_dose_unit', 'weight_kg'])
    
    # with caplog.at_level('WARNING'):
    base_df, _ = standardize_dose_to_base_units(med_df = input_df)
    base_df = base_df.sort('rn') # sort by rn to ensure the order of the rows is consistent
    
    # Verify columns exist
    assert '_clean_unit' in base_df.columns
    assert '_base_dose' in base_df.columns
    assert '_base_unit' in base_df.columns
    assert 'weight_kg' in base_df.columns

    # Verify base values
    assert_series_equal(
        base_df['_base_dose'], # actual
        test_df['_base_dose'], # expected
        check_names=False,
        #check_dtype=False
    )

    # Verify base units
    assert_series_equal(
        base_df['_base_unit'].fill_null('None'),
        _spelled(test_df['_base_unit']).fill_null('None'),
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
    unit_converter_test_data : pl.DataFrame
        Test fixture containing conversion test cases.
    caplog : pytest.LogCaptureFixture
        Pytest fixture for capturing log messages.

    Notes
    -----
    This test assumes the presence of _clean_unit column.
    """
    test_df: pl.DataFrame = unit_converter_test_data # .filter(pl.col('case') == 'valid')
    assert len(test_df) > 0
    q = """
    SELECT rn
        , _base_dose
        , _base_unit
        , _clean_unit as _preferred_unit
        , weight_kg
    FROM test_df
    """
    input_df = duckdb.sql(q).pl()
    
    result_df = _convert_base_units_to_preferred_units(med_df = input_df, override=True).pl()
    result_df = result_df.sort('rn') # sort by rn to ensure the order of the rows is consistent

    # Verify output columns exist
    assert 'med_dose_unit_converted' in result_df.columns
    assert 'med_dose_converted' in result_df.columns

    # Verify preferred values
    assert_series_equal(
        result_df['med_dose_converted'], # actual
        test_df['med_dose'], # expected
        check_names=False,
        # check_dtype=False
    ) 

def test__convert_base_units_to_preferred_units_new(convert_dose_units_by_med_category_test_data, caplog):
    """Test the _convert_base_units_to_preferred_units conversion function.

    Parameters
    ----------
    convert_dose_units_by_med_category_test_data : pl.DataFrame
        Test fixture containing conversion test cases by medication category.
    caplog : pytest.LogCaptureFixture
        Pytest fixture for capturing log messages.

    Notes
    -----
    This test assumes the presence of _preferred_unit column.
    """
    test_df: pl.DataFrame = convert_dose_units_by_med_category_test_data.filter(pl.col('case') == 'valid')
    assert len(test_df) > 0
    q = """
    SELECT rn
        , _base_dose
        , _base_unit
        , _preferred_unit
        , weight_kg
    FROM test_df
    """
    input_df = duckdb.sql(q).pl()
    
    result_df = _convert_base_units_to_preferred_units(med_df = input_df, override=True).pl()
    result_df = result_df.sort('rn') # sort by rn to ensure the order of the rows is consistent

    # Verify columns exist
    assert 'med_dose_unit_converted' in result_df.columns
    assert 'med_dose_converted' in result_df.columns

    # Verify converted dose values
    assert_series_equal(
        result_df['med_dose_converted'], # actual
        test_df['med_dose_converted'], # expected
        check_names=False,
        check_exact=False,
        rel_tol=1e-3,
        abs_tol=1e-5
    ) 
    
    # Verify converted units
    assert_series_equal(
        result_df['med_dose_unit_converted'], # actual
        test_df['med_dose_unit_converted'], # expected
        check_names=False,
        check_exact=False
    )

def test_convert_dose_units_by_med_category(convert_dose_units_by_med_category_test_data, caplog):
    """Test the public API function convert_dose_units_by_med_category.

    Tests the complete medication-specific unit conversion pipeline using
    a dictionary of preferred units for each medication category.

    Parameters
    ----------
    convert_dose_units_by_med_category_test_data : pl.DataFrame
        Test fixture containing conversion test cases by medication category.
    caplog : pytest.LogCaptureFixture
        Pytest fixture for capturing log messages.
    """
    test_df: pl.DataFrame = convert_dose_units_by_med_category_test_data
    
    input_df = test_df.select(['rn','med_category', 'med_dose', 'med_dose_unit', 'weight_kg'])
    
    preferred_units = {
        'propofol': 'mcg/kg/min',
        'propofol_lb': 'mcg/lb/min',
        'propofol_unweighted': 'mcg/min',
        'propofol_mcg': 'mcg',          # weighted amount → unweighted amount (rn=35, 36)
        'propofol_mcg_kg': 'mcg/kg',    # unweighted amount → weighted amount (rn=34)
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
    result_df = result_df.sort('rn') # sort by rn to ensure the order of the rows is consistent
    
    # check _convert_status
    assert_series_equal(
        result_df['_convert_status'], # actual
        test_df['_convert_status'], # expected
        check_names=False,
        # check_dtype=False
    )
    
    # check med_dose_preferred
    assert_series_equal(
        result_df['med_dose_converted'], # actual
        test_df['med_dose_converted'], # expected
        check_names=False,
        # check_dtype=False
        rel_tol=1e-3,
        abs_tol=1e-5
    )
    
    # check med_dose_unit_preferred: a failed row shows its cleaned source
    # unit, spelled out like every unit leaving clifpy
    assert_series_equal(
        result_df['med_dose_unit_converted'], # actual
        pl.Series(
            pl.select(
                pl.when(test_df['_convert_status'] == 'success')
                .then(test_df['med_dose_unit_converted'])
                .otherwise(_spelled(test_df['med_dose_unit_converted']))
            ).to_series()
        ), # expected
        check_names=False,
        # check_dtype=False
    )


def test_convert_dose_units_by_med_category_return_rel(convert_dose_units_by_med_category_test_data):
    """Test that return_format='duckdb' returns DuckDB relations instead of DataFrames.

    Validates that:

    1. Both return values are DuckDBPyRelation instances
    2. The data is equivalent when materialized to DataFrame
    3. Expected columns are present in the result
    """
    test_df: pl.DataFrame = convert_dose_units_by_med_category_test_data
    input_df = test_df.select(['rn','med_category', 'med_dose', 'med_dose_unit', 'weight_kg'])

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
        return_format='duckdb'
    )

    # Verify return types are DuckDB PyRelation
    assert isinstance(result_rel, duckdb.DuckDBPyRelation)
    assert isinstance(counts_rel, duckdb.DuckDBPyRelation)

    # Verify data is equivalent when materialized
    result_df = result_rel.pl()
    assert '_convert_status' in result_df.columns
    assert 'med_dose_converted' in result_df.columns
    assert 'med_dose_unit_converted' in result_df.columns

    # Verify counts can be materialized
    counts_df = counts_rel.pl()
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

# Each test below builds its own minimal in-test fixture (small pl.DataFrame
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
    med_df = pl.DataFrame({
        'rn': [0],
        'hospitalization_id': ['H_LAGGED'],
        'admin_dttm': [datetime.fromisoformat('2024-01-01 09:00')],
        'med_category': ['propofol_unweighted'],
        'med_dose': [10.0],
        'med_dose_unit': ['mcg/kg/min'],
    })
    vitals_df = pl.DataFrame({
        'hospitalization_id': ['H_LAGGED'],
        'recorded_dttm': [datetime.fromisoformat('2024-01-01 09:30')],
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
    assert result_no_fb['_convert_status'][0] == (
        'cannot convert weighted to unweighted: weight_kg is missing'
    )
    assert result_no_fb['_weight_source'][0] is None

    # --- fallback_on_earliest=True -> should succeed using the lagged 09:30 row ---
    result_fb, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        vitals_df=vitals_df,
        preferred_units=preferred_units,
        override=True,
        show_intermediate=True,
        fallback_on_earliest=True,
    )
    assert result_fb['_convert_status'][0] == 'success'
    assert result_fb['_weight_source'][0] == 'earliest_fallback'
    # 10 mcg/kg/min × 72 kg = 720 mcg/min
    assert result_fb['med_dose_converted'][0] == pytest.approx(720.0, rel=1e-6)
    assert result_fb['med_dose_unit_converted'][0] == 'mcg/min'


def test_temp_table_cleanup_after_call():
    """Test #2 of §6c: pandas-input -> the orchestrator promotes med_df into a
    per-call `_med_unit_input_<8 hex>` temp table (boundary 2a). With an eager
    return format, the `finally` block must drop every `_med_unit_*` table on exit.
    """
    # Sanity-check: drop any leftover _med_unit_* tables before the test.
    for (tname,) in duckdb.sql("SHOW TABLES").fetchall():
        if tname.startswith('_med_unit_'):
            duckdb.execute(f"DROP TABLE IF EXISTS {tname}")

    med_df = pl.DataFrame({
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
        return_format='polars',
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
    med_df = pl.DataFrame({
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
    assert result['med_dose_converted'][0] == dose
    assert result['med_dose_unit_converted'][0] == 'mcg/kg/min'
    assert result['_convert_status'][0] == 'success'


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
    med_df = pl.DataFrame({
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
    result = result.sort('rn')
    expected = pl.Series('_needs_wt', [r[3] for r in rows])
    assert_series_equal(
        result['_needs_wt'].cast(pl.Int64),
        expected,
        check_names=False,
    )


def test_kg_lb_constant_factor_no_weight():
    """Test #5 of §6c: explicit kg<->lb conversion with `weight_kg=NULL`.

    Both directions must succeed without consulting patient weight, using
    the `KG_PER_LB` constant. Covers stage-1 application (lb->kg) and
    stage-2 application (kg->lb).
    """
    med_df = pl.DataFrame({
        'rn': [0, 1],
        'med_category': ['kg_to_lb', 'lb_to_kg'],
        'med_dose': [100.0, 100.0],
        'med_dose_unit': ['mcg/kg/min', 'mcg/lb/hr'],
        'weight_kg': [None, None],
    }, schema_overrides={'weight_kg': pl.Float64})
    preferred_units = {
        'kg_to_lb': 'mcg/lb/min',  # factor 1/KG_PER_LB applied in stage 2
        'lb_to_kg': 'mcg/kg/min',  # factor KG_PER_LB applied in stage 1; stage 2 sees identity
    }

    result, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        preferred_units=preferred_units,
        override=True,
    )
    result = result.sort('rn')
    assert (result['_convert_status'] == 'success').all()

    # kg->lb: 100 / 2.20462 mcg/lb/min
    assert result['med_dose_converted'][0] == pytest.approx(
        100.0 / KG_PER_LB_LITERAL, rel=1e-9
    )
    assert result['med_dose_unit_converted'][0] == 'mcg/lb/min'

    # lb/hr -> kg/min: stage 1 collapses lb->kg via *KG_PER_LB and hr->min via /60.
    # 100 mcg/lb/hr  ==  (100 * KG_PER_LB / 60) mcg/kg/min
    assert result['med_dose_converted'][1] == pytest.approx(
        100.0 * KG_PER_LB_LITERAL / 60.0, rel=1e-9
    )
    assert result['med_dose_unit_converted'][1] == 'mcg/kg/min'


def test_user_prefilled_weight_kg_honored():
    """Test #6 of §6c: when `weight_kg` is in `med_df`, the orchestrator must
    skip the vitals lookup entirely. NULLs in the supplied column stay NULL,
    and only `_needs_wt=1 AND weight_kg IS NULL` rows fail.
    """
    med_df = pl.DataFrame({
        'rn': [0, 1, 2, 3],
        'med_category': ['propofol_unweighted'] * 4,
        'med_dose': [10.0, 10.0, 10.0, 10.0],
        'med_dose_unit': ['mcg/kg/min', 'mcg/kg/min', 'mcg/kg/min', 'mcg/kg/min'],
        'weight_kg': [70.0, None, 80.0, None],
    })

    # vitals_df=None must NOT trigger an error: user-prefilled weight wins.
    result, _ = convert_dose_units_by_med_category(
        med_df=med_df,
        vitals_df=None,
        preferred_units={'propofol_unweighted': 'mcg/min'},
        override=True,
    )
    result = result.sort('rn')

    # Rows 0, 2: weight present -> success.
    assert result['_convert_status'][0] == 'success'
    assert result['med_dose_converted'][0] == pytest.approx(700.0)
    assert result['_convert_status'][2] == 'success'
    assert result['med_dose_converted'][2] == pytest.approx(800.0)

    # Rows 1, 3: weight NULL -> weighted->unweighted failure (NULLs preserved).
    assert result['_convert_status'][1] == (
        'cannot convert weighted to unweighted: weight_kg is missing'
    )
    assert result['_convert_status'][3] == (
        'cannot convert weighted to unweighted: weight_kg is missing'
    )


def test_lazy_weight_join_skipped():
    """Test #7 of §6c: when no row needs weight, the `fetchone()` short-circuit
    must skip the vitals join entirely (vitals_df=None must NOT raise).
    """
    med_df = pl.DataFrame({
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
    med_df = pl.DataFrame({
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
    med_df = pl.DataFrame({
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
    med_df = pl.DataFrame({
        'rn': [0, 1, 2],
        'hospitalization_id': ['H_ASOF', 'H_LAGGED', 'H_NONE'],
        'admin_dttm': [
            datetime.fromisoformat('2024-01-01 10:00'),
            datetime.fromisoformat('2024-01-01 09:00'),
            datetime.fromisoformat('2024-01-01 12:00'),
        ],
        'med_category': ['propofol_unweighted'] * 3,
        'med_dose': [10.0, 10.0, 10.0],
        'med_dose_unit': ['mcg/kg/min'] * 3,
    })
    vitals_df = pl.DataFrame({
        'hospitalization_id': ['H_ASOF', 'H_LAGGED'],
        'recorded_dttm': [
            datetime.fromisoformat('2024-01-01 08:00'),  # before H_ASOF admin
            datetime.fromisoformat('2024-01-01 09:30'),  # after H_LAGGED admin
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
    by_id = {row['hospitalization_id']: row for row in result.iter_rows(named=True)}
    assert by_id['H_ASOF']['_weight_source'] == 'asof'
    assert by_id['H_LAGGED']['_weight_source'] == 'earliest_fallback'
    assert by_id['H_NONE']['_weight_source'] is None


# --- Lower priority ---------------------------------------------------------

def test_show_intermediate_surfaces_qa_columns():
    """Test #11 of §6c: `show_intermediate=True` must expose stage-2 multiplier
    columns (and the planning columns `_needs_wt`, `_base_wt`, `_pref_wt`).
    Default mode hides all of them.
    """
    med_df = pl.DataFrame({
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
        '_clean_unit',
        '_unit_class',
        '_weight_recorded_dttm',
    }
    assert hidden_cols.isdisjoint(default_result.columns), (
        f"Default mode leaked QA columns: "
        f"{hidden_cols & set(default_result.columns)}"
    )
    # the lean output keeps the weight's provenance: a pre-filled one
    assert default_result['_weight_source'].to_list() == ['prefilled']
    assert default_result['_weight_age_hours'].to_list() == [None]

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


# ===========================================
# Timezone preservation (issue #144)
# ===========================================
# The converter must NOT alter admin_dttm -- it changes dose columns only. On this
# (dev) branch the eager load returns tz-naive admin_dttm, which is immune to the
# ambient DuckDB default-connection zone, so these pass and GUARD that #144 stays
# fixed. The end-to-end orchestrator repro + full root-cause write-up live in
# tests/core/test_clif_orchestrator.py.

_DEMO_DIR = str(Path(__file__).parents[3] / "clifpy" / "data" / "clif_demo")


@pytest.fixture
def hostile_default_tz():
    """Pin DuckDB's *default* connection to a non-UTC zone for the test body.

    Reproduces the #144 ambient state on any runner regardless of its OS timezone:
    if the converter rendered a tz-aware ``admin_dttm`` through the default
    connection it would pick up this zone. Restores UTC on teardown.
    """
    duckdb.sql("SET TimeZone = 'America/Los_Angeles'")
    try:
        yield
    finally:
        duckdb.sql("SET TimeZone = 'UTC'")


@pytest.mark.parametrize("site_tz", ["UTC", "US/Eastern", "US/Central"])
def test_converter_preserves_admin_dttm_tz(site_tz, hostile_default_tz):
    """``convert_dose_units_by_med_category`` must leave ``admin_dttm``'s tz unchanged.

    Loads real demo continuous-med data in ``site_tz`` (polars, load_data's
    default), converts fentanyl under a hostile (Los Angeles) default
    connection, and asserts ``admin_dttm``'s timezone label and instants are
    identical before/after -- conversion touches dose columns only (issue #144).
    """
    med_df = load_data(
        "medication_admin_continuous", _DEMO_DIR, "parquet", site_tz=site_tz,
    ).filter(pl.col("med_category") == "fentanyl")
    assert not med_df.is_empty(), "expected fentanyl rows in demo continuous meds"
    src = med_df["admin_dttm"]

    converted, _ = convert_dose_units_by_med_category(
        med_df, preferred_units={"fentanyl": "mcg/min"}, override=True
    )
    out = converted["admin_dttm"]

    # #144 discriminator: the converter must not relabel the timezone.
    assert out.dtype.time_zone == src.dtype.time_zone, (
        f"converter changed admin_dttm tz {src.dtype.time_zone} -> {out.dtype.time_zone} (issue #144)"
    )
    # Instants (values) unchanged -- conversion changes dose columns only.
    assert sorted(out.drop_nulls().to_list()) == sorted(src.drop_nulls().to_list())
