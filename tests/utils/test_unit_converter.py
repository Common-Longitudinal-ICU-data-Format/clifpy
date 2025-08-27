"""
Tests for the unit_converter module.
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from clifpy.utils.unit_converter import (
    _normalize_dose_unit_formats,
    _normalize_dose_unit_names
)

# --- Helper Fixtures for CSV Loading ---
@pytest.fixture
def load_fixture_csv():
    """Load CSV fixture from tests/fixtures/unit_converter/"""
    def _load(filename):
        path = Path(__file__).parent.parent / 'fixtures' / 'unit_converter' / filename
        return pd.read_csv(path)
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
# Tests for `_normalize_dose_unit_names`
# ===========================================
def test_normalize_dose_unit_names(_normalize_dose_unit_names_test_data):
    """
    Test that the `_normalize_dose_unit_names` function correctly normalizes unit names.
    
    Validates normalization of:
    - Time units: 'hour', 'h' -> '/hr'; 'minute', 'm' -> '/min'
    - Volume units: 'liter', 'liters', 'litre', 'litres' -> 'l'
    - Unit units: 'units', 'unit' -> 'u'
    - Milli prefix: 'milli-units', 'milliunits' -> 'mu'
    
    Uses fixture data from test_normalize_dose_unit_names.csv for comprehensive testing.
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