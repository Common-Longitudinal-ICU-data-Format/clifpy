# Testing Guidelines for CLIF Tables

This guide outlines testing patterns for CLIF table classes, using the `MedicationAdminContinuous` tests as a reference implementation.

## Test Organization

### Directory Structure
```
tests/
├── tables/
│   └── test_<table_name>.py         # Main test file
└── fixtures/
    └── <table_name>/
        └── *.csv                     # Test data files
```

### Test Markers
Use pytest markers to group related tests:
```python
@pytest.mark.unit_conversion  # For conversion-related tests
@pytest.mark.usefixtures("patch_med_admin_continuous_schema_path")
```

This allows targeted test execution:
```bash
pytest -m unit_conversion -xvs  # -m: run only marked tests, -x: stop on first failure, -v: verbose, -s: show print statements
```

## Test Data Management

### CSV Fixtures
Store test data in CSV files for better readability and maintenance:

```python
@pytest.fixture
def load_fixture_csv():
    """Load CSV fixture from tests/fixtures/<table_name>/"""
    def _load(filename):
        path = Path(__file__).parent.parent / 'fixtures' / '<table_name>' / filename
        return pd.read_csv(path)
    return _load
```

### CSV Structure Example
`test_normalize_dose_unit_pattern.csv`:

| case    | med_dose_unit    | med_dose_unit_clean |
|---------|------------------|---------------------|
| valid   | ML/HR            | ml/hr               |
| valid   | mcg / kg/ min    | mcg/kg/min          |
| invalid | tablespoon/hr    | tablespoon/hr       |

Use a 'case' column to group related test scenarios in the same file.

## Test Patterns

### 1. Property Testing
Test that properties return expected values and handle edge cases:

```python
def test_acceptable_dose_unit_patterns():
    """Test valid and invalid patterns"""
    mac_obj = MedicationAdminContinuous()
    patterns = mac_obj._acceptable_dose_unit_patterns
    
    # Valid patterns
    assert 'mcg/kg/hr' in patterns
    assert 'mg/min' in patterns
    
    # Invalid patterns
    assert 'mcg/lb/min' not in patterns
    assert 'invalid/unit' not in patterns
```

### 2. Method Testing with Fixtures
Load test data from CSV and verify transformations:

```python
def test_normalize_dose_unit_pattern_recognized(normalize_dose_unit_pattern_test_data):
    """Test unit normalization for recognized patterns"""
    mac_obj = MedicationAdminContinuous()
    test_df = normalize_dose_unit_pattern_test_data.query("case == 'valid'")
    result_df, unrecognized = mac_obj._normalize_dose_unit_pattern(test_df[['med_dose_unit']])
    
    pd.testing.assert_series_equal(
        result_df['med_dose_unit_clean'].reset_index(drop=True),
        test_df['med_dose_unit_clean'].reset_index(drop=True),
        check_names=False
    )
    assert unrecognized is False  # All units should be recognized
```

### 3. Error Handling
Test that methods raise appropriate errors:

```python
def test_method_no_data_provided():
    """Test ValueError when no data provided"""
    obj = TableClass()
    # obj.df is None since no data was provided
    with pytest.raises(ValueError, match="No data provided"):
        obj.some_method()
```

### 4. Edge Cases
Always test:
- Empty DataFrames
- Missing required columns
- None/NaN values
- Data type mismatches

```python
def test_method_empty_dataframe():
    """Test handling of empty DataFrame"""
    obj = TableClass()
    empty_df = pd.DataFrame({'required_col': pd.Series([], dtype='object')})
    result = obj.some_method(empty_df)
    assert result.empty or result is not None  # Define expected behavior
```

### 5. Integration Testing
Test methods that depend on external data (e.g., vitals for weight-based calculations):

```python
def test_convert_with_vitals(convert_test_data, vitals_mock_data):
    """Test conversion using external vitals data"""
    obj = TableClass()
    result = obj.convert_dose_to_limited_units(
        vitals_df=vitals_mock_data,
        med_df=convert_test_data
    )
    # Verify the integration worked correctly
    assert 'weight_kg' in result.columns
```

## Test Documentation

### Docstrings
Write clear docstrings explaining what each test validates:

```python
def test_feature():
    """
    Test that feature X correctly handles Y scenario.
    
    Validates:
    - Condition A produces result B
    - Edge case C is handled gracefully
    """
```

### Logging Validation
Check that warnings and info messages are logged correctly:

```python
def test_warning_logged(caplog):
    """Test that warnings are logged for unrecognized data"""
    with caplog.at_level('WARNING'):
        obj.method_that_warns()
    assert "not recognized by the converter" in caplog.text
```

## Running Tests

### Full Test Suite
```bash
pytest tests/tables/test_<table_name>.py -xvs  # -x: stop on first failure, -v: verbose, -s: show print statements
```

### Specific Test Functions
```bash
pytest tests/tables/test_<table_name>.py::test_specific_function -xvs  # :: selects specific test function
```

### With Markers
```bash
pytest -m <marker_name> -xvs  # -m: filter by marker, -x: stop on first failure, -v: verbose, -s: show print statements
```

## Best Practices

1. **One assertion per test concept** - Group related assertions, but test one logical concept
2. **Use fixtures for reusable test data** - Avoid duplicating test data setup
3. **Test both success and failure paths** - Verify correct behavior and error handling
4. **Keep tests independent** - Each test should run successfully in isolation
5. **Use descriptive test names** - `test_<method>_<scenario>` pattern
6. **Mock external dependencies** - Use monkeypatch for file I/O, external APIs
7. **Test data should be minimal but complete** - Include only fields relevant to the test

## Example Test Implementation

See `tests/tables/test_medication_admin_continuous.py` for a complete implementation following these patterns. Key highlights:

- CSV fixtures in `tests/fixtures/medication_admin_continuous/`
- Grouped tests with pytest markers
- Error handling tests for missing data scenarios
- Integration tests with vitals data
- Clear separation between recognized and unrecognized test cases
- Proper terminology using "recognized/unrecognized" for pattern matching

This approach ensures consistent, maintainable tests across all CLIF table classes.