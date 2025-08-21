# Data Validation

CLIFpy provides comprehensive validation to ensure your data conforms to CLIF standards. This guide explains the validation process and how to interpret results.

## Overview

Validation in CLIFpy operates at multiple levels:

1. **Schema Validation** - Ensures required columns exist with correct data types
2. **Category Validation** - Verifies values match standardized categories
3. **Range Validation** - Checks values fall within clinically reasonable ranges
4. **Timezone Validation** - Ensures datetime columns are timezone-aware
5. **Duplicate Detection** - Identifies duplicate records based on composite keys
6. **Completeness Checks** - Analyzes missing data patterns

## Running Validation

### Basic Validation

```python
# Load and validate a table
table = TableClass.from_file('/data', 'parquet')
table.validate()

# Check if valid
if table.isvalid():
    print("Validation passed!")
else:
    print(f"Found {len(table.errors)} validation errors")
```

### Bulk Validation with Orchestrator

```python
from clifpy.clif_orchestrator import ClifOrchestrator

orchestrator = ClifOrchestrator('/data', 'parquet')
orchestrator.initialize(tables=['patient', 'labs', 'vitals'])

# Validate all tables
orchestrator.validate_all()
```

## Understanding Validation Results

### Error Types

Validation errors are stored in the `errors` attribute:

```python
# Review errors
for error in table.errors[:10]:  # First 10 errors
    print(f"Type: {error['type']}")
    print(f"Message: {error['message']}")
    print(f"Details: {error.get('details', 'N/A')}")
    print("-" * 50)
```

Common error types:
- `missing_column` - Required column not found
- `invalid_category` - Value not in permissible list
- `out_of_range` - Value outside acceptable range
- `invalid_timezone` - Datetime column not timezone-aware
- `duplicate_rows` - Duplicate records found

### Validation Reports

Validation results are automatically saved to the output directory:

```python
# Set custom output directory
table = TableClass.from_file(
    data_directory='/data',
    filetype='parquet',
    output_directory='/path/to/reports'
)

# After validation, these files are created:
# - validation_log_[table_name].log
# - validation_errors_[table_name].csv
# - missing_data_stats_[table_name].csv
```

## Schema Validation

Each table has a YAML schema defining its structure:

```yaml
# Example from patient_schema.yaml
columns:
  - name: patient_id
    data_type: VARCHAR
    required: true
    is_category_column: false
  - name: sex_category
    data_type: VARCHAR
    required: true
    is_category_column: true
    permissible_values:
      - Male
      - Female
      - Unknown
```

### Required Columns

```python
# Check which required columns are missing
if not table.isvalid():
    missing_cols = [e for e in table.errors if e['type'] == 'missing_column']
    for error in missing_cols:
        print(f"Missing required column: {error['column']}")
```

### Data Types

CLIFpy validates that columns have appropriate data types:
- `VARCHAR` - String/text data
- `DATETIME` - Timezone-aware datetime
- `NUMERIC` - Numeric values (int or float)

## Category Validation

Standardized categories ensure consistency across institutions:

```python
# Example: Validating location categories in ADT
valid_locations = ['ed', 'ward', 'stepdown', 'icu', 'procedural', 
                   'l&d', 'hospice', 'psych', 'rehab', 'radiology', 
                   'dialysis', 'other']

# Check for invalid categories
category_errors = [e for e in table.errors 
                   if e['type'] == 'invalid_category']
```

## Range Validation

Clinical values are checked against reasonable ranges:

```python
# Example: Vital signs ranges
ranges = {
    'heart_rate': (0, 300),
    'sbp': (0, 300),
    'dbp': (0, 200),
    'temp_c': (25, 44),
    'spo2': (50, 100)
}

# Identify out-of-range values
range_errors = [e for e in table.errors 
                if e['type'] == 'out_of_range']
```

## Timezone Validation

All datetime columns must be timezone-aware:

```python
# Check timezone issues
tz_errors = [e for e in table.errors 
             if 'timezone' in e.get('message', '').lower()]

if tz_errors:
    print("Datetime columns must be timezone-aware")
    print("Consider reloading with explicit timezone:")
    print("table = TableClass.from_file('/data', 'parquet', timezone='US/Central')")
```

## Duplicate Detection

Duplicates are identified based on composite keys:

```python
# Check for duplicates
duplicate_errors = [e for e in table.errors 
                    if e['type'] == 'duplicate_rows']

if duplicate_errors:
    for error in duplicate_errors:
        print(f"Found {error['count']} duplicate rows")
        print(f"Composite keys: {error['keys']}")
```

## Missing Data Analysis

CLIFpy analyzes missing data patterns:

```python
# Get missing data statistics
summary = table.get_summary()
if 'missing_data' in summary:
    print("Columns with missing data:")
    for col, count in summary['missing_data'].items():
        pct = (count / summary['num_rows']) * 100
        print(f"  {col}: {count} ({pct:.1f}%)")
```

## Custom Validation

Tables may include specific validation logic:

```python
# Example: Labs table validates reference ranges
# Example: Medications validates dose units match drug
# Example: Respiratory support validates device/mode combinations
```

## Best Practices

1. **Always validate after loading** - Catch issues early
2. **Review all error types** - Don't just check if valid
3. **Save validation reports** - Keep audit trail
4. **Fix data at source** - Update extraction/ETL process
5. **Document exceptions** - Some errors may be acceptable

## Handling Validation Errors

### Option 1: Fix and Reload

```python
# Identify issues
table.validate()
errors_df = pd.DataFrame(table.errors)
errors_df.to_csv('validation_errors.csv', index=False)

# Fix source data based on errors
# Then reload
table = TableClass.from_file('/fixed_data', 'parquet')
table.validate()
```

### Option 2: Filter Invalid Records

```python
# Remove records with invalid categories
valid_categories = ['Male', 'Female', 'Unknown']
cleaned_df = table.df[table.df['sex_category'].isin(valid_categories)]

# Create new table instance with cleaned data
table = TableClass(data=cleaned_df, timezone='US/Central')
```

### Option 3: Document and Proceed

```python
# For acceptable validation errors
if not table.isvalid():
    # Document why proceeding despite errors
    with open('validation_notes.txt', 'w') as f:
        f.write(f"Proceeding with {len(table.errors)} known issues:\n")
        f.write("- Missing optional columns\n")
        f.write("- Historical data outside current ranges\n")
```

## Next Steps

- Learn about [timezone handling](timezones.md)
- Explore [table-specific guides](tables/index.md)
- See [practical examples](../examples/index.md)