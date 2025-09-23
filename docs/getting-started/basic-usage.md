# Basic Usage

This guide covers the fundamental patterns for working with CLIFpy.

## Core Concepts

### Table Classes

Each CLIF table is represented by a Python class that inherits from `BaseTable`:

- `Patient` - Demographics and patient identification
- `Adt` - Admission, discharge, and transfer events
- `Hospitalization` - Hospital stay information
- `Labs` - Laboratory test results
- `Vitals` - Vital signs measurements
- `RespiratorySupport` - Ventilation and oxygen therapy
- `MedicationAdminContinuous` - Continuous infusions
- `MicrobiologyCulture` - Microbiology culture results
- `PatientAssessments` - Clinical assessment scores
- `Position` - Patient positioning

### Data Loading

All tables support two loading methods:

```python
# Method 1: From files
table = TableClass.from_file(
    data_directory='/path/to/data',
    filetype='parquet',  # or 'csv'
    timezone='US/Central'
)

# Method 2: From existing DataFrame
table = TableClass(
    data=existing_dataframe,
    timezone='US/Central'
)
```

### Validation

Every table includes built-in validation:

```python
# Run validation
table.validate()

# Check if valid
if table.isvalid():
    print("Validation passed!")
else:
    # Review errors
    for error in table.errors[:5]:
        print(f"{error['type']}: {error['message']}")
```

## Working with DataFrames

All table data is accessible via the `df` attribute:

```python
# Access the underlying DataFrame
df = table.df

# Use standard pandas operations
print(df.shape)
print(df.columns.tolist())
print(df.dtypes)

# Filter data
filtered = df[df['some_column'] > threshold]
```

## Common Operations

### Date Range Filtering

Most tables with datetime columns support date range filtering:

```python
from datetime import datetime

# Filter by date range
start = datetime(2023, 1, 1)
end = datetime(2023, 12, 31)

# For tables with custom methods
filtered = table.filter_by_date_range(start, end)

# Or using pandas
mask = (df['datetime_column'] >= start) & (df['datetime_column'] <= end)
filtered = df[mask]
```

### Category Filtering

Tables with standardized categories provide filtering methods:

```python
# Labs by category
chemistry = labs.filter_by_category('chemistry')
hematology = labs.filter_by_category('hematology')

# ADT by location
icu_stays = adt.filter_by_location_category('icu')
ed_visits = adt.filter_by_location_category('ed')

# Medications by group
vasopressors = meds.filter_by_med_group('vasopressor')
sedatives = meds.filter_by_med_group('sedative')
```

### Patient-specific Data

```python
# Single patient
patient_id = 'P12345'
patient_labs = labs.df[labs.df['patient_id'] == patient_id]

# Multiple patients
patient_ids = ['P001', 'P002', 'P003']
cohort_data = vitals.df[vitals.df['patient_id'].isin(patient_ids)]
```

## Output and Reporting

### Summary Statistics

```python
# Get table summary
summary = table.get_summary()
print(f"Rows: {summary['num_rows']}")
print(f"Columns: {summary['num_columns']}")
print(f"Memory usage: {summary['memory_usage_mb']:.2f} MB")

# Save summary to file
table.save_summary()
```

### Validation Reports

Validation results are automatically saved to the output directory:

```python
# Set custom output directory
table = TableClass.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    output_directory='/path/to/reports'
)

# After validation, check output files:
# - validation_log_[table_name].log
# - validation_errors_[table_name].csv
# - missing_data_stats_[table_name].csv
```

## Timezone Handling

CLIFpy ensures consistent timezone handling:

```python
# Specify timezone when loading
table = TableClass.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Central'  # All datetime columns converted to this timezone
)

# Datetime columns are timezone-aware
print(table.df['datetime_column'].dt.tz)
```

## Memory Management

For large datasets:

```python
# Load only specific columns
table = TableClass.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    columns=['patient_id', 'datetime', 'value']
)

# Load a sample
table = TableClass.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    sample_size=10000
)

# Apply filters during loading
table = TableClass.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    filters={'patient_id': patient_list}
)
```

## Error Handling

```python
try:
    table = TableClass.from_file('/path/to/data', 'parquet')
    table.validate()
    
    if not table.isvalid():
        # Handle validation errors
        error_df = pd.DataFrame(table.errors)
        error_df.to_csv('validation_errors.csv', index=False)
        
except FileNotFoundError:
    print("Data files not found")
except Exception as e:
    print(f"Error loading data: {e}")
```

## Next Steps

- [Explore the full User Guide](../user-guide/index.md)
- [Learn about the Orchestrator](../user-guide/orchestrator.md)
- [See table-specific guides](../user-guide/tables/index.md)
- [View practical examples](../examples/index.md)