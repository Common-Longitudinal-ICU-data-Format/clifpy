# CLIF Orchestrator

The `ClifOrchestrator` class provides a centralized interface for managing multiple CLIF tables with consistent configuration. This guide covers how to use the orchestrator effectively.

## Overview

The orchestrator simplifies working with multiple CLIF tables by:

- Ensuring consistent configuration across all tables
- Providing bulk operations (load, validate)
- Managing shared settings (timezone, file format, output directory)
- Offering a unified interface for multi-table workflows

## Basic Usage

### Initialization

```python
from clifpy.clif_orchestrator import ClifOrchestrator

# Create orchestrator with your data configuration
orchestrator = ClifOrchestrator(
    data_directory='/path/to/clif/data',
    filetype='parquet',  # or 'csv'
    timezone='US/Central',
    output_directory='/path/to/outputs'  # Optional
)
```

### Loading Tables

```python
# Load specific tables
orchestrator.initialize(tables=['patient', 'labs', 'vitals'])

# Load all available tables
all_tables = ['patient', 'hospitalization', 'adt', 'labs', 'vitals',
              'medication_admin_continuous', 'patient_assessments',
              'respiratory_support', 'position']
orchestrator.initialize(tables=all_tables)

# Load with sampling (useful for testing)
orchestrator.initialize(
    tables=['patient', 'labs'],
    sample_size=1000
)
```

### Accessing Tables

Once loaded, tables are available as attributes:

```python
# Access individual tables
patient_data = orchestrator.patient
labs_data = orchestrator.labs
vitals_data = orchestrator.vitals

# Get the underlying DataFrames
patient_df = orchestrator.patient.df
labs_df = orchestrator.labs.df
```

## Advanced Usage

### Selective Column Loading

Load only specific columns to reduce memory usage:

```python
orchestrator.initialize(
    tables=['labs', 'vitals'],
    columns={
        'labs': ['hospitalization_id', 'lab_result_dttm', 'lab_value', 'lab_name'],
        'vitals': ['hospitalization_id', 'recorded_dttm', 'vital_value']
    }
)
```

### Filtered Loading

Apply filters during loading:

```python
# Filter by patient IDs
hospitalization_ids = ['P001', 'P002', 'P003']
orchestrator.initialize(
    tables=['labs', 'vitals'],
    filters={
        'labs': {'hospitalization_id': patient_ids},
        'vitals': {'hospitalization_id': patient_ids}
    }
)

# Filter by categories
orchestrator.initialize(
    tables=['labs', 'adt'],
    filters={
        'labs': {'lab_category': 'lactate'},
        'adt': {'location_category': 'icu'}
    }
)
```

### Validation Workflow

```python
# Validate all loaded tables
orchestrator.validate_all()

# Check which tables are valid
for table_name in orchestrator.get_loaded_tables():
    table = getattr(orchestrator, table_name)
    if table.isvalid():
        print(f"✓ {table_name} passed validation")
    else:
        print(f"✗ {table_name} has {len(table.errors)} errors")
```

## Utility Methods

### Get Loaded Tables

```python
# List of loaded table names
loaded = orchestrator.get_loaded_tables()
print(f"Loaded tables: {', '.join(loaded)}")

# Get table objects
table_objects = orchestrator.get_tables_obj_list()
for table in table_objects:
    print(f"{table.table_name}: {len(table.df)} rows")
```

### Individual Table Loading

You can also load tables individually:

```python
# Load tables one at a time
orchestrator.load_patient_data(sample_size=1000)
orchestrator.load_labs_data(
    columns=['hospitalization_id', 'lab_result_dttm', 'lab_value']
)
orchestrator.load_vitals_data(
    filters={'vital_category': ['heart_rate', 'sbp', 'dbp']}
)
```

## Common Patterns

### Multi-table Analysis

```python
# Load related tables for ICU analysis
orchestrator.initialize(
    tables=['patient', 'adt', 'labs', 'vitals', 'respiratory_support']
)

# Get ICU patients
icu_stays = orchestrator.adt.filter_by_location_category('icu')
icu_patient_ids = icu_stays['patient_id'].unique()

# Analyze their labs
icu_labs = orchestrator.labs.df[
    orchestrator.labs.df['patient_id'].isin(icu_patient_ids)
]

# Check ventilation status
vent_patients = orchestrator.respiratory_support.df[
    orchestrator.respiratory_support.df['device_category'] == 'IMV'
]['patient_id'].unique()
```

## Best Practices

1. **Load only what you need**: Use column and filter parameters to reduce memory usage
2. **Validate early**: Run validation immediately after loading to catch issues
3. **Use consistent timezones**: The orchestrator ensures all tables use the same timezone
4. **Check output directory**: Validation reports and logs are saved to the output directory
5. **Handle missing tables gracefully**: Check if a table is loaded before accessing it

## Error Handling

```python
# Check if table is loaded
if orchestrator.labs is not None:
    labs_data = orchestrator.labs.df
else:
    print("Labs table not loaded")

# Handle validation errors
orchestrator.validate_all()
for table_name in orchestrator.get_loaded_tables():
    table = getattr(orchestrator, table_name)
    if not table.isvalid():
        # Save errors for review
        error_file = f"{table_name}_errors.csv"
        pd.DataFrame(table.errors).to_csv(error_file, index=False)
        print(f"Saved {len(table.errors)} errors to {error_file}")
```

## Next Steps

- Learn about individual [table types](tables/index.md)
- Understand [data validation](validation.md)
- See [practical examples](../examples/index.md)