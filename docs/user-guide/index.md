# User Guide

Welcome to the CLIFpy User Guide. This guide provides comprehensive documentation for working with CLIF data using CLIFpy.

## Overview

CLIFpy is designed to make working with CLIF (Common Longitudinal ICU data Format) data straightforward and efficient. Whether you're a researcher analyzing ICU outcomes, a data scientist building predictive models, or a clinician exploring patient data, this guide will help you make the most of CLIFpy.

## Guide Organization

### [CLIF Orchestrator](orchestrator.md)
Learn how to manage multiple CLIF tables simultaneously with consistent configuration and validation.

### [Wide Dataset Creation](wide-dataset.md)
Create comprehensive time-series datasets by joining multiple CLIF tables with automatic pivoting and high-performance processing.

### [Outlier Handling](outlier-handling.md)
Detect and remove physiologically implausible values using configurable ranges and category-specific validation.

### [Tables](tables/index.md)
Detailed guides for each CLIF table type: 

- Patient demographics
- ADT (Admission, Discharge, Transfer) events
- Hospitalization information
- Laboratory results
- Vital signs
- Respiratory support
- Medication administration
- Clinical assessments
- Patient positioning

### [Data Validation](validation.md)
Understand how CLIFpy validates your data against CLIF schemas and how to interpret validation results.

### [Working with Timezones](timezones.md)
Learn best practices for handling timezone-aware datetime data across different hospital systems.

## Key Concepts

### Table-Based Architecture

CLIFpy organizes ICU data into standardized tables, each representing a specific aspect of patient care:

```python
from clifpy.tables import Patient, Labs, Vitals

# Method 1: Direct parameters (traditional)
patient = Patient.from_file('/data', 'parquet', timezone='US/Eastern')
labs = Labs.from_file('/data', 'parquet', timezone='US/Eastern')

# Method 2: Using configuration file (recommended)
patient = Patient.from_file(config_path='./clif_config.json')
labs = Labs.from_file(config_path='./clif_config.json')
```

### Consistent Interface

All tables share common methods inherited from `BaseTable`: 

- `from_file()` - Load data from files
- `validate()` - Run comprehensive validation
- `isvalid()` - Check validation status
- `get_summary()` - Get table statistics

### Standardized Categories

CLIF defines standardized categories for consistent data representation:
- Lab categories: chemistry, hematology, coagulation, etc.
- Location categories: icu, ward, ed, etc.
- Medication groups: vasopressor, sedative, antibiotic, etc.

### Timezone Awareness

All datetime columns are timezone-aware to handle data from different time zones correctly:

```python
# Specify timezone when loading
table = TableClass.from_file(
    data_directory='/data',
    filetype='parquet',
    timezone='US/Central'
)
```

## Configuration Files

CLIFpy supports configuration files for easier data loading and consistent settings across projects. You can use a `clif_config.json` file to centralize your configuration:

### Configuration Structure

Create a `clif_config.json` file with the following structure:

```json
{
  "data_directory": "/path/to/data",
  "filetype": "parquet",
  "timezone": "US/Eastern", 
  "output_directory": "/path/to/output"  // optional
}
```

### Using Configuration Files

Load tables using the config file:

```python
from clifpy.tables import Patient, Labs, Vitals

# Using configuration file
patient = Patient.from_file(config_path='./clif_config.json')
labs = Labs.from_file(config_path='./clif_config.json')

# Or with the orchestrator
from clifpy.clif_orchestrator import ClifOrchestrator
orchestrator = ClifOrchestrator(config_path='./clif_config.json')
```

You can still override specific parameters when needed:

```python
# Use config but override timezone and add sampling
vitals = Vitals.from_file(
    config_path='./clif_config.json',
    timezone='UTC',
    sample_size=1000
)
```

## Common Workflows

### Loading and Validating Data

```python
from clifpy.clif_orchestrator import ClifOrchestrator

# Method 1: Direct parameters
orchestrator = ClifOrchestrator('/data', 'parquet', 'US/Central')

# Method 2: Using configuration file (recommended)
orchestrator = ClifOrchestrator(config_path='./clif_config.json')

# Both methods work the same way after initialization
orchestrator.initialize(tables=['patient', 'labs', 'vitals'])

# Validate all tables
orchestrator.validate_all()

# Check validation status
for table_name in orchestrator.get_loaded_tables():
    table = getattr(orchestrator, table_name)
    print(f"{table_name}: {'Valid' if table.isvalid() else 'Invalid'}")
```

### Filtering and Analysis

```python
# Category-based filtering
icu_stays = adt.filter_by_location_category('icu')

# Patient cohort analysis 
cohort_ids = ['P001', 'P002', 'P003']
cohort_vitals = vitals.df[vitals.df['hospitalization_id'].isin(cohort_ids)]
```

## Best Practices

1. **Always validate data** after loading to ensure compliance with CLIF standards
2. **Use configuration files** for consistent settings across your project (create `clif_config.json`)
3. **Use appropriate timezones** for your data source
4. **Filter early** to reduce memory usage with large datasets
5. **Review validation errors** to understand data quality issues
6. **Use the orchestrator** when working with multiple related tables

## Next Steps

- Explore specific [table guides](tables/index.md)
- Learn about [data validation](validation.md)
- See [practical examples](../examples/index.md)
- Review the [API reference](../api/index.md)