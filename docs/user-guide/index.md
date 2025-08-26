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

# Each table is a self-contained unit
patient = Patient.from_file('/data', 'parquet')
labs = Labs.from_file('/data', 'parquet')
vitals = Vitals.from_file('/data', 'parquet')
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

## Common Workflows

### Loading and Validating Data

```python
from clifpy.clif_orchestrator import ClifOrchestrator

# Load multiple tables
orchestrator = ClifOrchestrator('/data', 'parquet', 'US/Central')
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
2. **Use appropriate timezones** for your data source
3. **Filter early** to reduce memory usage with large datasets
4. **Review validation errors** to understand data quality issues
5. **Use the orchestrator** when working with multiple related tables

## Next Steps

- Explore specific [table guides](tables/index.md)
- Learn about [data validation](validation.md)
- See [practical examples](../examples/index.md)
- Review the [API reference](../api/index.md)