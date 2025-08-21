# Quick Start

This guide will get you up and running with CLIFpy in just a few minutes.

## Loading Demo Data

CLIFpy includes demo data to help you get started:

```python
from clifpy.data import load_dataset

# Load all demo tables
tables = load_dataset()

# Access individual tables
patient_df = tables['patient']
labs_df = tables['labs']
vitals_df = tables['vitals']
```

## Using Individual Tables

### Loading a Single Table

```python
from clifpy.tables import Patient

# Load patient data from files
patient = Patient.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Central'
)

# Validate the data
patient.validate()

# Check if data is valid
if patient.isvalid():
    print("Data validation passed!")
else:
    print(f"Found {len(patient.errors)} validation errors")
```

### Working with Lab Data

```python
from clifpy.tables import Labs

# Load lab data
labs = Labs.from_file('/path/to/data', 'parquet')

# Get recent lab results
recent_labs = labs.get_recent(hours=24)

# Filter by lab category
chemistry_labs = labs.filter_by_category('chemistry')

# Get common lab panels
cbc = labs.get_common_labs('cbc')
bmp = labs.get_common_labs('bmp')
```

### Analyzing Vital Signs

```python
from clifpy.tables import Vitals

# Load vitals data
vitals = Vitals.from_file('/path/to/data', 'parquet')

# Get specific vital types
heart_rates = vitals.filter_by_vital_type('heart_rate')
blood_pressures = vitals.filter_by_vital_type('sbp')

# Calculate summary statistics
hr_stats = vitals.get_summary_by_vital_type()
print(hr_stats)
```

## Using the Orchestrator

For working with multiple tables at once:

```python
from clifpy.clif_orchestrator import ClifOrchestrator

# Initialize orchestrator
orchestrator = ClifOrchestrator(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Central'
)

# Load multiple tables
orchestrator.initialize(
    tables=['patient', 'labs', 'vitals', 'adt'],
    sample_size=1000  # Optional: load sample for testing
)

# Validate all tables
orchestrator.validate_all()

# Get summary of loaded tables
loaded = orchestrator.get_loaded_tables()
print(f"Loaded tables: {loaded}")
```

## Common Patterns

### Filtering by Patient

```python
# Get data for specific patients
patient_ids = ['P001', 'P002', 'P003']

# Filter labs
patient_labs = labs.df[labs.df['patient_id'].isin(patient_ids)]

# Filter vitals
patient_vitals = vitals.df[vitals.df['patient_id'].isin(patient_ids)]
```

### Time-based Analysis

```python
from datetime import datetime, timedelta

# Get data from the last 7 days
end_date = datetime.now()
start_date = end_date - timedelta(days=7)

# Filter ADT movements
recent_movements = adt.filter_by_date_range(start_date, end_date)

# Get ICU admissions
icu_admissions = adt.filter_by_location_category('icu')
```

### Clinical Calculations

```python
# Calculate SOFA scores
from clifpy.tables import PatientAssessments

assessments = PatientAssessments.from_file('/path/to/data', 'parquet')

# Get assessment trends
gcs_trend = assessments.get_assessment_trend(
    patient_id='P001',
    assessment_category='neurological',
    hours=48
)

# Check compliance
compliance = assessments.get_assessment_compliance(
    assessment_category='pain',
    expected_frequency_hours=4
)
```

## Next Steps

- [Learn about basic usage patterns](basic-usage.md)
- [Explore the full User Guide](../user-guide/index.md)
- [See more examples](../examples/index.md)
- [Read the API documentation](../api/index.md)