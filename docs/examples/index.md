# Examples

This section provides practical examples of using CLIFpy for common ICU data analysis tasks.

## Available Examples

### [Loading Data](loading-data.md)
Learn different ways to load CLIF data, including:
- Loading from CSV and Parquet files
- Using filters and column selection
- Working with sample data
- Handling large datasets efficiently

### [Analyzing ICU Stays](icu-analysis.md)
Common ICU analysis patterns:
- Identifying ICU admissions
- Calculating length of stay
- Tracking patient movement
- Analyzing severity of illness

### [Clinical Calculations](clinical-calculations.md)
Implement clinical calculations and scores:
- Calculating SOFA scores
- Tracking vasopressor requirements
- Monitoring ventilation parameters
- Assessing prone positioning compliance

## Quick Examples

### Basic Data Loading

```python
from clifpy.tables import Patient, Labs, Vitals
from clifpy.clif_orchestrator import ClifOrchestrator

# Load individual tables
patient = Patient.from_file('/data', 'parquet', timezone='US/Central')
labs = Labs.from_file('/data', 'parquet', timezone='US/Central')

# Or use orchestrator for multiple tables
orchestrator = ClifOrchestrator('/data', 'parquet', 'US/Central')
orchestrator.initialize(tables=['patient', 'labs', 'vitals', 'adt'])
```

### Finding ICU Patients

```python
# Get ICU admissions
icu_stays = orchestrator.adt.filter_by_location_category('icu')
icu_patients = icu_stays['patient_id'].unique()

# Get their demographics
icu_demographics = orchestrator.patient.df[
    orchestrator.patient.df['patient_id'].isin(icu_patients)
]
```

### Analyzing Lab Trends

```python
# Get recent abnormal labs
recent_labs = orchestrator.labs.get_recent(hours=24)
abnormal = recent_labs[
    (recent_labs['lab_name'] == 'creatinine') & 
    (recent_labs['lab_value'] > 2.0)
]

# Track patient's lab trend
patient_labs = orchestrator.labs.df[
    orchestrator.labs.df['patient_id'] == 'P12345'
].sort_values('lab_datetime')
```

### Medication Analysis

```python
# Find patients on multiple vasopressors
vasopressors = orchestrator.medication_admin_continuous.filter_by_med_group('vasopressor')
concurrent = orchestrator.medication_admin_continuous.get_concurrent_medications('P12345')
multi_pressor = concurrent[concurrent['medication_group'] == 'vasopressor']
```

## Example Notebooks

The repository includes Jupyter notebooks demonstrating:
- `labs_demo.ipynb` - Laboratory data analysis
- `respiratory_support_demo.ipynb` - Ventilation analysis
- `position_demo.ipynb` - Prone positioning analysis

## Next Steps

- Explore specific examples in detail
- Review the [API documentation](../api/index.md)
- See the [User Guide](../user-guide/index.md) for comprehensive coverage