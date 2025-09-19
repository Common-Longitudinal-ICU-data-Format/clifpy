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
icu_stays = orchestrator.adt.df[orchestrator.adt.df['location_category'] == 'icu']
icu_patients = icu_stays['hospitalization_id'].unique()

# Get their demographics
icu_demographics = orchestrator.patient.df[
    orchestrator.patient.df['patient_id'].isin(
        orchestrator.hospitalization.df[
            orchestrator.hospitalization.df['hospitalization_id'].isin(icu_patients)
        ]['patient_id']
    )
]
```

### Analyzing Lab Trends

```python
# Get recent abnormal labs (last 24 hours)
from datetime import datetime, timedelta
cutoff_time = datetime.now() - timedelta(hours=24)
recent_labs = orchestrator.labs.df[
    orchestrator.labs.df['lab_datetime'] >= cutoff_time
]
abnormal = recent_labs[
    (recent_labs['lab_name'] == 'creatinine') & 
    (recent_labs['lab_value'] > 2.0)
]

# Track patient's lab trend
patient_labs = orchestrator.labs.df[
    orchestrator.labs.df['hospitalization_id'] == 'H12345'
].sort_values('lab_datetime')
```

### Medication Analysis

```python
# Find patients on vasopressors
vasopressors = orchestrator.medication_admin_continuous.df[
    orchestrator.medication_admin_continuous.df['med_category'] == 'vasopressor'
]

# Check concurrent medications for a patient at a specific time
patient_id = 'H12345'
time_point = pd.Timestamp('2023-01-01 12:00:00')
time_window = pd.Timedelta(hours=4)

concurrent = orchestrator.medication_admin_continuous.df[
    (orchestrator.medication_admin_continuous.df['hospitalization_id'] == patient_id) &
    (orchestrator.medication_admin_continuous.df['admin_dttm'] >= time_point - time_window) &
    (orchestrator.medication_admin_continuous.df['admin_dttm'] <= time_point)
]
concurrent_vasopressors = concurrent[concurrent['med_category'] == 'vasopressor']
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