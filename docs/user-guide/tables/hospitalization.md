# Hospitalization Table

## Overview

The Hospitalization table contains core information about each hospital admission, including admission and discharge times, patient age, and admission/discharge categories. This table serves as a central reference point for linking other clinical data tables.

## Data Dictionary

For the complete field definitions and requirements, see the [official CLIF Data Dictionary for Hospitalization](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#hospitalization).

## Key Fields

- `hospitalization_id`: Unique identifier for each hospital admission
- `patient_id`: Links to patient demographics
- `admission_dttm` / `discharge_dttm`: Hospital admission and discharge timestamps
- `age_at_admission`: Patient age in years at time of admission
- `admission_type_category`: Type of admission (emergency, elective, etc.)
- `discharge_category`: Discharge disposition (home, expired, transfer, etc.)

## Common Usage Patterns

### Loading Hospitalization Data

```python
from clifpy.tables import Hospitalization

# Load hospitalization data
hospitalization = Hospitalization.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Central'
)

# Validate the data
hospitalization.validate()
```

### Calculating Hospital Length of Stay

```python
# Calculate hospital LOS in days
hospitalization.df['hospital_los_days'] = (
    hospitalization.df['discharge_dttm'] - hospitalization.df['admission_dttm']
).dt.total_seconds() / 86400

# Summary statistics
print(f"Median hospital LOS: {hospitalization.df['hospital_los_days'].median():.1f} days")
print(f"Mean hospital LOS: {hospitalization.df['hospital_los_days'].mean():.1f} days")
```

### Analyzing Admission Types

```python
# Admission type distribution
admission_counts = hospitalization.df['admission_type_category'].value_counts()
print("\nAdmission Types:")
print(admission_counts)

# Emergency vs elective admissions
emergency = hospitalization.df[hospitalization.df['admission_type_category'] == 'emergency']
print(f"\nEmergency admissions: {len(emergency)} ({len(emergency)/len(hospitalization.df)*100:.1f}%)")
```

### Mortality Analysis

```python
# In-hospital mortality
expired = hospitalization.df[hospitalization.df['discharge_category'] == 'expired']
mortality_rate = len(expired) / len(hospitalization.df) * 100
print(f"In-hospital mortality rate: {mortality_rate:.1f}%")

# Mortality by age group
hospitalization.df['age_group'] = pd.cut(
    hospitalization.df['age_at_admission'],
    bins=[0, 18, 45, 65, 85, 150],
    labels=['<18', '18-44', '45-64', '65-84', 'e85']
)

mortality_by_age = hospitalization.df.groupby('age_group')['discharge_category'].apply(
    lambda x: (x == 'expired').sum() / len(x) * 100
)
print("\nMortality by age group:")
print(mortality_by_age)
```

### Creating Patient Cohorts

```python
# Elderly ICU patients (requires join with ADT)
elderly = hospitalization.df[hospitalization.df['age_at_admission'] >= 65]
elderly_ids = elderly['hospitalization_id'].tolist()

# Recent admissions
recent = hospitalization.df[
    hospitalization.df['admission_dttm'] >= pd.Timestamp('2023-01-01').tz_localize('UTC')
]
```

## Data Quality Considerations

- **Timestamp Validation**: Ensure discharge_dttm > admission_dttm
- **Age Verification**: Check for implausible age values (< 0 or > 120)
- **Category Standards**: Validate admission/discharge categories against CLIF standards
- **Missing Discharges**: Identify hospitalizations without discharge times (current admissions)

```python
# Data quality checks
# Check for invalid timestamps
invalid_times = hospitalization.df[
    hospitalization.df['discharge_dttm'] <= hospitalization.df['admission_dttm']
]
if not invalid_times.empty:
    print(f"Found {len(invalid_times)} records with invalid discharge times")

# Check age ranges
invalid_age = hospitalization.df[
    (hospitalization.df['age_at_admission'] < 0) | 
    (hospitalization.df['age_at_admission'] > 120)
]
if not invalid_age.empty:
    print(f"Found {len(invalid_age)} records with invalid ages")
```

## Related Tables

- **Patient**: Links via patient_id for demographics
- **ADT**: Links via hospitalization_id for location tracking
- **All Clinical Tables**: hospitalization_id serves as primary key for joining labs, vitals, medications, etc.
- **Hospital Diagnosis**: Links diagnoses to specific admissions

## API Reference

For detailed API documentation, see [Hospitalization API](../../api/tables.md#hospitalization)