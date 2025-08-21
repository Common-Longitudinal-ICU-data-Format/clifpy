# Patient Table

The Patient table contains core demographic information and serves as the primary reference for all other CLIF tables through the `patient_id` field.

## Overview

The Patient table includes:
- Unique patient identifiers
- Birth and death dates
- Demographics (sex, race, ethnicity)
- Primary language

## Required Columns

| Column | Type | Description |
|--------|------|-------------|
| patient_id | VARCHAR | Unique patient identifier |
| birth_date | DATETIME | Date of birth |
| death_dttm | DATETIME | Date/time of death (null if alive) |
| race_name | VARCHAR | Free-text race description |
| race_category | VARCHAR | Standardized race category |
| ethnicity_name | VARCHAR | Free-text ethnicity description |
| ethnicity_category | VARCHAR | Standardized ethnicity category |
| sex_name | VARCHAR | Free-text sex description |
| sex_category | VARCHAR | Standardized sex category |
| language_name | VARCHAR | Primary language |
| language_category | VARCHAR | Standardized language category |

## Standardized Categories

### Race Categories
- Black or African American
- White
- American Indian or Alaska Native
- Asian
- Native Hawaiian or Other Pacific Islander
- Unknown
- Other

### Ethnicity Categories
- Hispanic
- Non-Hispanic
- Unknown

### Sex Categories
- Male
- Female
- Unknown

## Usage Examples

### Loading Patient Data

```python
from clifpy.tables import Patient

# Load from file
patient = Patient.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Central'
)

# Validate the data
patient.validate()
```

### Basic Analysis

```python
# Get summary statistics
summary = patient.get_summary()
print(f"Total patients: {summary['num_rows']}")

# Demographics distribution
demographics = patient.df.groupby(['sex_category', 'race_category']).size()
print(demographics)

# Age calculation (if needed)
patient.df['age'] = (
    pd.Timestamp.now() - patient.df['birth_date']
).dt.days / 365.25

# Find elderly patients
elderly = patient.df[patient.df['age'] >= 65]
```

### Cohort Building

```python
# Female patients over 65
cohort = patient.df[
    (patient.df['sex_category'] == 'Female') & 
    (patient.df['age'] >= 65)
]

# Living patients
alive = patient.df[patient.df['death_dttm'].isna()]

# Specific ethnicity
hispanic = patient.df[patient.df['ethnicity_category'] == 'Hispanic']
```

### Joining with Other Tables

```python
# Get patient demographics for lab results
labs_with_demographics = labs.df.merge(
    patient.df[['patient_id', 'age', 'sex_category']],
    on='patient_id',
    how='left'
)

# Analyze by demographic groups
lab_by_sex = labs_with_demographics.groupby('sex_category')['lab_value'].mean()
```

## Data Quality Checks

```python
# Check for missing demographics
missing_sex = patient.df[patient.df['sex_category'].isna()]
missing_race = patient.df[patient.df['race_category'].isna()]

# Validate age ranges
patient.df['age'] = (pd.Timestamp.now() - patient.df['birth_date']).dt.days / 365.25
invalid_age = patient.df[(patient.df['age'] < 0) | (patient.df['age'] > 120)]

# Check death date consistency
invalid_death = patient.df[
    patient.df['death_dttm'] < patient.df['birth_date']
]
```

## Best Practices

1. **Always validate** demographic categories against standardized values
2. **Handle missing data** appropriately for demographic fields
3. **Calculate age** at time of admission, not current date
4. **Protect PHI** by using only de-identified patient_ids
5. **Document** any demographic data transformations

## API Reference

For detailed API documentation, see [Patient API](../../api/tables.md#patient)