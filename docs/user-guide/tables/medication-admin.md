# Medication Administration Continuous Table

## Overview

The Medication Administration Continuous table tracks continuous infusion medications throughout hospitalization. This includes vasopressors, sedatives, analgesics, insulin, and other critical drips. The data is essential for assessing illness severity, calculating vasopressor dosing, and monitoring sedation practices.

## Data Dictionary

For the complete field definitions and requirements, see the [official CLIF Data Dictionary for Medication Administration Continuous](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#medication_admin_continuous).

## Key Fields

- `hospitalization_id`: Links to hospitalization table
- `admin_dttm`: Timestamp of medication administration/documentation
- `med_name`: Specific medication name
- `med_category`: Medication category (vasopressor, sedative, analgesic, etc.)
- `med_group`: Medication grouping for similar drugs
- `dosage`: Numeric dosage amount
- `dosage_unit`: Unit of dosage measurement
- `weight_based_dosage`: Weight-based dosing amount
- `weight_based_dosage_unit`: Unit for weight-based dosing (mcg/kg/min, etc.)
- `infusion_rate`: Rate of infusion
- `infusion_rate_unit`: Unit for infusion rate

## Common Usage Patterns

### Loading Medication Data

```python
from clifpy.tables import MedicationAdminContinuous

# Load medication administration data
meds = MedicationAdminContinuous.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Central'
)

# Validate the data
meds.validate()
```

### Vasopressor Analysis

```python
# Find all vasopressor administrations
vasopressors = meds.df[meds.df['med_category'] == 'vasopressor']
vasopressor_patients = vasopressors['hospitalization_id'].unique()
print(f"Patients receiving vasopressors: {len(vasopressor_patients)}")

# Most common vasopressors
vasopress_counts = vasopressors['med_name'].value_counts()
print("\nTop vasopressors used:")
for med, count in vasopress_counts.head(5).items():
    print(f"  {med}: {count} administrations")

# Norepinephrine dosing analysis
norepi = vasopressors[vasopressors['med_name'].str.contains('norepinephrine', case=False, na=False)]
if not norepi.empty and 'weight_based_dosage' in norepi.columns:
    norepi_doses = norepi[norepi['weight_based_dosage'].notna()]
    print(f"\nNorepinephrine doses - Mean: {norepi_doses['weight_based_dosage'].mean():.3f} mcg/kg/min")
    print(f"Norepinephrine doses - Max: {norepi_doses['weight_based_dosage'].max():.3f} mcg/kg/min")
```

### Multiple Vasopressor Use

```python
# Find patients on multiple vasopressors simultaneously
def count_concurrent_vasopressors(hospitalization_id, timestamp):
    """Count vasopressors active at given timestamp"""
    # Look within reasonable time window (e.g., 4 hours)
    window = pd.Timedelta(hours=4)
    concurrent = vasopressors[
        (vasopressors['hospitalization_id'] == hospitalization_id) &
        (vasopressors['admin_dttm'] >= timestamp - window) &
        (vasopressors['admin_dttm'] <= timestamp)
    ]
    return concurrent['med_name'].nunique()

# Identify high-severity patients (e2 vasopressors)
high_severity = []
for patient_id in vasopressor_patients[:100]:  # Sample first 100
    patient_vasopressors = vasopressors[vasopressors['hospitalization_id'] == patient_id]
    max_concurrent = 0
    for timestamp in patient_vasopressors['admin_dttm']:
        concurrent = count_concurrent_vasopressors(patient_id, timestamp)
        max_concurrent = max(max_concurrent, concurrent)
    if max_concurrent >= 2:
        high_severity.append(patient_id)

print(f"\nPatients on e2 concurrent vasopressors: {len(high_severity)}")
```

### Sedation Practices

```python
# Analyze sedative use
sedatives = meds.df[meds.df['med_category'] == 'sedative']
sedative_counts = sedatives['med_name'].value_counts()
print("\nTop sedatives used:")
for med, count in sedative_counts.head(5).items():
    print(f"  {med}: {count} administrations")

# Propofol dosing
propofol = sedatives[sedatives['med_name'].str.contains('propofol', case=False, na=False)]
if not propofol.empty:
    propofol_rates = propofol[propofol['infusion_rate'].notna()]
    print(f"\nPropofol infusion rates - Mean: {propofol_rates['infusion_rate'].mean():.1f} mcg/kg/min")
```

### Analgesic Management

```python
# Analyze analgesic use
analgesics = meds.df[meds.df['med_category'] == 'analgesic']

# Opioid infusions
opioids = analgesics[analgesics['med_group'] == 'opioid']
opioid_patients = opioids['hospitalization_id'].unique()
print(f"\nPatients on continuous opioid infusions: {len(opioid_patients)}")

# Fentanyl equivalents (if available)
fentanyl = opioids[opioids['med_name'].str.contains('fentanyl', case=False, na=False)]
if not fentanyl.empty:
    fentanyl_doses = fentanyl[fentanyl['weight_based_dosage'].notna()]
    print(f"Fentanyl doses - Mean: {fentanyl_doses['weight_based_dosage'].mean():.2f} mcg/kg/hr")
```

### Insulin Protocol Analysis

```python
# Analyze insulin drips
insulin = meds.df[meds.df['med_name'].str.contains('insulin', case=False, na=False)]
insulin_patients = insulin['hospitalization_id'].unique()
print(f"\nPatients on insulin drips: {len(insulin_patients)}")

# Insulin infusion rates
if not insulin.empty:
    insulin_rates = insulin[insulin['infusion_rate'].notna()]
    print(f"Insulin infusion rates - Mean: {insulin_rates['infusion_rate'].mean():.2f} units/hr")
    print(f"Insulin infusion rates - Max: {insulin_rates['infusion_rate'].max():.1f} units/hr")
```

### Creating Medication Timelines

```python
# Create medication timeline for a patient
def create_med_timeline(hospitalization_id):
    """Create timeline of all continuous medications"""
    patient_meds = meds.df[meds.df['hospitalization_id'] == hospitalization_id]
    
    # Group by medication and get start/stop times
    timeline = []
    for med_name in patient_meds['med_name'].unique():
        med_data = patient_meds[patient_meds['med_name'] == med_name].sort_values('admin_dttm')
        
        # Simple approach - assume continuous if gaps < 6 hours
        start_time = med_data.iloc[0]['admin_dttm']
        end_time = med_data.iloc[-1]['admin_dttm']
        
        timeline.append({
            'medication': med_name,
            'category': med_data.iloc[0]['med_category'],
            'start': start_time,
            'end': end_time,
            'duration_hours': (end_time - start_time).total_seconds() / 3600
        })
    
    return pd.DataFrame(timeline).sort_values('start')

# Example usage
patient_timeline = create_med_timeline('H12345')
print(patient_timeline)
```

## Data Quality Considerations

- **Dosage Units**: Verify units are appropriate for each medication
- **Weight-Based Dosing**: Check calculations when both dosage and weight-based dosage provided
- **Infusion Gaps**: Identify unexpected gaps in continuous infusions
- **Dosage Ranges**: Validate doses are within safe/reasonable ranges

```python
# Data quality checks
# Check for missing units
missing_units = meds.df[
    meds.df['dosage'].notna() & 
    meds.df['dosage_unit'].isna()
]
if not missing_units.empty:
    print(f"Found {len(missing_units)} records with dosage but no units")

# Check vasopressor dosing ranges
norepi_doses = vasopressors[
    (vasopressors['med_name'].str.contains('norepinephrine', case=False, na=False)) &
    (vasopressors['weight_based_dosage'].notna())
]
high_dose_norepi = norepi_doses[norepi_doses['weight_based_dosage'] > 2.0]  # >2 mcg/kg/min
if not high_dose_norepi.empty:
    print(f"Found {len(high_dose_norepi)} very high-dose norepinephrine administrations")

# Check for infusion continuity
for patient_id in vasopressor_patients[:10]:  # Check first 10 patients
    patient_meds = vasopressors[vasopressors['hospitalization_id'] == patient_id]
    for med in patient_meds['med_name'].unique():
        med_times = patient_meds[patient_meds['med_name'] == med]['admin_dttm'].sort_values()
        gaps = med_times.diff()
        large_gaps = gaps[gaps > pd.Timedelta(hours=6)]
        if not large_gaps.empty:
            print(f"Patient {patient_id}, {med}: {len(large_gaps)} gaps > 6 hours")
```

## Related Tables

- **Hospitalization**: Primary link via hospitalization_id
- **Vitals**: Correlate blood pressure with vasopressor doses
- **Labs**: Match drug levels or metabolic effects
- **ADT**: Verify ICU medications align with ICU location
- **Respiratory Support**: Associate sedation with mechanical ventilation

## API Reference

For detailed API documentation, see [MedicationAdminContinuous API](../../api/tables.md#clifpy.tables.medication_admin_continuous.MedicationAdminContinuous)