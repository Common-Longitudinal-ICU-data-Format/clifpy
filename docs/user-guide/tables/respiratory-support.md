# Respiratory Support Table

## Overview

The Respiratory Support table captures detailed information about mechanical ventilation and other forms of respiratory assistance. This includes ventilator settings, modes, measured parameters, and oxygen delivery devices. This data is critical for tracking respiratory failure severity, ventilator weaning, and calculating ventilator-free days.

## Data Dictionary

For the complete field definitions and requirements, see the [official CLIF Data Dictionary for Respiratory Support](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#respiratory_support).

## Key Fields

- `hospitalization_id`: Links to hospitalization table
- `recorded_dttm`: Timestamp of respiratory support documentation
- `device_category`: Type of support (imv, nippv, high_flow_nc, nasal_cannula, etc.)
- `device_name`: Specific device name
- `mode_category`: Ventilation mode category (ac/vc, simv, psv, etc.)
- `fio2_set`: Fraction of inspired oxygen setting
- `peep_set`: Positive end-expiratory pressure setting
- `resp_rate_set`: Set respiratory rate
- `tidal_volume_set`: Set tidal volume (mL)
- `pressure_support_set`: Pressure support level
- `measured_*`: Measured values (tidal volume, minute ventilation, etc.)

## Common Usage Patterns

### Loading Respiratory Support Data

```python
from clifpy.tables import RespiratorySupport

# Load respiratory support data
resp_support = RespiratorySupport.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Central'
)

# Validate the data
resp_support.validate()
```

### Identifying Mechanical Ventilation

```python
# Find invasive mechanical ventilation
imv = resp_support.df[resp_support.df['device_category'] == 'imv']
imv_patients = imv['hospitalization_id'].unique()
print(f"Patients on invasive ventilation: {len(imv_patients)}")

# Find non-invasive ventilation
nippv = resp_support.df[resp_support.df['device_category'] == 'nippv']
nippv_patients = nippv['hospitalization_id'].unique()
print(f"Patients on non-invasive ventilation: {len(nippv_patients)}")
```

### Ventilator Settings Analysis

```python
# Analyze FiO2 settings for mechanically ventilated patients
imv_fio2 = imv[imv['fio2_set'].notna()]
print(f"Average FiO2 on IMV: {imv_fio2['fio2_set'].mean():.2f}")

# High FiO2 requirements (>0.6)
high_fio2 = imv_fio2[imv_fio2['fio2_set'] > 0.6]
print(f"IMV observations with FiO2 > 0.6: {len(high_fio2)} ({len(high_fio2)/len(imv_fio2)*100:.1f}%)")

# PEEP analysis
imv_peep = imv[imv['peep_set'].notna()]
print(f"Average PEEP on IMV: {imv_peep['peep_set'].mean():.1f} cmH2O")
```

### Ventilation Mode Distribution

```python
# Mode category distribution for IMV
mode_dist = imv['mode_category'].value_counts()
print("\nIMV Mode Distribution:")
for mode, count in mode_dist.items():
    print(f"  {mode}: {count} ({count/len(imv)*100:.1f}%)")

# Track mode changes for a patient
patient_id = 'H12345'
patient_modes = imv[imv['hospitalization_id'] == patient_id].sort_values('recorded_dttm')
mode_changes = patient_modes['mode_category'].ne(patient_modes['mode_category'].shift()).sum() - 1
print(f"\nPatient {patient_id} had {mode_changes} ventilator mode changes")
```

### Calculating Ventilator-Free Days

```python
# Calculate ventilator-free days at day 28
def calculate_vfd_28(hospitalization_id, admission_date):
    """Calculate ventilator-free days at day 28"""
    # Get all IMV records for this hospitalization
    patient_imv = imv[imv['hospitalization_id'] == hospitalization_id]
    
    if patient_imv.empty:
        return 28  # Never on ventilator
    
    # Find ventilator days within first 28 days
    day_28 = admission_date + pd.Timedelta(days=28)
    vent_in_28 = patient_imv[patient_imv['recorded_dttm'] <= day_28]
    
    # Count unique days on ventilator
    vent_days = vent_in_28['recorded_dttm'].dt.date.nunique()
    
    return max(0, 28 - vent_days)

# Example usage
vfd = calculate_vfd_28('H12345', pd.Timestamp('2023-01-01'))
print(f"Ventilator-free days at day 28: {vfd}")
```

### Using Waterfall Processing

```python
# Apply waterfall processing for complete ventilation timeline
processed = resp_support.waterfall(verbose=True)

# The waterfall method:
# - Creates hourly scaffolds for continuous timeline
# - Forward fills settings within ventilation episodes
# - Infers missing device/mode information
# - Adds hierarchical episode IDs

# Access the processed data
df_processed = processed.df

# Filter out scaffold rows if needed for analysis
actual_observations = df_processed[df_processed['is_scaffold'] == False]
```

### Weaning Assessment

```python
# Identify potential weaning trials (pressure support ventilation)
psv = resp_support.df[resp_support.df['mode_category'].str.contains('psv', na=False)]

# Low pressure support (<= 10 cmH2O) with low PEEP (<= 5)
weaning_trials = psv[
    (psv['pressure_support_set'] <= 10) & 
    (psv['peep_set'] <= 5) &
    (psv['fio2_set'] <= 0.4)
]

print(f"Potential weaning trials identified: {len(weaning_trials)}")
```

## Data Quality Considerations

- **FiO2 Scaling**: Verify FiO2 is properly scaled (0-1.0, not 0-100)
- **Setting Ranges**: Check ventilator settings are within reasonable ranges
- **Mode Consistency**: Ensure mode categories align with device categories
- **Continuous Recording**: Look for gaps in ventilation documentation

```python
# Data quality checks
# Check FiO2 scaling
fio2_issues = resp_support.df[
    (resp_support.df['fio2_set'] > 1.0) & 
    (resp_support.df['fio2_set'] <= 100)
]
if not fio2_issues.empty:
    print(f"Found {len(fio2_issues)} records with FiO2 possibly in percentage form")

# Check for reasonable ventilator settings
imv_settings = imv[imv['tidal_volume_set'].notna()]
tv_outliers = imv_settings[
    (imv_settings['tidal_volume_set'] < 200) | 
    (imv_settings['tidal_volume_set'] > 1000)
]
if not tv_outliers.empty:
    print(f"Found {len(tv_outliers)} records with unusual tidal volumes")

# Check for documentation gaps
for patient_id in imv_patients[:5]:  # Check first 5 patients
    patient_data = imv[imv['hospitalization_id'] == patient_id].sort_values('recorded_dttm')
    time_gaps = patient_data['recorded_dttm'].diff()
    large_gaps = time_gaps[time_gaps > pd.Timedelta(hours=12)]
    if not large_gaps.empty:
        print(f"Patient {patient_id}: {len(large_gaps)} gaps > 12 hours in ventilator documentation")
```

## Related Tables

- **Hospitalization**: Primary link via hospitalization_id
- **Vitals**: Correlate SpO2 and respiratory rate with ventilator settings
- **Labs**: Match blood gases with ventilator parameters
- **ADT**: Verify respiratory support aligns with ICU location
- **Position**: Track prone positioning during mechanical ventilation

## Special Features

### Waterfall Processing

The RespiratorySupport table includes a special `waterfall()` method for creating dense, analysis-ready ventilation timelines:

```python
# Process respiratory data with waterfall
processed = resp_support.waterfall(
    bfill=False,  # Only forward fill (default)
    verbose=True  # Show processing messages
)

# Returns a new RespiratorySupport instance with:
# - Hourly scaffold rows for complete timeline
# - Forward-filled numeric settings
# - Inferred device/mode information
# - Episode tracking IDs
```

See the [Respiratory Support Waterfall](../waterfall.md) guide for detailed information.

## API Reference

For detailed API documentation, see [RespiratorySupport API](../../api/tables.md#respiratorysupport)