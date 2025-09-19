# Vitals Table

## Overview

The Vitals table contains physiological measurements recorded throughout a patient's hospitalization, including heart rate, blood pressure, temperature, respiratory rate, oxygen saturation, and other bedside observations. Vital signs are fundamental for monitoring patient stability and calculating severity scores.

## Data Dictionary

For the complete field definitions and requirements, see the [official CLIF Data Dictionary for Vitals](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#vitals).

## Key Fields

- `hospitalization_id`: Links to hospitalization table
- `recorded_dttm`: Timestamp when vital sign was recorded
- `vital_name`: Type of vital sign (e.g., heart_rate, sbp, temperature)
- `vital_category`: Category grouping (e.g., blood_pressure, temperature, neurological)
- `vital_value`: Numeric measurement value
- `vital_value_unit`: Unit of measurement
- `measurement_site`: Location where measurement taken (if applicable)

## Common Usage Patterns

### Loading Vitals Data

```python
from clifpy.tables import Vitals

# Load vitals data
vitals = Vitals.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Central'
)

# Validate the data
vitals.validate()
```

### Analyzing Vital Sign Trends

```python
# Get heart rate data
heart_rate = vitals.df[vitals.df['vital_name'] == 'heart_rate']

# Summary statistics
print(f"Heart Rate - Mean: {heart_rate['vital_value'].mean():.1f} bpm")
print(f"Heart Rate - Median: {heart_rate['vital_value'].median():.1f} bpm")

# Find tachycardia (HR > 100)
tachycardia = heart_rate[heart_rate['vital_value'] > 100]
print(f"Tachycardia episodes: {len(tachycardia)}")

# Find bradycardia (HR < 60)
bradycardia = heart_rate[heart_rate['vital_value'] < 60]
print(f"Bradycardia episodes: {len(bradycardia)}")
```

### Blood Pressure Analysis

```python
# Get blood pressure measurements
sbp = vitals.df[vitals.df['vital_name'] == 'sbp']
dbp = vitals.df[vitals.df['vital_name'] == 'dbp']

# Calculate MAP (Mean Arterial Pressure)
# Need to match SBP and DBP by timestamp
bp_data = sbp.merge(
    dbp[['hospitalization_id', 'recorded_dttm', 'vital_value']], 
    on=['hospitalization_id', 'recorded_dttm'],
    suffixes=('_sbp', '_dbp')
)
bp_data['map'] = bp_data['vital_value_dbp'] + (bp_data['vital_value_sbp'] - bp_data['vital_value_dbp']) / 3

# Find hypotension (MAP < 65)
hypotension = bp_data[bp_data['map'] < 65]
print(f"Hypotensive episodes (MAP < 65): {len(hypotension)}")
```

### Temperature Patterns

```python
# Temperature analysis
temp = vitals.df[vitals.df['vital_name'] == 'temperature']

# Convert Fahrenheit to Celsius if needed
if temp['vital_value_unit'].iloc[0] == 'F':
    temp['temp_celsius'] = (temp['vital_value'] - 32) * 5/9
else:
    temp['temp_celsius'] = temp['vital_value']

# Find fever (temp > 38°C)
fever = temp[temp['temp_celsius'] > 38]
print(f"Fever episodes: {len(fever)}")

# Find hypothermia (temp < 35°C)
hypothermia = temp[temp['temp_celsius'] < 35]
print(f"Hypothermia episodes: {len(hypothermia)}")
```

### Oxygen Saturation Monitoring

```python
# SpO2 analysis
spo2 = vitals.df[vitals.df['vital_name'] == 'spo2']

# Find hypoxemia (SpO2 < 90%)
hypoxemia = spo2[spo2['vital_value'] < 90]
print(f"Hypoxemia episodes (SpO2 < 90%): {len(hypoxemia)}")

# Calculate time below 90%
patient_id = 'H12345'
patient_spo2 = spo2[spo2['hospitalization_id'] == patient_id].sort_values('recorded_dttm')
below_90 = patient_spo2[patient_spo2['vital_value'] < 90]
if not below_90.empty:
    print(f"Patient {patient_id} had {len(below_90)} SpO2 readings < 90%")
```

### Early Warning Score Components

```python
# Get vital signs for early warning score calculation
def get_recent_vitals(hospitalization_id, timestamp, window_hours=1):
    """Get most recent vitals within time window"""
    time_window = pd.Timedelta(hours=window_hours)
    
    recent = vitals.df[
        (vitals.df['hospitalization_id'] == hospitalization_id) &
        (vitals.df['recorded_dttm'] >= timestamp - time_window) &
        (vitals.df['recorded_dttm'] <= timestamp)
    ]
    
    # Get most recent value for each vital type
    vital_values = {}
    for vital in ['heart_rate', 'sbp', 'respiratory_rate', 'temperature', 'spo2']:
        vital_data = recent[recent['vital_name'] == vital]
        if not vital_data.empty:
            vital_values[vital] = vital_data.sort_values('recorded_dttm').iloc[-1]['vital_value']
    
    return vital_values

# Example usage
current_vitals = get_recent_vitals('H12345', pd.Timestamp.now())
```

## Data Quality Considerations

- **Physiological Ranges**: Verify vital signs are within plausible ranges
- **Unit Consistency**: Check units match expected values (e.g., temperature in C or F)
- **Frequency**: Monitor measurement frequency for appropriate care level
- **Outlier Detection**: Flag potentially erroneous values

```python
# Define physiological ranges
normal_ranges = {
    'heart_rate': (30, 250),
    'sbp': (50, 300),
    'dbp': (20, 200),
    'respiratory_rate': (4, 60),
    'temperature': (32, 42),  # Celsius
    'spo2': (50, 100),
    'weight': (20, 300)  # kg
}

# Check for out-of-range values
for vital_name, (min_val, max_val) in normal_ranges.items():
    vital_subset = vitals.df[vitals.df['vital_name'] == vital_name]
    if not vital_subset.empty:
        out_of_range = vital_subset[
            (vital_subset['vital_value'] < min_val) | 
            (vital_subset['vital_value'] > max_val)
        ]
        if not out_of_range.empty:
            print(f"{vital_name}: {len(out_of_range)} values outside normal range")
```

## Related Tables

- **Hospitalization**: Primary link via hospitalization_id
- **Labs**: Correlate vital signs with laboratory values
- **Respiratory Support**: Match SpO2 with ventilator settings
- **Medication Admin**: Associate vital changes with medication administration
- **ADT**: Link vital sign frequency with level of care

## API Reference

For detailed API documentation, see [Vitals API](../../api/tables.md#vitals)