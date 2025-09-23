# Labs Table

## Overview

The Labs table contains laboratory test results throughout a patient's hospitalization. This includes chemistry panels, hematology, coagulation studies, blood gases, and other diagnostic tests. Laboratory data is crucial for monitoring patient status, calculating severity scores, and guiding clinical decisions.

## Data Dictionary

For the complete field definitions and requirements, see the [official CLIF Data Dictionary for Labs](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#labs).

## Key Fields

- `hospitalization_id`: Links to hospitalization table
- `lab_datetime`: Timestamp when lab was collected/resulted
- `lab_name`: Specific test name (e.g., hemoglobin, creatinine)
- `lab_category`: Test category (chemistry, hematology, blood_gas, etc.)
- `lab_value`: Numeric result value
- `lab_value_unit`: Unit of measurement
- `specimen_type`: Sample source (blood, urine, etc.)

## Common Usage Patterns

### Loading Labs Data

```python
from clifpy.tables import Labs

# Load labs data
labs = Labs.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Central'
)

# Validate the data
labs.validate()
```

### Analyzing Specific Lab Values

```python
# Get all creatinine values
creatinine = labs.df[labs.df['lab_name'] == 'creatinine']

# Summary statistics
print(f"Creatinine values - Mean: {creatinine['lab_value'].mean():.2f}")
print(f"Creatinine values - Median: {creatinine['lab_value'].median():.2f}")

# Find abnormal values
abnormal_creat = creatinine[creatinine['lab_value'] > 2.0]
print(f"Abnormal creatinine (>2.0): {len(abnormal_creat)} results")
```

### Tracking Lab Trends

```python
# Track patient's creatinine over time
patient_id = 'H12345'
patient_creat = labs.df[
    (labs.df['hospitalization_id'] == patient_id) &
    (labs.df['lab_name'] == 'creatinine')
].sort_values('lab_datetime')

# Plot trend (requires matplotlib)
import matplotlib.pyplot as plt
plt.figure(figsize=(10, 6))
plt.plot(patient_creat['lab_datetime'], patient_creat['lab_value'], marker='o')
plt.xlabel('Time')
plt.ylabel('Creatinine (mg/dL)')
plt.title(f'Creatinine Trend for Patient {patient_id}')
plt.xticks(rotation=45)
plt.tight_layout()
```

### Finding Critical Values

```python
# Define critical value thresholds
critical_thresholds = {
    'potassium': (2.5, 6.5),
    'sodium': (120, 160),
    'glucose': (40, 500),
    'hemoglobin': (5, 20),
    'platelet_count': (20, 1000)
}

# Find all critical labs
critical_labs = []
for lab_name, (low, high) in critical_thresholds.items():
    lab_data = labs.df[labs.df['lab_name'] == lab_name]
    critical = lab_data[
        (lab_data['lab_value'] < low) | 
        (lab_data['lab_value'] > high)
    ]
    if not critical.empty:
        critical_labs.append(critical)

if critical_labs:
    all_critical = pd.concat(critical_labs)
    print(f"Found {len(all_critical)} critical lab values")
```

### Lab Categories Analysis

```python
# Distribution of lab categories
category_counts = labs.df['lab_category'].value_counts()
print("\nLab Categories:")
for category, count in category_counts.items():
    print(f"  {category}: {count:,} results")

# Most common labs
top_labs = labs.df['lab_name'].value_counts().head(10)
print("\nTop 10 Most Common Labs:")
print(top_labs)
```

### Calculate Derived Values

```python
# Calculate anion gap from chemistry labs
def calculate_anion_gap(hospitalization_id, timestamp):
    # Get values within 1 hour of timestamp
    time_window = pd.Timedelta(hours=1)
    labs_subset = labs.df[
        (labs.df['hospitalization_id'] == hospitalization_id) &
        (labs.df['lab_datetime'] >= timestamp - time_window) &
        (labs.df['lab_datetime'] <= timestamp + time_window)
    ]
    
    sodium = labs_subset[labs_subset['lab_name'] == 'sodium']['lab_value'].iloc[0] if not labs_subset[labs_subset['lab_name'] == 'sodium'].empty else None
    chloride = labs_subset[labs_subset['lab_name'] == 'chloride']['lab_value'].iloc[0] if not labs_subset[labs_subset['lab_name'] == 'chloride'].empty else None
    bicarb = labs_subset[labs_subset['lab_name'] == 'bicarbonate']['lab_value'].iloc[0] if not labs_subset[labs_subset['lab_name'] == 'bicarbonate'].empty else None
    
    if all([sodium, chloride, bicarb]):
        return sodium - (chloride + bicarb)
    return None
```

## Data Quality Considerations

- **Reference Ranges**: Lab values should be physiologically plausible
- **Unit Consistency**: Verify units match expected values for each lab type
- **Specimen Types**: Ensure appropriate specimen types for specific tests
- **Duplicate Results**: Check for duplicate entries at same timestamp

```python
# Check for implausible values
implausible = {
    'hemoglobin': (0, 25),
    'creatinine': (0, 30),
    'glucose': (0, 2000),
    'sodium': (100, 200),
    'potassium': (1, 10)
}

for lab_name, (min_val, max_val) in implausible.items():
    lab_subset = labs.df[labs.df['lab_name'] == lab_name]
    out_of_range = lab_subset[
        (lab_subset['lab_value'] < min_val) | 
        (lab_subset['lab_value'] > max_val)
    ]
    if not out_of_range.empty:
        print(f"{lab_name}: {len(out_of_range)} values outside plausible range")
```

## Related Tables

- **Hospitalization**: Primary link via hospitalization_id
- **Vitals**: Correlate lab values with vital signs
- **Medication Admin**: Associate labs with drug levels or treatment effects
- **ADT**: Link lab timing with patient location

## API Reference

For detailed API documentation, see [Labs API](../../api/tables.md#labs)