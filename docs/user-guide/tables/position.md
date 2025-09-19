# Position Table

## Overview

The Position table documents patient positioning throughout their hospitalization, with particular emphasis on prone positioning for acute respiratory distress syndrome (ARDS) management. This data is critical for evaluating compliance with prone positioning protocols and correlating positioning with oxygenation outcomes.

## Data Dictionary

For the complete field definitions and requirements, see the [official CLIF Data Dictionary for Position](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#position).

## Key Fields

- `hospitalization_id`: Links to hospitalization table
- `recorded_dttm`: Timestamp when position was recorded
- `position_name`: Specific position name/description
- `position_category`: Standardized position category (prone, supine, lateral, sitting, standing, ambulating)

## Common Usage Patterns

### Loading Position Data

```python
from clifpy.tables import Position

# Load position data
position = Position.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Central'
)

# Validate the data
position.validate()
```

### Prone Positioning Analysis

```python
# Identify prone positioning episodes
prone = position.df[position.df['position_category'] == 'prone']
prone_patients = prone['hospitalization_id'].unique()
print(f"Patients receiving prone positioning: {len(prone_patients)}")

# Calculate prone positioning duration
def calculate_prone_duration(hospitalization_id):
    """Calculate total hours in prone position"""
    patient_positions = position.df[
        position.df['hospitalization_id'] == hospitalization_id
    ].sort_values('recorded_dttm')
    
    total_prone_hours = 0
    in_prone = False
    prone_start = None
    
    for _, row in patient_positions.iterrows():
        if row['position_category'] == 'prone' and not in_prone:
            prone_start = row['recorded_dttm']
            in_prone = True
        elif row['position_category'] != 'prone' and in_prone:
            duration = (row['recorded_dttm'] - prone_start).total_seconds() / 3600
            total_prone_hours += duration
            in_prone = False
    
    return total_prone_hours

# Example for prone patients
prone_durations = []
for patient_id in prone_patients[:20]:  # Sample first 20
    duration = calculate_prone_duration(patient_id)
    if duration > 0:
        prone_durations.append(duration)

if prone_durations:
    print(f"\nProne positioning duration:")
    print(f"  Mean: {np.mean(prone_durations):.1f} hours")
    print(f"  Median: {np.median(prone_durations):.1f} hours")
```

### Position Distribution

```python
# Analyze position distribution
position_dist = position.df['position_category'].value_counts()
print("\nPosition Distribution:")
for pos, count in position_dist.items():
    percentage = count / len(position.df) * 100
    print(f"  {pos}: {count:,} ({percentage:.1f}%)")

# Time spent in each position
position_times = {}
for hosp_id in position.df['hospitalization_id'].unique()[:50]:  # Sample
    patient_pos = position.df[
        position.df['hospitalization_id'] == hosp_id
    ].sort_values('recorded_dttm')
    
    for i in range(len(patient_pos) - 1):
        pos_cat = patient_pos.iloc[i]['position_category']
        duration = (patient_pos.iloc[i + 1]['recorded_dttm'] - 
                   patient_pos.iloc[i]['recorded_dttm']).total_seconds() / 3600
        
        if pos_cat not in position_times:
            position_times[pos_cat] = []
        position_times[pos_cat].append(duration)

# Calculate average time per position
print("\nAverage time per position:")
for pos, times in position_times.items():
    print(f"  {pos}: {np.mean(times):.1f} hours")
```

### Prone Positioning Compliance

```python
# Check prone positioning protocol compliance
# Typical protocol: prone for 16+ hours per day when indicated

def assess_prone_compliance(hospitalization_id, target_hours=16):
    """Assess compliance with prone positioning protocol"""
    patient_positions = position.df[
        position.df['hospitalization_id'] == hospitalization_id
    ].sort_values('recorded_dttm')
    
    # Group by date
    patient_positions['date'] = patient_positions['recorded_dttm'].dt.date
    
    compliance_days = []
    for date in patient_positions['date'].unique():
        day_positions = patient_positions[patient_positions['date'] == date]
        
        # Calculate prone hours for this day
        prone_hours = 0
        for i in range(len(day_positions) - 1):
            if day_positions.iloc[i]['position_category'] == 'prone':
                duration = (day_positions.iloc[i + 1]['recorded_dttm'] - 
                          day_positions.iloc[i]['recorded_dttm']).total_seconds() / 3600
                prone_hours += min(duration, 24)  # Cap at 24 hours
        
        if prone_hours >= target_hours:
            compliance_days.append(date)
    
    return compliance_days

# Check compliance for prone patients
compliant_patients = 0
for patient_id in prone_patients[:20]:  # Sample
    compliant_days = assess_prone_compliance(patient_id)
    if compliant_days:
        compliant_patients += 1

print(f"\nProne positioning compliance (e16 hrs/day): {compliant_patients}/20 patients")
```

### Mobility Assessment

```python
# Track patient mobility progression
mobility_positions = ['sitting', 'standing', 'ambulating']
mobile = position.df[position.df['position_category'].isin(mobility_positions)]

# Patients achieving mobility
mobile_patients = mobile['hospitalization_id'].unique()
print(f"\nPatients with documented mobility: {len(mobile_patients)}")

# Mobility milestones
for patient_id in mobile_patients[:10]:  # Sample
    patient_mobility = position.df[
        (position.df['hospitalization_id'] == patient_id) &
        (position.df['position_category'].isin(mobility_positions))
    ].sort_values('recorded_dttm')
    
    if not patient_mobility.empty:
        first_mobility = patient_mobility.iloc[0]
        print(f"\nPatient {patient_id}:")
        print(f"  First mobility: {first_mobility['position_category']} at {first_mobility['recorded_dttm']}")
        
        # Check progression
        if 'ambulating' in patient_mobility['position_category'].values:
            first_ambulation = patient_mobility[
                patient_mobility['position_category'] == 'ambulating'
            ].iloc[0]['recorded_dttm']
            print(f"  First ambulation: {first_ambulation}")
```

### Position Changes and Pressure Injury Prevention

```python
# Analyze position change frequency
def calculate_position_changes(hospitalization_id):
    """Count position changes per day"""
    patient_positions = position.df[
        position.df['hospitalization_id'] == hospitalization_id
    ].sort_values('recorded_dttm')
    
    if len(patient_positions) < 2:
        return []
    
    # Count changes per day
    patient_positions['date'] = patient_positions['recorded_dttm'].dt.date
    changes_per_day = []
    
    for date in patient_positions['date'].unique():
        day_positions = patient_positions[patient_positions['date'] == date]
        
        # Count position changes
        changes = 0
        for i in range(1, len(day_positions)):
            if day_positions.iloc[i]['position_category'] != day_positions.iloc[i-1]['position_category']:
                changes += 1
        
        changes_per_day.append(changes)
    
    return changes_per_day

# Analyze turning frequency
all_changes = []
for patient_id in position.df['hospitalization_id'].unique()[:50]:  # Sample
    changes = calculate_position_changes(patient_id)
    all_changes.extend(changes)

if all_changes:
    print(f"\nPosition changes per day:")
    print(f"  Mean: {np.mean(all_changes):.1f}")
    print(f"  Median: {np.median(all_changes):.0f}")
    
    # Flag patients with insufficient turning (< 4 times/day)
    low_turning = [c for c in all_changes if c < 4]
    print(f"  Days with <4 position changes: {len(low_turning)} ({len(low_turning)/len(all_changes)*100:.1f}%)")
```

### Correlating Position with Respiratory Support

```python
# Link positioning with respiratory outcomes (requires joining tables)
# Example: Check if prone patients are on mechanical ventilation

# Assuming you have respiratory support data loaded
# prone_on_vent = prone.merge(
#     respiratory_support.df[['hospitalization_id', 'recorded_dttm', 'device_category']],
#     on='hospitalization_id',
#     how='inner'
# )

# Filter for concurrent times and IMV
# This would show correlation between prone positioning and ventilation
```

## Data Quality Considerations

- **Position Categories**: Ensure all positions map to standard categories
- **Documentation Frequency**: Check for appropriate position documentation intervals
- **Temporal Consistency**: Verify position changes are logically sequenced
- **Duration Validation**: Check for unrealistic position durations

```python
# Data quality checks
# Check for valid position categories
valid_categories = ['prone', 'supine', 'lateral', 'sitting', 'standing', 'ambulating']
invalid_positions = position.df[
    ~position.df['position_category'].isin(valid_categories)
]
if not invalid_positions.empty:
    print(f"Found {len(invalid_positions)} records with non-standard position categories")
    print(f"Invalid categories: {invalid_positions['position_category'].unique()}")

# Check documentation frequency
patient_sample = position.df['hospitalization_id'].unique()[:20]
doc_frequencies = []

for patient_id in patient_sample:
    patient_pos = position.df[
        position.df['hospitalization_id'] == patient_id
    ].sort_values('recorded_dttm')
    
    if len(patient_pos) > 1:
        # Calculate time between position documentations
        time_gaps = patient_pos['recorded_dttm'].diff().dropna()
        avg_gap = time_gaps.mean().total_seconds() / 3600
        doc_frequencies.append(avg_gap)

if doc_frequencies:
    print(f"\nAverage time between position documentation: {np.mean(doc_frequencies):.1f} hours")
    
    # Flag patients with documentation gaps > 12 hours
    large_gaps = [f for f in doc_frequencies if f > 12]
    if large_gaps:
        print(f"Patients with documentation gaps >12 hours: {len(large_gaps)}")

# Check for impossible position sequences
# (e.g., ambulating ’ prone without intermediate position)
sequence_issues = []
for patient_id in patient_sample:
    patient_pos = position.df[
        position.df['hospitalization_id'] == patient_id
    ].sort_values('recorded_dttm')
    
    for i in range(1, len(patient_pos)):
        prev_pos = patient_pos.iloc[i-1]['position_category']
        curr_pos = patient_pos.iloc[i]['position_category']
        
        # Check for direct ambulating to prone transition
        if prev_pos == 'ambulating' and curr_pos == 'prone':
            sequence_issues.append(patient_id)
            break

if sequence_issues:
    print(f"\nPatients with questionable position sequences: {len(sequence_issues)}")
```

## Related Tables

- **Hospitalization**: Primary link via hospitalization_id
- **Respiratory Support**: Correlate prone positioning with ventilation parameters
- **Vitals**: Analyze SpO2/PaO2 changes with position changes
- **ADT**: Verify positioning aligns with unit capabilities (ICU for prone)
- **Patient Assessments**: Link mobility with functional assessments

## API Reference

For detailed API documentation, see [Position API](../../api/tables.md#position)