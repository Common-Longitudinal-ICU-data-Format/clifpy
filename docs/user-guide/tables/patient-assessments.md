# Patient Assessments Table

## Overview

The Patient Assessments table contains clinical assessment scores and scales used to evaluate patient status, including Glasgow Coma Scale (GCS), Richmond Agitation-Sedation Scale (RASS), pain scores, and other standardized assessments. These assessments are crucial for monitoring neurological status, sedation levels, and overall patient comfort.

## Data Dictionary

For the complete field definitions and requirements, see the [official CLIF Data Dictionary for Patient Assessments](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#patient_assessments).

## Key Fields

- `hospitalization_id`: Links to hospitalization table
- `assessment_dttm`: Timestamp when assessment was performed
- `assessment_name`: Type of assessment (e.g., gcs_total, rass, pain_scale)
- `assessment_category`: Category of assessment (neurological, pain, sedation, etc.)
- `assessment_value`: Numeric score or value
- `assessment_value_text`: Text description of assessment value

## Common Usage Patterns

### Loading Patient Assessments Data

```python
from clifpy.tables import PatientAssessments

# Load patient assessments data
assessments = PatientAssessments.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Central'
)

# Validate the data
assessments.validate()
```

### Glasgow Coma Scale Analysis

```python
# Get GCS scores
gcs_total = assessments.df[assessments.df['assessment_name'] == 'gcs_total']
gcs_eye = assessments.df[assessments.df['assessment_name'] == 'gcs_eye']
gcs_verbal = assessments.df[assessments.df['assessment_name'] == 'gcs_verbal']
gcs_motor = assessments.df[assessments.df['assessment_name'] == 'gcs_motor']

# Analyze GCS distribution
print(f"GCS Total scores - Mean: {gcs_total['assessment_value'].mean():.1f}")
print(f"GCS Total scores - Median: {gcs_total['assessment_value'].median():.0f}")

# Find severely impaired patients (GCS d 8)
severe_gcs = gcs_total[gcs_total['assessment_value'] <= 8]
severe_patients = severe_gcs['hospitalization_id'].unique()
print(f"\nPatients with severe GCS (d8): {len(severe_patients)}")

# GCS component analysis
if not gcs_eye.empty and not gcs_verbal.empty and not gcs_motor.empty:
    print(f"\nGCS Components:")
    print(f"  Eye: Mean {gcs_eye['assessment_value'].mean():.1f}")
    print(f"  Verbal: Mean {gcs_verbal['assessment_value'].mean():.1f}")
    print(f"  Motor: Mean {gcs_motor['assessment_value'].mean():.1f}")
```

### RASS (Sedation) Monitoring

```python
# Analyze RASS scores
rass = assessments.df[assessments.df['assessment_name'] == 'rass']

if not rass.empty:
    # RASS distribution
    rass_dist = rass['assessment_value'].value_counts().sort_index()
    print("\nRASS Score Distribution:")
    rass_categories = {
        4: "Combative",
        3: "Very agitated",
        2: "Agitated",
        1: "Restless",
        0: "Alert and calm",
        -1: "Drowsy",
        -2: "Light sedation",
        -3: "Moderate sedation",
        -4: "Deep sedation",
        -5: "Unarousable"
    }
    
    for score, count in rass_dist.items():
        desc = rass_categories.get(int(score), "Unknown")
        print(f"  {score} ({desc}): {count} assessments")
    
    # Find over-sedated patients (RASS d -3)
    over_sedated = rass[rass['assessment_value'] <= -3]
    print(f"\nOver-sedation assessments (RASS d -3): {len(over_sedated)}")
```

### Pain Score Analysis

```python
# Analyze pain scores
pain = assessments.df[assessments.df['assessment_category'] == 'pain']

if not pain.empty:
    # Pain score statistics
    print(f"\nPain scores - Mean: {pain['assessment_value'].mean():.1f}")
    print(f"Pain scores - Median: {pain['assessment_value'].median():.0f}")
    
    # Categorize pain levels (assuming 0-10 scale)
    pain['pain_category'] = pd.cut(
        pain['assessment_value'],
        bins=[-0.1, 3, 6, 10],
        labels=['Mild (0-3)', 'Moderate (4-6)', 'Severe (7-10)']
    )
    
    pain_dist = pain['pain_category'].value_counts()
    print("\nPain Level Distribution:")
    for level, count in pain_dist.items():
        print(f"  {level}: {count} ({count/len(pain)*100:.1f}%)")
    
    # Find uncontrolled pain (score e 7)
    severe_pain = pain[pain['assessment_value'] >= 7]
    print(f"\nSevere pain assessments (e7): {len(severe_pain)}")
```

### Tracking Assessment Trends

```python
# Track GCS trend for a patient
def track_gcs_trend(hospitalization_id):
    """Track GCS trend over time for a patient"""
    patient_gcs = gcs_total[
        gcs_total['hospitalization_id'] == hospitalization_id
    ].sort_values('assessment_dttm')
    
    if patient_gcs.empty:
        return None
    
    # Calculate GCS changes
    patient_gcs['gcs_change'] = patient_gcs['assessment_value'].diff()
    
    # Find improvements and deteriorations
    improvements = patient_gcs[patient_gcs['gcs_change'] > 0]
    deteriorations = patient_gcs[patient_gcs['gcs_change'] < 0]
    
    return {
        'initial_gcs': patient_gcs.iloc[0]['assessment_value'],
        'final_gcs': patient_gcs.iloc[-1]['assessment_value'],
        'lowest_gcs': patient_gcs['assessment_value'].min(),
        'highest_gcs': patient_gcs['assessment_value'].max(),
        'improvements': len(improvements),
        'deteriorations': len(deteriorations),
        'total_assessments': len(patient_gcs)
    }

# Example usage
patient_trend = track_gcs_trend('H12345')
if patient_trend:
    print(f"GCS Trend Summary:")
    print(f"  Initial: {patient_trend['initial_gcs']}")
    print(f"  Final: {patient_trend['final_gcs']}")
    print(f"  Range: {patient_trend['lowest_gcs']}-{patient_trend['highest_gcs']}")
```

### Delirium Assessment

```python
# Check for CAM-ICU or other delirium assessments
delirium = assessments.df[
    assessments.df['assessment_name'].str.contains('cam|delirium', case=False, na=False)
]

if not delirium.empty:
    # Analyze delirium positive rates
    delirium_positive = delirium[
        (delirium['assessment_value'] == 1) | 
        (delirium['assessment_value_text'].str.contains('positive', case=False, na=False))
    ]
    
    delirium_rate = len(delirium_positive) / len(delirium) * 100
    print(f"\nDelirium positive rate: {delirium_rate:.1f}%")
    
    # Patients with delirium
    delirium_patients = delirium_positive['hospitalization_id'].unique()
    print(f"Patients with positive delirium screening: {len(delirium_patients)}")
```

### Creating Assessment Summaries

```python
# Create comprehensive assessment summary for a patient
def create_assessment_summary(hospitalization_id):
    """Create summary of all assessments for a patient"""
    patient_assess = assessments.df[
        assessments.df['hospitalization_id'] == hospitalization_id
    ]
    
    if patient_assess.empty:
        return None
    
    summary = {}
    
    # Group by assessment type
    for assess_name in patient_assess['assessment_name'].unique():
        assess_data = patient_assess[patient_assess['assessment_name'] == assess_name]
        
        summary[assess_name] = {
            'count': len(assess_data),
            'mean': assess_data['assessment_value'].mean(),
            'min': assess_data['assessment_value'].min(),
            'max': assess_data['assessment_value'].max(),
            'first': assess_data.iloc[0]['assessment_value'],
            'last': assess_data.iloc[-1]['assessment_value']
        }
    
    return summary

# Example usage
patient_summary = create_assessment_summary('H12345')
if patient_summary:
    print("Patient Assessment Summary:")
    for assess, stats in patient_summary.items():
        print(f"\n{assess}:")
        print(f"  Assessments: {stats['count']}")
        print(f"  Range: {stats['min']}-{stats['max']}")
        print(f"  FirstLast: {stats['first']}{stats['last']}")
```

## Data Quality Considerations

- **Score Ranges**: Verify assessment values are within valid ranges for each scale
- **Assessment Frequency**: Check for appropriate documentation frequency
- **Component Consistency**: For multi-component scores (like GCS), verify components sum correctly
- **Missing Assessments**: Identify gaps in critical assessments

```python
# Data quality checks
# Validate GCS total vs components
gcs_complete = assessments.df[
    assessments.df['assessment_name'].str.contains('gcs', case=False, na=False)
].pivot_table(
    index=['hospitalization_id', 'assessment_dttm'],
    columns='assessment_name',
    values='assessment_value'
)

if 'gcs_total' in gcs_complete.columns and all(col in gcs_complete.columns for col in ['gcs_eye', 'gcs_verbal', 'gcs_motor']):
    gcs_complete['calculated_total'] = (
        gcs_complete['gcs_eye'] + 
        gcs_complete['gcs_verbal'] + 
        gcs_complete['gcs_motor']
    )
    mismatched = gcs_complete[
        gcs_complete['gcs_total'] != gcs_complete['calculated_total']
    ]
    if not mismatched.empty:
        print(f"Found {len(mismatched)} GCS assessments with mismatched totals")

# Check for out-of-range values
range_checks = {
    'gcs_total': (3, 15),
    'gcs_eye': (1, 4),
    'gcs_verbal': (1, 5),
    'gcs_motor': (1, 6),
    'rass': (-5, 4),
    'pain_scale': (0, 10)
}

for assess_name, (min_val, max_val) in range_checks.items():
    assess_data = assessments.df[assessments.df['assessment_name'] == assess_name]
    if not assess_data.empty:
        out_of_range = assess_data[
            (assess_data['assessment_value'] < min_val) | 
            (assess_data['assessment_value'] > max_val)
        ]
        if not out_of_range.empty:
            print(f"{assess_name}: {len(out_of_range)} values outside valid range ({min_val}-{max_val})")
```

## Related Tables

- **Hospitalization**: Primary link via hospitalization_id
- **Medication Admin Continuous**: Correlate RASS scores with sedation dosing
- **Respiratory Support**: Match GCS/RASS with ventilation status
- **Vitals**: Compare neurological assessments with vital signs
- **ADT**: Verify ICU assessments align with ICU location

## API Reference

For detailed API documentation, see [PatientAssessments API](../../api/tables.md#patientassessments)