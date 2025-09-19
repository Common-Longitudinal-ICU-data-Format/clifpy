# ADT (Admission, Discharge, Transfer) Table

## Overview

The ADT table tracks patient movement throughout their hospital stay, including admissions, discharges, and transfers between different units and levels of care. This table is essential for understanding patient flow, calculating length of stay, and identifying ICU admissions.

## Data Dictionary

For the complete field definitions and requirements, see the [official CLIF Data Dictionary for ADT](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#adt).

## Key Fields

- `hospitalization_id`: Links to hospitalization table
- `in_dttm` / `out_dttm`: Timestamps for unit entry/exit  
- `location_category`: Standardized location (icu, ward, ed, or, pacu)
- `location_name`: Specific unit/ward name
- `hospital_id` / `hospital_type`: Facility information

## Common Usage Patterns

### Loading ADT Data

```python
from clifpy.tables import Adt

# Load ADT data
adt = Adt.from_file(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Central'
)

# Validate the data
adt.validate()
```

### Finding ICU Stays

```python
# Get all ICU admissions
icu_stays = adt.df[adt.df['location_category'] == 'icu']

# Get unique ICU patients
icu_patients = icu_stays['hospitalization_id'].unique()
print(f"Number of ICU stays: {len(icu_stays)}")
print(f"Number of unique ICU patients: {len(icu_patients)}")
```

### Calculating Length of Stay

```python
# Calculate ICU length of stay in hours
icu_los = (icu_stays['out_dttm'] - icu_stays['in_dttm']).dt.total_seconds() / 3600
icu_stays['los_hours'] = icu_los

# Summary statistics
print(f"Median ICU LOS: {icu_los.median():.1f} hours")
print(f"Mean ICU LOS: {icu_los.mean():.1f} hours")
```

### Tracking Patient Movement

```python
# Get patient journey through hospital
patient_id = 'H12345'
patient_journey = adt.df[
    adt.df['hospitalization_id'] == patient_id
].sort_values('in_dttm')

# Print location sequence
for _, row in patient_journey.iterrows():
    print(f"{row['in_dttm']}: {row['location_category']} - {row['location_name']}")
```

## Data Quality Considerations

- **Timeline Continuity**: Verify no gaps between consecutive stays
- **Overlapping Stays**: Check for overlapping in/out times for same patient
- **Valid Categories**: Ensure location_category values are standardized
- **Timestamp Order**: Validate that out_dttm > in_dttm for all records
- **Missing Discharges**: Identify stays without out_dttm

```python
# Check for overlapping stays
overlaps = []
for hosp_id in adt.df['hospitalization_id'].unique():
    stays = adt.df[adt.df['hospitalization_id'] == hosp_id].sort_values('in_dttm')
    for i in range(len(stays) - 1):
        if stays.iloc[i]['out_dttm'] > stays.iloc[i + 1]['in_dttm']:
            overlaps.append(hosp_id)
            break

if overlaps:
    print(f"Found {len(overlaps)} hospitalizations with overlapping stays")
```

## Related Tables

- **Hospitalization**: Primary link via hospitalization_id
- **Labs/Vitals**: Join using patient_id and timestamp ranges
- **Respiratory Support**: Align ventilation periods with ICU stays
- **Medication Admin**: Track medications by location

## API Reference

For detailed API documentation, see [Adt API](../../api/tables.md#adt)