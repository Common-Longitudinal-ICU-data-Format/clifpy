# Wide Dataset Creation with pyCLIF

The pyCLIF library now includes powerful functionality for creating wide datasets by joining multiple CLIF tables with automatic pivoting of category-based data. This feature is designed to replicate and enhance the sophisticated wide dataset creation logic from the original notebook while making it reusable and configurable.

## Overview

The wide dataset functionality allows you to:
- **Automatically join** multiple CLIF tables (patient, hospitalization, ADT, and optional tables)
- **Pivot category-based data** from vitals, labs, medications, and assessments
- **Sample or filter** hospitalizations for targeted analysis
- **Handle time-based alignment** of events across different tables
- **Save results** in multiple formats (DataFrame, CSV, Parquet)

## Quick Start

```python
from pyclif import CLIF

# Initialize with your data
clif = CLIF(
    data_dir="/path/to/CLIF_data",
    filetype='parquet',
    timezone="US/Eastern"
)

# Create a wide dataset (tables auto-loaded as needed)
wide_df = clif.create_wide_dataset(
    optional_tables=['vitals', 'labs'],
    category_filters={
        'vitals': ['map', 'heart_rate', 'spo2'],
        'labs': ['hemoglobin', 'wbc', 'sodium']
    },
    sample=True  # 20 random hospitalizations
)
```

## Core Functionality

### Base Tables (Always Included)
- **patient**: Demographics and patient information
- **hospitalization**: Admission/discharge details
- **adt**: Admission, discharge, and transfer events

### Optional Tables (User-Specified)
- **vitals**: Vital signs (pivoted by `vital_category`)
- **labs**: Laboratory results (pivoted by `lab_category`)
- **medication_admin_continuous**: Continuous medications (pivoted by `med_category`)
- **patient_assessments**: Clinical assessments (pivoted by `assessment_category`)
- **respiratory_support**: Respiratory support data

## Parameters

### `create_wide_dataset()` Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `optional_tables` | List[str] | None | List of optional tables to include |
| `category_filters` | Dict[str, List[str]] | None | Categories to pivot for each table |
| `sample` | bool | False | If True, randomly select 20 hospitalizations |
| `hospitalization_ids` | List[str] | None | Specific hospitalization IDs to include |
| `output_format` | str | 'dataframe' | Output format: 'dataframe', 'csv', 'parquet' |
| `save_to_data_location` | bool | False | Save output to data directory |
| `output_filename` | str | None | Custom filename (auto-generated if None) |
| `auto_load` | bool | True | Automatically load missing tables |

## Usage Examples

### Example 1: Sample Mode
Create a wide dataset with 20 random hospitalizations:

```python
wide_df = clif.create_wide_dataset(
    optional_tables=['vitals', 'labs'],
    category_filters={
        'vitals': ['map', 'heart_rate', 'spo2', 'respiratory_rate'],
        'labs': ['hemoglobin', 'wbc', 'sodium', 'potassium']
    },
    sample=True,
    save_to_data_location=True,
    output_format='parquet'
)
```

### Example 2: Specific Hospitalizations
Target specific encounters for analysis:

```python
target_ids = ['12345', '67890', '11111']
wide_df = clif.create_wide_dataset(
    hospitalization_ids=target_ids,
    optional_tables=['medication_admin_continuous', 'patient_assessments'],
    category_filters={
        'medication_admin_continuous': ['norepinephrine', 'propofol', 'fentanyl'],
        'patient_assessments': ['gcs_total', 'rass', 'sbt_delivery_pass_fail']
    },
    output_filename='targeted_encounters'
)
```

### Example 3: Comprehensive Dataset
Create a full wide dataset with all optional tables:

```python
wide_df = clif.create_wide_dataset(
    optional_tables=['vitals', 'labs', 'medication_admin_continuous', 'patient_assessments'],
    category_filters={
        'vitals': ['map', 'heart_rate', 'spo2', 'respiratory_rate', 'temp_c'],
        'labs': ['hemoglobin', 'wbc', 'sodium', 'potassium', 'creatinine'],
        'medication_admin_continuous': ['norepinephrine', 'epinephrine', 'propofol'],
        'patient_assessments': ['gcs_total', 'rass', 'sbt_delivery_pass_fail']
    },
    save_to_data_location=True
)
```

## Available Categories

### Vitals Categories
Common vital sign categories include:
- `map`, `heart_rate`, `sbp`, `dbp`, `spo2`, `respiratory_rate`, `temp_c`, `weight_kg`, `height_cm`

### Labs Categories  
Common laboratory categories include:
- `hemoglobin`, `wbc`, `sodium`, `potassium`, `creatinine`, `bun`, `glucose`, `lactate`

### Medication Categories
Common continuous medication categories include:
- `norepinephrine`, `epinephrine`, `phenylephrine`, `vasopressin`, `dopamine`
- `propofol`, `fentanyl`, `midazolam`, `lorazepam`, `morphine`

### Assessment Categories
Common assessment categories include:
- `gcs_total`, `rass`, `sbt_delivery_pass_fail`, `sat_delivery_pass_fail`
- `sbt_screen_pass_fail`, `sat_screen_pass_fail`

## Output Structure

The resulting wide dataset includes:

### Core Columns
- Patient demographics (`patient_id`, `sex_category`, `race_category`, etc.)
- Hospitalization details (`hospitalization_id`, `admission_dttm`, `discharge_dttm`, etc.)
- Event timing (`event_time`, `day_number`, `hosp_id_day_key`)
- Location information (from ADT table)

### Pivoted Columns
- Individual columns for each specified category (e.g., `map`, `heart_rate`, `norepinephrine`)
- Values aligned by timestamp and hospitalization

### Time-Based Features
- `day_number`: Sequential day number within each hospitalization
- `hosp_id_day_key`: Unique identifier combining hospitalization and day
- `event_time`: Timestamp for each record

## Auto-Loading Feature

The function automatically loads required tables if they haven't been loaded yet:

```python
# No need to manually load tables
clif = CLIF(data_dir="/path/to/data", filetype='parquet')

# Tables will be auto-loaded as needed
wide_df = clif.create_wide_dataset(
    optional_tables=['vitals', 'labs']  # These will be loaded automatically
)
```

## Error Handling

The function includes robust error handling:

- **Missing tables**: Warns and skips if optional tables aren't available
- **Missing columns**: Handles alternative timestamp column names
- **Missing categories**: Adds NaN columns for standard assessments/medications
- **Empty data**: Gracefully handles cases where no data remains after filtering

## Performance Considerations

### Memory Optimization
- Use `sample=True` for testing and development
- Specify `hospitalization_ids` for targeted analysis
- Use `category_filters` to limit pivoted columns

### Output Management
- Use `save_to_data_location=True` for large datasets
- Choose `output_format='parquet'` for better compression
- Set `output_format='dataframe'` only for immediate analysis

## Implementation Details

### Temporal Alignment
The function creates a unified timeline by:
1. Collecting all unique timestamps from included tables
2. Creating a cartesian product of hospitalizations × timestamps
3. Joining table-specific data based on matching timestamps

### Pivoting Logic
Category-based tables are pivoted using DuckDB for performance:
- Creates unique combination IDs (`hospitalization_id_YYYYMMDDHHMM`)
- Pivots on category columns using `PIVOT` SQL operation
- Handles missing values and duplicate timestamps

### Data Integration
Tables are joined using a combination of:
- Hospitalization IDs for patient-level data
- Timestamp-based combo IDs for time-series data
- Left joins to preserve all timestamps

## Best Practices

1. **Start Small**: Use `sample=True` for initial testing
2. **Filter Categories**: Specify only needed categories to reduce memory usage
3. **Save Large Datasets**: Use file output for datasets > 1GB
4. **Check Data Quality**: Validate timestamp alignment and missing values
5. **Document Choices**: Record which categories and filters were used

## Troubleshooting

### Common Issues

**Memory Errors**
```python
# Use sampling or filtering
wide_df = clif.create_wide_dataset(sample=True)  # or
wide_df = clif.create_wide_dataset(hospitalization_ids=small_list)
```

**Missing Columns**
```python
# Check available categories in your data first
print(clif.vitals.df['vital_category'].unique())
```

**Empty Results**
```python
# Verify data exists for your filters
print(clif.hospitalization.df['hospitalization_id'].nunique())
```

## Integration with Existing Workflow

The wide dataset function is designed to integrate seamlessly with existing pyCLIF workflows:

```python
# Traditional approach
clif = CLIF(data_dir="/path/to/data")
clif.initialize(['patient', 'vitals', 'labs'])

# Enhanced with wide dataset
wide_df = clif.create_wide_dataset(
    optional_tables=['vitals', 'labs'],
    category_filters={'vitals': ['map'], 'labs': ['hemoglobin']}
)

# Continue with analysis
analysis_results = analyze_wide_dataset(wide_df)
```

---

This functionality brings the power of the original notebook's wide dataset creation into a reusable, configurable, and robust function that can be easily integrated into any pyCLIF workflow.