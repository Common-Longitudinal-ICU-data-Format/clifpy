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
from pyclif import ClifOrchestrator

# Initialize with your data
co = ClifOrchestrator(
    data_directory="/path/to/CLIF_data",
    filetype='parquet',
    timezone="US/Eastern"
)

# Create a wide dataset (tables auto-loaded as needed)
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['map', 'heart_rate', 'spo2'],
        'labs': ['hemoglobin', 'wbc', 'sodium']
    },
    sample=True  # 20 random hospitalizations
)

# Access the wide dataset
wide_df = co.wide_df

# OPTIONAL: Convert to temporal aggregation (hourly dataset)
hourly_df = co.convert_wide_to_hourly(
    aggregation_config={
        'mean': ['heart_rate', 'map'],
        'median': ['hemoglobin', 'sodium']
    },
    hourly_window=1  # 1-hour windows
)
```

**Note**: The wide dataset method `create_wide_dataset()` stores the result in `co.wide_df`. For temporal aggregation capabilities, see the [Temporal Aggregation](#temporal-aggregation-hourly-dataset) section.

## Core Functionality

### Base Tables (Always Included)
- **patient**: Demographics and patient information
- **hospitalization**: Admission/discharge details
- **adt**: Admission, discharge, and transfer events

### Optional Tables (User-Specified)

The tables available for wide dataset creation are defined in `clifpy/schemas/wide_tables_config.yaml`.

**Currently Supported Tables:**

**Pivot Tables** (narrow to wide conversion):
- **vitals**: Vital signs (pivoted by `vital_category`)
- **labs**: Laboratory results (pivoted by `lab_category`)
- **medication_admin_continuous**: Continuous medications (pivoted by `med_category`)
- **medication_admin_intermittent**: Intermittent medications (pivoted by `med_category`)
- **patient_assessments**: Clinical assessments (pivoted by `assessment_category`)

**Wide Tables** (already in wide format):
- **respiratory_support**: Respiratory support data (column selection)

**Note:** Additional tables can be enabled by updating `clifpy/schemas/wide_tables_config.yaml`. Tables like `crrt_therapy`, `position`, `ecmo_mcs`, and `code_status` are configured but not yet enabled by default.

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

Category values are defined in each table's schema file (`clifpy/schemas/*_schema.yaml`).

### Vitals Categories
Common vital sign categories include:
- `map`, `heart_rate`, `sbp`, `dbp`, `spo2`, `respiratory_rate`, `temp_c`, `weight_kg`, `height_cm`

### Labs Categories
Common laboratory categories include:
- `hemoglobin`, `wbc`, `sodium`, `potassium`, `creatinine`, `bun`, `glucose`, `lactate`

### Medication Categories
Common medication categories include:
- `norepinephrine`, `epinephrine`, `phenylephrine`, `vasopressin`, `dopamine`
- `propofol`, `fentanyl`, `midazolam`, `lorazepam`, `morphine`

### Assessment Categories
Common assessment categories include:
- `gcs_total`, `rass`, `sbt_delivery_pass_fail`, `sat_delivery_pass_fail`
- `sbt_screen_pass_fail`, `sat_screen_pass_fail`

**To check available categories in your data:**
```python
# For any loaded table
print(clif.vitals.df['vital_category'].unique())
print(clif.labs.df['lab_category'].unique())
```

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

## Temporal Aggregation (Hourly Dataset)

The `convert_wide_to_hourly()` method transforms wide time-series datasets into temporally aggregated data with flexible window sizes and user-defined aggregation methods. This feature is essential for creating analysis-ready datasets with consistent time intervals.

### Overview

**What is Temporal Aggregation?**

Temporal aggregation converts irregularly-spaced time-series events into fixed-window aggregates. For example, multiple vital sign measurements within an hour can be aggregated into a single hourly record using various statistical methods (mean, max, min, etc.).

**Key Benefits:**
- **Reduced Data Density**: Convert high-frequency data into manageable time windows
- **Aligned Observations**: Synchronize measurements from different sources to common time intervals
- **Feature Engineering**: Create time-based features for machine learning (e.g., hourly averages, daily maximums)
- **Missing Data Handling**: Option to create complete time series with gap filling
- **High Performance**: Uses DuckDB for efficient processing of large datasets

### Quick Start

```python
from pyclif import ClifOrchestrator

# Create orchestrator and wide dataset
co = ClifOrchestrator(config_path='config.json')
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'spo2', 'map'],
        'labs': ['hemoglobin', 'sodium', 'creatinine']
    }
)

# Convert to hourly aggregation
hourly_df = co.convert_wide_to_hourly(
    aggregation_config={
        'mean': ['heart_rate', 'sbp', 'map'],
        'max': ['heart_rate', 'sbp'],
        'min': ['spo2'],
        'median': ['hemoglobin', 'sodium']
    },
    hourly_window=1,  # 1-hour windows
    fill_gaps=False   # Sparse output (only windows with data)
)
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `aggregation_config` | Dict[str, List[str]] | **Required** | Mapping of aggregation methods to column lists |
| `wide_df` | pd.DataFrame | None | Wide dataset to aggregate. If None, uses `self.wide_df` |
| `id_name` | str | 'hospitalization_id' | Column for grouping: 'hospitalization_id' or 'encounter_block' |
| `hourly_window` | int | 1 | Window size in hours (1-72). Common values: 1 (hourly), 6 (quarter-day), 12 (half-day), 24 (daily) |
| `fill_gaps` | bool | False | If True, create rows for all windows (0 to max) with NaN for missing data. If False, only windows with data appear (sparse) |
| `memory_limit` | str | '4GB' | DuckDB memory limit (e.g., '4GB', '8GB', '16GB') |
| `temp_directory` | str | None | Directory for temporary files (default: system temp) |
| `batch_size` | int | None | Process in batches if specified (auto-determined if None) |

### Aggregation Methods

The `aggregation_config` parameter maps aggregation methods to lists of column names. Each column will be aggregated using the specified method and renamed with an appropriate suffix.

#### Statistical Aggregations

**`max`**: Maximum value in window
```python
aggregation_config = {
    'max': ['heart_rate', 'sbp', 'temp_c']
}
# Creates columns: heart_rate_max, sbp_max, temp_c_max
```

**`min`**: Minimum value in window
```python
aggregation_config = {
    'min': ['spo2', 'map', 'dbp']
}
# Creates columns: spo2_min, map_min, dbp_min
```

**`mean`**: Average value in window
```python
aggregation_config = {
    'mean': ['heart_rate', 'respiratory_rate', 'glucose']
}
# Creates columns: heart_rate_mean, respiratory_rate_mean, glucose_mean
```

**`median`**: Median value in window
```python
aggregation_config = {
    'median': ['sodium', 'potassium', 'creatinine']
}
# Creates columns: sodium_median, potassium_median, creatinine_median
```

#### Temporal Aggregations

**`first`**: First (earliest) value in window
```python
aggregation_config = {
    'first': ['gcs_total', 'rass', 'device_category']
}
# Creates columns: gcs_total_first, rass_first, device_category_first
```

**`last`**: Last (most recent) value in window
```python
aggregation_config = {
    'last': ['cam_total', 'braden_total']
}
# Creates columns: cam_total_last, braden_total_last
```

#### Presence Detection

**`boolean`**: Binary indicator (1 if any data exists, 0 if no data)
```python
aggregation_config = {
    'boolean': ['norepinephrine', 'propofol', 'fentanyl']
}
# Creates columns: norepinephrine_boolean, propofol_boolean, fentanyl_boolean
# Values: 1 = medication present in window, 0 = not present
```

#### Categorical Encoding

**`one_hot_encode`**: Creates binary columns for each unique categorical value
```python
aggregation_config = {
    'one_hot_encode': ['device_category', 'mode_category']
}
# Creates columns like: device_category_Mechanical_Ventilation,
#                       device_category_High_Flow_Nasal_Cannula, etc.
# Values: 1 = category present in window, 0 = not present
```

**Note**: One-hot encoding is limited to 100 unique values per column. Columns with >50 values will generate a warning.

### Window Types and Gap Filling

#### Event-Based Windowing

Windows are **relative to each group's first event**, not calendar boundaries:

```
Window 0: [first_event, first_event + hourly_window hours)
Window 1: [first_event + hourly_window, first_event + 2*hourly_window)
Window N: [first_event + N*hourly_window, ...)
```

**Example**: If a patient's first event is at 2024-01-15 14:30:00 with `hourly_window=6`:
- Window 0: 2024-01-15 14:30:00 to 2024-01-15 20:30:00
- Window 1: 2024-01-15 20:30:00 to 2024-01-16 02:30:00
- Window 2: 2024-01-16 02:30:00 to 2024-01-16 08:30:00

#### Sparse vs Dense Output

**Sparse Output (`fill_gaps=False`)** - Default behavior:
- Only creates rows for windows that contain actual data
- Missing windows are not represented in the output
- Smaller dataset size, efficient for most analyses
- Gaps can be identified by non-consecutive window numbers

**Dense Output (`fill_gaps=True`)**:
- Creates rows for ALL windows from 0 to maximum window per group
- Missing windows filled with NaN values (not forward-filled)
- Larger dataset size, useful for time-series ML models
- Ensures regular time intervals for all groups

**Example**:
```python
# If a hospitalization has data at windows 0, 1, 5, 8

# Sparse output (fill_gaps=False):
# Creates 4 rows with window_numbers: 0, 1, 5, 8

# Dense output (fill_gaps=True):
# Creates 9 rows with window_numbers: 0, 1, 2, 3, 4, 5, 6, 7, 8
# Windows 2, 3, 4, 6, 7 have NaN for all aggregated columns
```

### Output Structure

The aggregated dataset contains the following columns:

#### Group & Window Identifiers
- `{id_name}`: Group identifier (`hospitalization_id` or `encounter_block`)
- `window_number`: Sequential window index (0-indexed, starts at 0 for each group)
- `window_start_dttm`: Window start timestamp (inclusive)
- `window_end_dttm`: Window end timestamp (exclusive, equals start + hourly_window hours)

#### Context Columns
- `patient_id`: Patient identifier
- `day_number`: Day number within hospitalization

#### Aggregated Columns
All columns specified in `aggregation_config` with appropriate suffixes:
- Statistical: `{column}_max`, `{column}_min`, `{column}_mean`, `{column}_median`
- Temporal: `{column}_first`, `{column}_last`
- Presence: `{column}_boolean`
- One-hot: `{column}_{category_value}` (e.g., `device_category_Mechanical_Ventilation`)

**Example Output Schema**:
```
hospitalization_id | window_number | window_start_dttm   | window_end_dttm     | patient_id | day_number | heart_rate_mean | sbp_max | spo2_min
-------------------|---------------|---------------------|---------------------|------------|------------|-----------------|---------|----------
H001               | 0             | 2024-01-15 08:00:00 | 2024-01-15 09:00:00 | P001       | 1          | 75.3            | 135     | 94
H001               | 1             | 2024-01-15 09:00:00 | 2024-01-15 10:00:00 | P001       | 1          | 78.1            | 142     | 92
```

### Usage Examples

#### Example 1: Standard Hourly Aggregation

Create hourly windows with multiple aggregation methods:

```python
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'dbp', 'spo2', 'map'],
        'labs': ['hemoglobin', 'wbc', 'sodium', 'creatinine']
    }
)

hourly_df = co.convert_wide_to_hourly(
    aggregation_config={
        'mean': ['heart_rate', 'sbp', 'dbp', 'map'],
        'max': ['heart_rate', 'sbp'],
        'min': ['spo2', 'dbp'],
        'median': ['hemoglobin', 'sodium', 'creatinine']
    },
    hourly_window=1,
    fill_gaps=False
)

print(f"Shape: {hourly_df.shape}")
print(f"Columns: {hourly_df.columns.tolist()}")
```

#### Example 2: 6-Hour Windows for Daily Analysis

Aggregate data into 6-hour blocks (quarter-day intervals):

```python
co.create_wide_dataset(
    tables_to_load=['vitals', 'medication_admin_continuous'],
    category_filters={
        'vitals': ['heart_rate', 'map', 'temp_c'],
        'medication_admin_continuous': ['norepinephrine', 'propofol']
    }
)

six_hour_df = co.convert_wide_to_hourly(
    aggregation_config={
        'mean': ['heart_rate', 'map', 'temp_c'],
        'max': ['heart_rate', 'map'],
        'boolean': ['norepinephrine', 'propofol']
    },
    hourly_window=6,
    fill_gaps=False
)

# Window 0: Hours 0-6, Window 1: Hours 6-12, Window 2: Hours 12-18, Window 3: Hours 18-24
```

#### Example 3: Daily Aggregation

Create daily summaries with 24-hour windows:

```python
daily_df = co.convert_wide_to_hourly(
    aggregation_config={
        'mean': ['heart_rate', 'sbp', 'map'],
        'max': ['temp_c', 'heart_rate'],
        'min': ['spo2', 'map'],
        'median': ['hemoglobin', 'sodium', 'creatinine']
    },
    hourly_window=24,  # Daily windows
    fill_gaps=True     # Ensure all days are present
)
```

#### Example 4: Using Encounter Blocks After Stitching

Group by encounter blocks instead of individual hospitalizations:

```python
# Enable encounter stitching
co = ClifOrchestrator(
    config_path='config.json',
    stitch_encounter=True,
    stitch_time_interval=6
)

co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'spo2'],
        'labs': ['hemoglobin', 'creatinine']
    }
)

# Aggregate by encounter block (treats linked hospitalizations as one unit)
hourly_df = co.convert_wide_to_hourly(
    aggregation_config={
        'mean': ['heart_rate', 'sbp', 'spo2'],
        'median': ['hemoglobin', 'creatinine']
    },
    id_name='encounter_block',  # Group by encounter instead of hospitalization
    hourly_window=1
)
```

#### Example 5: Gap Filling for ML Pipelines

Create complete time series with gap filling for machine learning:

```python
# Dense output ensures regular time intervals for LSTM/RNN models
ml_ready_df = co.convert_wide_to_hourly(
    aggregation_config={
        'mean': ['heart_rate', 'sbp', 'map', 'respiratory_rate'],
        'last': ['gcs_total', 'rass']
    },
    hourly_window=1,
    fill_gaps=True  # Creates rows for ALL windows, gaps filled with NaN
)

# Now every hospitalization has a complete sequence of windows 0, 1, 2, ..., N
# Missing data appears as NaN (ready for imputation strategies)
```

#### Example 6: One-Hot Encoding Categorical Variables

Encode categorical variables as binary indicators:

```python
co.create_wide_dataset(
    tables_to_load=['respiratory_support', 'patient_assessments'],
    category_filters={
        'respiratory_support': ['device_category', 'mode_category'],
        'patient_assessments': ['rass', 'cam_total']
    }
)

encoded_df = co.convert_wide_to_hourly(
    aggregation_config={
        'one_hot_encode': ['device_category', 'mode_category'],
        'first': ['rass', 'cam_total']
    },
    hourly_window=2
)

# Creates binary columns for each unique device/mode:
# device_category_Mechanical_Ventilation, device_category_High_Flow_Nasal_Cannula, etc.
```

#### Example 7: Mixed Aggregation Methods

Use different aggregation methods for the same column:

```python
comprehensive_df = co.convert_wide_to_hourly(
    aggregation_config={
        'mean': ['heart_rate', 'sbp', 'map'],
        'max': ['heart_rate', 'sbp', 'temp_c'],
        'min': ['heart_rate', 'spo2', 'map'],
        'median': ['glucose', 'lactate'],
        'first': ['gcs_total'],
        'last': ['rass'],
        'boolean': ['norepinephrine', 'propofol', 'fentanyl']
    },
    hourly_window=1
)

# heart_rate appears in 3 aggregations: heart_rate_mean, heart_rate_max, heart_rate_min
# This allows capturing different aspects of the same variable
```

### Integration with Wide Dataset Workflow

Complete workflow from data loading to hourly aggregation:

```python
from pyclif import ClifOrchestrator

# 1. Initialize orchestrator
co = ClifOrchestrator(config_path='config.json')

# 2. Create wide dataset with desired tables and categories
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs', 'medication_admin_continuous'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'dbp', 'spo2', 'map', 'temp_c'],
        'labs': ['hemoglobin', 'wbc', 'sodium', 'potassium', 'creatinine'],
        'medication_admin_continuous': ['norepinephrine', 'epinephrine', 'propofol']
    }
)

# 3. Convert to hourly aggregation
hourly_df = co.convert_wide_to_hourly(
    aggregation_config={
        'mean': ['heart_rate', 'sbp', 'dbp', 'map', 'temp_c'],
        'max': ['heart_rate', 'sbp', 'temp_c'],
        'min': ['spo2', 'dbp', 'map'],
        'median': ['hemoglobin', 'wbc', 'sodium', 'potassium', 'creatinine'],
        'boolean': ['norepinephrine', 'epinephrine', 'propofol']
    },
    hourly_window=1,
    fill_gaps=False
)

# 4. Analyze aggregated data
print(f"Dataset shape: {hourly_df.shape}")
print(f"Unique hospitalizations: {hourly_df['hospitalization_id'].nunique()}")
print(f"Total time windows: {len(hourly_df)}")
print(f"Average windows per hospitalization: {len(hourly_df) / hourly_df['hospitalization_id'].nunique():.1f}")

# 5. Save or continue analysis
hourly_df.to_parquet('hourly_aggregated_dataset.parquet')
```

### Performance Considerations

#### Batch Processing

For large datasets (>1M rows or >10K hospitalizations), the function automatically uses batch processing:

```python
hourly_df = co.convert_wide_to_hourly(
    aggregation_config=config,
    hourly_window=1,
    batch_size=5000,  # Process 5000 hospitalizations per batch
    memory_limit='8GB'  # Increase memory limit for large batches
)
```

#### Memory Management

- **Default memory limit**: 4GB (configurable)
- **Auto-batching**: Triggered for datasets >1M rows or >10K groups
- **Batch size**: Auto-determined or manually specified
- **Temp files**: Stored in system temp directory (configurable)

```python
# For very large datasets
hourly_df = co.convert_wide_to_hourly(
    aggregation_config=config,
    memory_limit='16GB',
    temp_directory='/path/to/fast/storage',
    batch_size=2000
)
```

#### Choosing Window Sizes

| Window Size | Use Case | Typical Output Size |
|-------------|----------|---------------------|
| 1 hour | High-resolution analysis, detailed trends | Large (1 row per hour) |
| 2 hours | Balanced resolution and size | Medium-large |
| 6 hours | Quarter-day analysis (morning/afternoon/evening/night) | Medium |
| 12 hours | Bi-daily patterns | Small-medium |
| 24 hours | Daily summaries | Small |
| 72 hours | Multi-day trends (maximum) | Very small |

### Best Practices

1. **Start with Sparse Output**: Use `fill_gaps=False` initially to understand your data density
   ```python
   hourly_df = co.convert_wide_to_hourly(config, fill_gaps=False)
   # Check window distribution
   print(hourly_df.groupby('hospitalization_id')['window_number'].max().describe())
   ```

2. **Choose Appropriate Window Sizes**: Balance temporal resolution with data volume
   - High-frequency data (vitals): 1-2 hour windows
   - Intermittent data (labs): 6-12 hour windows
   - Daily summaries: 24 hour windows

3. **Use Multiple Aggregations**: Capture different aspects of variables
   ```python
   aggregation_config = {
       'mean': ['heart_rate'],  # Average trend
       'max': ['heart_rate'],   # Peak values
       'min': ['heart_rate']    # Lowest values
   }
   ```

4. **Boolean for Presence Detection**: Use for medications and interventions
   ```python
   aggregation_config = {
       'boolean': ['norepinephrine', 'mechanical_ventilation']
   }
   # Answers: "Was this intervention present in this time window?"
   ```

5. **Gap Filling for ML**: Use `fill_gaps=True` for models requiring regular intervals
   ```python
   ml_df = co.convert_wide_to_hourly(config, fill_gaps=True, hourly_window=1)
   # Perfect for LSTM, RNN, or other sequence models
   ```

6. **Monitor Performance**: Use batch processing for large datasets
   ```python
   # Check dataset size first
   print(f"Wide dataset size: {co.wide_df.shape}")

   # Use batching if needed
   if len(co.wide_df) > 1_000_000:
       hourly_df = co.convert_wide_to_hourly(config, batch_size=5000)
   ```

7. **Validate Output**: Check aggregation results match expectations
   ```python
   # Verify window boundaries
   print(hourly_df[['window_number', 'window_start_dttm', 'window_end_dttm']].head())

   # Check time differences
   hourly_df['window_duration'] = (
       hourly_df['window_end_dttm'] - hourly_df['window_start_dttm']
   ).dt.total_seconds() / 3600
   print(f"All windows are {hourly_df['window_duration'].unique()[0]} hours")
   ```

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

## Extending Table Support

To add support for additional tables (e.g., `crrt_therapy`, `position`):

### Step 1: Check the Configuration File
Open `clifpy/schemas/wide_tables_config.yaml` and find your table:

```yaml
crrt_therapy:
  type: wide
  timestamp_column: recorded_dttm
  description: "CRRT therapy data"
  supported: false  # Change this to true
```

### Step 2: Enable the Table
Simply change `supported: false` to `supported: true`:

```yaml
crrt_therapy:
  type: wide
  timestamp_column: recorded_dttm
  description: "CRRT therapy data"
  supported: true  # Now enabled!
```

### Step 3: Test with Your Data
```python
clif = CLIF(data_dir="/path/to/data")
wide_df = clif.create_wide_dataset(
    optional_tables=['crrt_therapy'],
    category_filters={
        'crrt_therapy': ['crrt_mode_category', 'blood_flow_rate']
    }
)
```

### Adding Completely New Tables

If your table isn't in the config yet:

1. Add a new entry to `clifpy/schemas/wide_tables_config.yaml`:
```yaml
  my_new_table:
    type: pivot  # or 'wide'
    timestamp_column: recorded_dttm
    category_column: my_category  # for pivot tables
    value_column: my_value  # for pivot tables
    description: "Description of table"
    supported: true
```

2. Ensure the table schema exists in `clifpy/schemas/my_new_table_schema.yaml`

3. Test thoroughly with your data

## Best Practices

### For Wide Dataset Creation

1. **Start Small**: Use `sample=True` for initial testing
2. **Filter Categories**: Specify only needed categories to reduce memory usage
3. **Save Large Datasets**: Use file output for datasets > 1GB
4. **Check Data Quality**: Validate timestamp alignment and missing values
5. **Document Choices**: Record which categories and filters were used
6. **Check Config**: Review `clifpy/schemas/wide_tables_config.yaml` for supported tables

### For Temporal Aggregation

7. **Choose the Right Window Size**: Match window size to your analysis needs
   - **High-frequency monitoring**: 1-2 hour windows for vitals trending
   - **Clinical decision points**: 6-hour windows for shift-based assessments
   - **Daily summaries**: 24-hour windows for outcome studies
   - **Multi-day trends**: 72-hour windows for weekly patterns

8. **Start with Sparse Output**: Use `fill_gaps=False` initially to understand data density, then switch to `fill_gaps=True` only if needed for ML models

9. **Use Appropriate Aggregation Methods**: Match aggregation method to data type
   - **Continuous numeric** (vitals, labs): Use `mean`, `median`, `max`, `min`
   - **Categorical** (device types, modes): Use `first`, `last`, or `one_hot_encode`
   - **Binary presence** (medications, interventions): Use `boolean`
   - **Multiple aspects**: Apply different methods to same column (e.g., `mean` and `max` for heart rate)

10. **Consider Grouping Strategy**: Choose `id_name` based on analysis scope
    - `hospitalization_id`: Treat each admission independently
    - `encounter_block`: Analyze linked admissions as continuous care episodes (requires encounter stitching)

11. **Plan for Missing Data**: Decide how to handle gaps
    - `fill_gaps=False`: Sparse, efficient, suitable for most analyses
    - `fill_gaps=True`: Dense, complete time series, required for many ML models

12. **Monitor Memory Usage**: For large datasets, use batch processing and adjust memory limits
    ```python
    # Check size before aggregating
    print(f"Wide dataset: {co.wide_df.shape}")

    # Use batching if needed
    if len(co.wide_df) > 1_000_000:
        hourly_df = co.convert_wide_to_hourly(
            aggregation_config=config,
            batch_size=5000,
            memory_limit='8GB'
        )
    ```

13. **Validate Aggregation Results**: Check that windows and aggregations make clinical sense
    ```python
    # Verify window consistency
    hourly_df['window_hours'] = (
        (hourly_df['window_end_dttm'] - hourly_df['window_start_dttm'])
        .dt.total_seconds() / 3600
    )
    assert hourly_df['window_hours'].nunique() == 1, "Inconsistent window sizes!"

    # Check aggregation ranges
    print(hourly_df[['heart_rate_mean', 'heart_rate_max', 'heart_rate_min']].describe())
    ```

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

**Unsupported Table Error**
```python
# Check if table is supported in config
import yaml
with open('clifpy/schemas/wide_tables_config.yaml') as f:
    config = yaml.safe_load(f)
    table_config = config['tables'].get('my_table')
    if table_config:
        print(f"Table type: {table_config.get('type')}")
        print(f"Supported: {table_config.get('supported')}")
    else:
        print("Table not in config - needs to be added")
```

## Integration with Existing Workflow

The wide dataset and temporal aggregation functions are designed to integrate seamlessly with existing pyCLIF workflows:

### Complete Analysis Pipeline

```python
from pyclif import ClifOrchestrator

# 1. Initialize orchestrator
co = ClifOrchestrator(
    data_directory="/path/to/data",
    filetype='parquet',
    timezone='US/Eastern'
)

# 2. Create wide dataset with desired tables
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs', 'medication_admin_continuous'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'map', 'spo2'],
        'labs': ['hemoglobin', 'creatinine', 'sodium'],
        'medication_admin_continuous': ['norepinephrine', 'propofol']
    }
)

# 3. Optional: Convert to hourly aggregation
hourly_df = co.convert_wide_to_hourly(
    aggregation_config={
        'mean': ['heart_rate', 'sbp', 'map'],
        'max': ['heart_rate', 'sbp'],
        'min': ['spo2'],
        'median': ['hemoglobin', 'creatinine', 'sodium'],
        'boolean': ['norepinephrine', 'propofol']
    },
    hourly_window=1,
    fill_gaps=False
)

# 4. Continue with your analysis
# Example: Calculate summary statistics
summary = hourly_df.groupby('hospitalization_id').agg({
    'heart_rate_mean': ['mean', 'std'],
    'sbp_max': 'max',
    'norepinephrine_boolean': 'sum'  # Total hours on norepinephrine
})

# Example: Prepare for machine learning
from sklearn.model_selection import train_test_split

# Create features from hourly data
X = hourly_df[[c for c in hourly_df.columns if '_mean' in c or '_max' in c]]
y = your_outcome_variable

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
```

### Flexible Workflow Options

```python
# Option 1: Just wide dataset (no aggregation)
co.create_wide_dataset(tables_to_load=['vitals'], category_filters={'vitals': ['heart_rate']})
detailed_analysis = analyze_raw_events(co.wide_df)

# Option 2: Wide dataset + hourly aggregation
co.create_wide_dataset(tables_to_load=['vitals'], category_filters={'vitals': ['heart_rate']})
hourly_df = co.convert_wide_to_hourly(aggregation_config={'mean': ['heart_rate']})
time_series_analysis = analyze_hourly(hourly_df)

# Option 3: Multiple aggregation strategies
co.create_wide_dataset(tables_to_load=['vitals'], category_filters={'vitals': ['heart_rate']})

# Create multiple aggregated views
hourly_vital_trends = co.convert_wide_to_hourly(
    aggregation_config={'mean': ['heart_rate'], 'max': ['heart_rate']},
    hourly_window=1
)

daily_summaries = co.convert_wide_to_hourly(
    aggregation_config={'mean': ['heart_rate'], 'max': ['heart_rate'], 'min': ['heart_rate']},
    hourly_window=24
)
```

---

This functionality brings comprehensive wide dataset creation and temporal aggregation capabilities into a reusable, configurable, and robust framework that can be easily integrated into any pyCLIF workflow. Whether you need raw event-level data or aggregated time-series features, the system provides flexible options to support diverse analytical needs.