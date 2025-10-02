# Wide Dataset Creation

The wide dataset functionality enables you to create comprehensive time-series dataset by joining multiple CLIF tables with automatic pivoting and high-performance processing using DuckDB. This feature transforms narrow, category-based data (like vitals and labs) into wide format suitable for machine learning and analysis.

## Overview

Wide dataset creation through the ClifOrchestrator provides:

- **Automatic table joining** across patient, hospitalization, ADT, and optional tables
- **Intelligent pivoting** of category-based data (vitals, labs, medications, assessments) 
- **High-performance processing** using DuckDB for large datasets
- **Memory-efficient batch processing** to handle datasets of any size
- **Flexible filtering** by hospitalization IDs, time windows, and categories
- **System resource optimization** with configurable memory and thread settings

**Important**: Wide dataset functionality is only available through the `ClifOrchestrator` and requires specific tables to be loaded.

## Quick Start

```python
from clifpy.clif_orchestrator import ClifOrchestrator

# Initialize orchestrator
co = ClifOrchestrator(
    data_directory='/path/to/clif/data',
    filetype='parquet',
    timezone='US/Central'
)

# Create wide dataset with sample data
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        # Pivot tables - specify category VALUES to pivot into columns
        'vitals': ['heart_rate', 'sbp', 'spo2'],  # vital_category values
        'labs': ['hemoglobin', 'sodium', 'glucose']  # lab_category values
    },
    sample=True  # Use 20 random hospitalizations for testing
)

# Access the created wide dataset
wide_df = co.wide_df
```

## Accessing Wide Dataset Results

The `create_wide_dataset()` method stores the resulting DataFrame in the orchestrator's `wide_df` property rather than returning it directly. This approach provides several benefits:

- **Persistent storage**: The wide dataset remains accessible throughout your session
- **Memory management**: Avoids creating multiple copies of large DataFrames
- **Consistent access pattern**: Aligns with other orchestrator table properties

```python
# Create the wide dataset (no return value)
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={'vitals': ['heart_rate'], 'labs': ['hemoglobin']}
)

# Access the result via the property
wide_df = co.wide_df

# Check if wide dataset exists
if co.wide_df is not None:
    print(f"Wide dataset shape: {co.wide_df.shape}")
else:
    print("No wide dataset has been created yet")

# Direct access to the DataFrame
print(f"Hospitalizations: {co.wide_df['hospitalization_id'].nunique()}")
```

## Function Parameters

The `create_wide_dataset()` method accepts the following parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tables_to_load` | List[str] | None | Tables to include: 'vitals', 'labs', 'medication_admin_continuous', 'patient_assessments', 'respiratory_support' |
| `category_filters` | Dict[str, List[str]] | None | Specific categories to pivot for each table |
| `sample` | bool | False | If True, randomly select 20 hospitalizations for testing |
| `hospitalization_ids` | List[str] | None | Specific hospitalization IDs to process |
| `cohort_df` | DataFrame | None | DataFrame with time windows (requires 'hospitalization_id', 'start_time', 'end_time' columns) |
| `output_format` | str | 'dataframe' | Output format: 'dataframe', 'csv', or 'parquet' |
| `save_to_data_location` | bool | False | Save output to data directory |
| `output_filename` | str | None | Custom filename (auto-generated if None) |
| `return_dataframe` | bool | True | Store DataFrame in `wide_df` property even when saving to file |
| `batch_size` | int | 1000 | Number of hospitalizations per batch (use -1 to disable batching) |
| `memory_limit` | str | None | DuckDB memory limit (e.g., '8GB', '16GB') |
| `threads` | int | None | Number of threads for DuckDB processing |
| `show_progress` | bool | True | Show progress bars for long operations |

## Understanding category_filters: Pivot vs Wide Tables

The `category_filters` parameter behaves differently depending on the table type. Understanding this distinction is crucial for correctly specifying your data requirements.

### Pivot Tables (Narrow to Wide)

**Pivot tables** store data in narrow format with a category column and need to be pivoted into wide format:

| Table Name | Category Column | What to Specify |
|------------|----------------|-----------------|
| `vitals` | `vital_category` | Category values to pivot (e.g., 'heart_rate', 'sbp') |
| `labs` | `lab_category` | Category values to pivot (e.g., 'hemoglobin', 'sodium') |
| `medication_admin_continuous` | `med_category` | Medication names to pivot (e.g., 'norepinephrine', 'propofol') |
| `medication_admin_intermittent` | `med_category` | Medication names to pivot (e.g., 'norepinephrine', 'propofol') |
| `patient_assessments` | `assessment_category` | Assessment types to pivot (e.g., 'RASS', 'gcs_total') |

**Example - Pivot Tables:**
```python
category_filters = {
    'vitals': ['heart_rate', 'sbp', 'dbp', 'spo2'],      # These are CATEGORY VALUES
    'labs': ['hemoglobin', 'wbc', 'sodium', 'creatinine'], # These are CATEGORY VALUES
    'medication_admin_continuous': ['norepinephrine', 'propofol']  # These are CATEGORY VALUES
}
```

**How it works:**
- Input: Rows with different `vital_category` values (heart_rate, sbp, etc.)
- Output: Columns for each category (`heart_rate`, `sbp`, etc.)

### Wide Tables (Already Wide)

**Wide tables** are already in wide format with multiple data columns:

| Table Name | What to Specify |
|------------|-----------------|
| `respiratory_support` | Column names to keep from the table schema |

**Example - Wide Tables:**
```python
category_filters = {
    'respiratory_support': [                    # These are COLUMN NAMES
        'device_category',                      # Column name
        'fio2_set',                            # Column name
        'peep_set',                            # Column name
        'tidal_volume_set',                    # Column name
        'resp_rate_set'                        # Column name
    ]
}
```

**How it works:**
- Input: Table with many columns (device_category, fio2_set, peep_set, etc.)
- Output: Only specified columns are kept in the wide dataset

### Mixed Usage Example

You can specify both pivot and wide tables in the same `category_filters` dictionary:

```python
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs', 'respiratory_support'],
    category_filters={
        # PIVOT TABLES - specify category values
        'vitals': ['heart_rate', 'sbp', 'spo2'],
        'labs': ['hemoglobin', 'sodium', 'creatinine'],

        # WIDE TABLES - specify column names
        'respiratory_support': ['device_category', 'fio2_set', 'peep_set']
    }
)
```

### Finding Values in Your Data

To see what category values are actually present in your loaded data:

```python
# After loading tables
co.initialize(['vitals', 'labs', 'medication_admin_continuous', 'patient_assessments'])

# Check available categories
print("Available vitals:", co.vitals.df['vital_category'].unique())
print("Available labs:", co.labs.df['lab_category'].unique())
print("Available medications:", co.medication_admin_continuous.df['med_category'].unique())
print("Available assessments:", co.patient_assessments.df['assessment_category'].unique())

# Check respiratory support columns
print("Respiratory support columns:", co.respiratory_support.df.columns.tolist())
```

## Best Practices: System Resource Management

**Always check your system resources before running wide dataset creation on large datasets:**

```python
# Check system resources first
resources = co.get_sys_resource_info()
print(f"Available RAM: {resources['memory_available_gb']:.1f} GB")
print(f"Recommended threads: {resources['max_recommended_threads']}")

# Configure based on available resources
memory_limit = f"{int(resources['memory_available_gb'] * 0.7)}GB"  # Use 70% of available RAM
threads = resources['max_recommended_threads']

# Create wide dataset with optimized settings
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'spo2'],
        'labs': ['hemoglobin', 'sodium']
    },
    memory_limit=memory_limit,
    threads=threads,
    batch_size=1000  # Adjust based on dataset size
)

# Access the created dataset
wide_df = co.wide_df
```

## Usage Examples

### Example 1: Development and Testing

Use sampling for initial development and testing:

```python
# Sample mode for development
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        # PIVOT TABLES - specify category values from vital_category and lab_category columns
        'vitals': ['heart_rate', 'sbp', 'dbp', 'spo2', 'respiratory_rate'],
        'labs': ['hemoglobin', 'wbc', 'sodium', 'potassium', 'creatinine']
    },
    sample=True,  # Only 20 random hospitalizations
    show_progress=True
)

# Access the created dataset
wide_df = co.wide_df
print(f"Sample dataset shape: {wide_df.shape}")
print(f"Unique hospitalizations: {wide_df['hospitalization_id'].nunique()}")
```

### Example 2: Production Use with Resource Optimization

For production use with large datasets:

```python
# Get system info and configure accordingly
resources = co.get_sys_resource_info(print_summary=False)

# Production settings
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs', 'medication_admin_continuous'],
    category_filters={
        # PIVOT TABLES - specify category values
        'vitals': ['heart_rate', 'sbp', 'dbp', 'spo2', 'temp_c'],
        'labs': ['hemoglobin', 'wbc', 'sodium', 'potassium'],
        'medication_admin_continuous': ['norepinephrine', 'propofol', 'fentanyl']  # med_category values
    },
    batch_size=500,  # Smaller batches for large datasets
    memory_limit="12GB",
    threads=resources['max_recommended_threads'],
    save_to_data_location=True,
    output_format='parquet',
    output_filename='wide_dataset_production'
)

# Access the created dataset
wide_df = co.wide_df
```

### Example 3: Targeted Analysis with Specific IDs

Process specific hospitalizations:

```python
# Analyze specific patient cohort
target_ids = ['12345', '67890', '11111', '22222']

co.create_wide_dataset(
    hospitalization_ids=target_ids,
    tables_to_load=['vitals', 'labs', 'patient_assessments'],
    category_filters={
        # PIVOT TABLES - specify category values
        'vitals': ['heart_rate', 'sbp', 'spo2'],  # vital_category values
        'labs': ['lactate', 'hemoglobin'],  # lab_category values
        'patient_assessments': ['gcs_total', 'rass']  # assessment_category values (note: case-sensitive!)
    }
)

# Access the created dataset
wide_df = co.wide_df
print(f"Analyzed {len(target_ids)} specific hospitalizations")
```

### Example 4: Time Window Filtering with Cohort DataFrame

Filter data to specific time windows:

```python
import pandas as pd

# Define cohort with time windows
cohort_df = pd.DataFrame({
    'hospitalization_id': ['12345', '67890', '11111'],
    'start_time': ['2023-01-01 08:00:00', '2023-01-05 12:00:00', '2023-01-10 06:00:00'],
    'end_time': ['2023-01-03 18:00:00', '2023-01-07 20:00:00', '2023-01-12 14:00:00']
})

# Convert to datetime
cohort_df['start_time'] = pd.to_datetime(cohort_df['start_time'])
cohort_df['end_time'] = pd.to_datetime(cohort_df['end_time'])

# Create wide dataset with time filtering
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        # PIVOT TABLES - specify category values
        'vitals': ['heart_rate', 'sbp'],  # vital_category values
        'labs': ['hemoglobin', 'sodium']  # lab_category values
    },
    cohort_df=cohort_df  # Only include data within specified time windows
)

# Access the created dataset
wide_df = co.wide_df
```

### Example 5: Mixed Pivot and Wide Tables

Use both pivot and wide tables together:

```python
# Mixed example with both table types
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs', 'respiratory_support'],
    category_filters={
        # PIVOT TABLES - specify category values to pivot
        'vitals': ['heart_rate', 'sbp', 'map'],
        'labs': ['hemoglobin', 'creatinine'],

        # WIDE TABLES - specify column names to keep
        'respiratory_support': [
            'device_category',      # Column name from respiratory_support table
            'mode_category',        # Column name
            'fio2_set',            # Column name
            'peep_set',            # Column name
            'tidal_volume_set'     # Column name
        ]
    }
)

# Access the created dataset
wide_df = co.wide_df
print(f"Dataset includes both pivoted categories and wide table columns")
```

### Example 6: No Batch Processing for Small Datasets

Disable batching for small datasets:

```python
# Small dataset - process all at once
co.create_wide_dataset(
    hospitalization_ids=small_id_list,  # < 100 hospitalizations
    tables_to_load=['vitals', 'labs'],
    category_filters={
        # PIVOT TABLES - specify category values
        'vitals': ['heart_rate', 'sbp'],
        'labs': ['hemoglobin']
    },
    batch_size=-1  # Disable batching
)

# Access the created dataset
wide_df = co.wide_df
```


## Memory Management and Batch Processing

### Understanding Batch Processing

Batch processing divides hospitalizations into smaller groups to prevent memory (larger-than-memory, OOM) issues:

```python
# Large dataset - use batching
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={...},
    batch_size=1000,  # Process 1000 hospitalizations at a time
    memory_limit="8GB"
)
wide_df = co.wide_df

# Small dataset - disable batching for better performance
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={...},
    batch_size=-1  # Process all at once
)
wide_df = co.wide_df
```

### Memory Optimization Guidelines

| Dataset Size | Batch Size | Memory Limit | Threads |
|-------------|------------|--------------|---------|
| < 1,000 hospitalizations | -1 (no batching) | 4GB | 2-4 |
| 1,000 - 10,000 hospitalizations | 1000 | 8GB | 4-8 |
| > 10,000 hospitalizations | 500 | 16GB+ | 6-12 |

## Hourly Aggregation

The hourly aggregation feature transforms your wide time-series data into hourly buckets with configurable aggregation methods. This is essential for creating consistent time-series features for machine learning, calculating clinical scores (like SOFA), and analyzing temporal patterns.

### Understanding Hourly Aggregation

Hourly aggregation:
- Groups events into hourly time buckets based on `event_time`
- Applies specified aggregation methods (mean, max, min, etc.) to each column
- Creates consistent time series with one row per hour per ID
- Calculates `nth_hour` representing hours since the first event
- Supports flexible grouping by different ID columns (hospitalization or encounter)

### Function Parameters

The `convert_wide_to_hourly()` method accepts:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `aggregation_config` | Dict[str, List[str]] | Required | Maps aggregation methods to column lists |
| `wide_df` | DataFrame | None | Input wide dataset (uses stored `co.wide_df` if None) |
| `id_name` | str | 'hospitalization_id' | Column for grouping: 'hospitalization_id', 'encounter_block', or custom |
| `memory_limit` | str | '4GB' | DuckDB memory limit (e.g., '8GB', '16GB') |
| `temp_directory` | str | None | Directory for temporary files |
| `batch_size` | int | Auto | Batch size for processing large datasets |

### Aggregation Methods and Column Suffixes

Each column gets a suffix based on its aggregation method:

| Method | Suffix | Description | Use Case |
|--------|--------|-------------|----------|
| `max` | `_max` | Maximum value within hour | Worst vital signs, peak lab values |
| `min` | `_min` | Minimum value within hour | Lowest counts, minimum scores |
| `mean` | `_mean` | Average value within hour | Typical vital signs, average doses |
| `median` | `_median` | Median value within hour | Robust central tendency |
| `first` | `_first` | First value chronologically | Initial assessments, admission values |
| `last` | `_last` | Last value chronologically | Most recent status, discharge values |
| `boolean` | `_boolean` | 1 if any value present, 0 if absent | Medication administration, interventions |
| (auto) | `_c` | Carry-forward for non-aggregated columns | Demographics, static values |

**Note**: Columns not explicitly specified in `aggregation_config` are automatically assigned to 'first' aggregation with `_c` suffix to distinguish them from explicitly requested aggregations.

### Special Columns (No Suffix)

These columns maintain their original names without suffixes:
- **`patient_id`**: Patient identifier
- **`day_number`**: Sequential day within stay
- **`nth_hour`**: Hours since first event (0, 1, 2, ...) - key temporal column
- **`event_time_hour`**: Timestamp of the hour bucket
- **`hour_bucket`**: Hour of day (0-23)
- **ID column**: The grouping column (varies based on `id_name` parameter)

### Basic Usage

```python
# Create aggregation configuration
aggregation_config = {
    'max': ['sbp', 'map', 'creatinine'],
    'mean': ['heart_rate', 'respiratory_rate'],
    'min': ['spo2', 'platelet_count'],
    'median': ['temp_c'],
    'first': ['gcs_total', 'rass'],
    'last': ['assessment_value'],
    'boolean': ['norepinephrine', 'propofol'],  # 1 if present, 0 if absent
}

# Convert to hourly (automatically uses stored wide_df)
hourly_df = co.convert_wide_to_hourly(
    aggregation_config=aggregation_config,
    memory_limit='8GB'
)

print(f"Hourly dataset: {hourly_df.shape}")
print(f"Hour range: {hourly_df['nth_hour'].min()} to {hourly_df['nth_hour'].max()}")
print(f"Columns created: {list(hourly_df.columns)}")
```

### Using Encounter Blocks for Aggregation

When encounter stitching is enabled, you can aggregate by `encounter_block` to treat linked hospitalizations as continuous stays:

```python
# Enable encounter stitching during initialization
co = ClifOrchestrator(
    data_directory='/path/to/data',
    stitch_encounter=True,
    stitch_time_interval=6  # Link hospitalizations within 6 hours
)

# Create wide dataset (encounter_block column automatically added)
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp'],
        'labs': ['hemoglobin', 'creatinine']
    }
)

# Aggregate by encounter blocks instead of individual hospitalizations
hourly_df_encounter = co.convert_wide_to_hourly(
    aggregation_config=aggregation_config,
    id_name='encounter_block'  # Group by encounter blocks
)

# Compare with hospitalization-level aggregation
hourly_df_hosp = co.convert_wide_to_hourly(
    aggregation_config=aggregation_config,
    id_name='hospitalization_id'  # Default behavior
)

print(f"Encounter blocks: {hourly_df_encounter['encounter_block'].nunique()}")
print(f"Hospitalizations: {hourly_df_hosp['hospitalization_id'].nunique()}")
```

### Example: Comparing Aggregation Strategies

```python
# Define consistent aggregation config
config = {
    'mean': ['heart_rate', 'map', 'spo2'],
    'max': ['creatinine', 'bilirubin'],
    'boolean': ['norepinephrine', 'vasopressin']
}

# Method 1: Aggregate by hospitalization (default)
hourly_by_hosp = co.convert_wide_to_hourly(
    aggregation_config=config,
    id_name='hospitalization_id'
)

# Method 2: Aggregate by encounter block (for linked stays)
hourly_by_encounter = co.convert_wide_to_hourly(
    aggregation_config=config,
    id_name='encounter_block'
)

# Analysis
print("=== Aggregation Comparison ===")
print(f"By hospitalization: {len(hourly_by_hosp)} hourly records")
print(f"By encounter block: {len(hourly_by_encounter)} hourly records")

# Encounter blocks create longer continuous stays
max_hours_hosp = hourly_by_hosp.groupby('hospitalization_id')['nth_hour'].max()
max_hours_enc = hourly_by_encounter.groupby('encounter_block')['nth_hour'].max()
print(f"Max stay duration (hospitalizations): {max_hours_hosp.max()} hours")
print(f"Max stay duration (encounters): {max_hours_enc.max()} hours")
```

### Output Structure

The hourly dataset contains:
1. **ID column** (based on `id_name` parameter)
2. **Temporal columns**: `nth_hour`, `event_time_hour`, `hour_bucket`, `day_number`
3. **Patient columns**: `patient_id`
4. **Aggregated columns** with appropriate suffixes
5. **Carry-forward columns** with `_c` suffix

Example output structure:
```
hospitalization_id | nth_hour | heart_rate_mean | sbp_max | norepinephrine_boolean | age_at_admission_c
123456            | 0        | 75.5           | 130     | 0                     | 65.0
123456            | 1        | 82.3           | 135     | 1                     | 65.0
123456            | 2        | 79.0           | 128     | 1                     | 65.0
```

### Memory Optimization for Large Datasets

```python
# For very large datasets, use batching
hourly_df = co.convert_wide_to_hourly(
    aggregation_config=aggregation_config,
    batch_size=5000,  # Process 5000 IDs at a time
    memory_limit='16GB',
    temp_directory='/path/to/fast/disk'  # SSD recommended
)
```

### Common Patterns and Tips

1. **Vital Signs**: Use `mean` for typical values, `max`/`min` for extremes
2. **Medications**: Use `boolean` for presence/absence or `mean` for average doses
3. **Lab Values**: Use `max` for values you want to catch peaks (creatinine, bilirubin)
4. **Assessments**: Use `first` or `last` depending on clinical relevance
5. **Demographics**: Automatically get `_c` suffix (carry-forward)

## Output Structure

The wide dataset includes:

### Core Columns
- `patient_id`: Patient identifier
- `hospitalization_id`: Hospitalization identifier  
- `event_time`: Timestamp for each event
- `day_number`: Sequential day within hospitalization
- `hosp_id_day_key`: Unique hospitalization-daily identifier

### Patient Demographics
- `age_at_admission`: Patient age
- Additional patient table columns

### ADT Information
- Location and transfer data from ADT table

### Pivoted Data Columns
- Individual columns for each category (e.g., `heart_rate`, `hemoglobin`, `norepinephrine`) as per use provided in `category_filters`
- Values aligned by timestamp and hospitalization


## Troubleshooting

### Common Issues and Solutions

**Memory Errors**
```python
# Solution: Reduce batch size and set memory limit
co.create_wide_dataset(
    batch_size=250,  # Smaller batches
    memory_limit="4GB",  # Conservative limit
    sample=True  # Test with sample first
)
wide_df = co.wide_df
```

**System Crashes**
```python
# Solution: Check resources first and configure accordingly
resources = co.get_sys_resource_info()
if resources['memory_available_gb'] < 8:
    print("Warning: Low memory available. Consider using smaller batch_size.")
    batch_size = 250
else:
    batch_size = 1000
```

**Empty Results**
```python
# Check if tables and categories exist
print("Available tables:", co.get_loaded_tables())
if hasattr(co, 'vitals') and co.vitals is not None:
    print("Vital categories:", co.vitals.df['vital_category'].unique())
```

**Slow Performance**
```python
# Use optimize settings
co.create_wide_dataset(
    tables_to_load=['vitals'],  # Start with one table
    category_filters={
        'vitals': ['heart_rate', 'sbp']  # Limit categories
    },
    threads=co.get_sys_resource_info(print_summary=False)['max_recommended_threads']
)
wide_df = co.wide_df
```

### Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| "Memory limit exceeded" | Dataset too large for available RAM | Reduce batch_size, set memory_limit |
| "No event times found" | No data in specified tables/categories | Check table data and category filters |
| "Missing required columns" | cohort_df missing required columns | Ensure 'hospitalization_id', 'start_time', 'end_time' columns exist |

## Integration with Existing Workflows

Wide dataset creation integrates seamlessly with existing ClifOrchestrator workflows:

```python
# Traditional approach
co = ClifOrchestrator('/path/to/data', 'parquet', 'UTC')
co.initialize(['patient', 'hospitalization', 'adt', 'vitals', 'labs'])

# Enhanced with wide dataset
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp'],
        'labs': ['hemoglobin']
    }
)

# Continue with validation and analysis
co.validate_all()
analysis_results = your_analysis_function(co.wide_df)
```

## Next Steps

- Learn about [data validation](validation.md) to ensure data quality
- Explore [individual table guides](tables.md) for detailed table documentation
- See the [orchestrator guide](orchestrator.md) for advanced orchestrator features
- Review [timezone handling](timezones.md) for multi-site data