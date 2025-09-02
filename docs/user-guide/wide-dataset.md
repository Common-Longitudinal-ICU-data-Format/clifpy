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
wide_df = co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'spo2'],
        'labs': ['hemoglobin', 'sodium', 'glucose']
    },
    sample=True  # Use 20 random hospitalizations for testing
)
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
| `return_dataframe` | bool | True | Return DataFrame even when saving to file |
| `batch_size` | int | 1000 | Number of hospitalizations per batch (use -1 to disable batching) |
| `memory_limit` | str | None | DuckDB memory limit (e.g., '8GB', '16GB') |
| `threads` | int | None | Number of threads for DuckDB processing |
| `show_progress` | bool | True | Show progress bars for long operations |

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
wide_df = co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'spo2'],
        'labs': ['hemoglobin', 'sodium']
    },
    memory_limit=memory_limit,
    threads=threads,
    batch_size=1000  # Adjust based on dataset size
)
```

## Usage Examples

### Example 1: Development and Testing

Use sampling for initial development and testing:

```python
# Sample mode for development
wide_df = co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'dbp', 'spo2', 'respiratory_rate'],
        'labs': ['hemoglobin', 'wbc', 'sodium', 'potassium', 'creatinine']
    },
    sample=True,  # Only 20 random hospitalizations
    show_progress=True
)

print(f"Sample dataset shape: {wide_df.shape}")
print(f"Unique hospitalizations: {wide_df['hospitalization_id'].nunique()}")
```

### Example 2: Production Use with Resource Optimization

For production use with large datasets:

```python
# Get system info and configure accordingly
resources = co.get_sys_resource_info(print_summary=False)

# Production settings
wide_df = co.create_wide_dataset(
    tables_to_load=['vitals', 'labs', 'medication_admin_continuous'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'dbp', 'spo2', 'temp_c'],
        'labs': ['hemoglobin', 'wbc', 'sodium', 'potassium'],
        'medication_admin_continuous': ['norepinephrine', 'propofol', 'fentanyl']
    },
    batch_size=500,  # Smaller batches for large datasets
    memory_limit="12GB",
    threads=resources['max_recommended_threads'],
    save_to_data_location=True,
    output_format='parquet',
    output_filename='wide_dataset_production'
)
```

### Example 3: Targeted Analysis with Specific IDs

Process specific hospitalizations:

```python
# Analyze specific patient cohort
target_ids = ['12345', '67890', '11111', '22222']

wide_df = co.create_wide_dataset(
    hospitalization_ids=target_ids,
    tables_to_load=['vitals', 'labs', 'patient_assessments'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'spo2'],
        'labs': ['lactate', 'hemoglobin'],
        'patient_assessments': ['gcs_total', 'rass']
    }
)

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
wide_df = co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp'],
        'labs': ['hemoglobin', 'sodium']
    },
    cohort_df=cohort_df  # Only include data within specified time windows
)
```

### Example 5: No Batch Processing for Small Datasets

Disable batching for small datasets:

```python
# Small dataset - process all at once
wide_df = co.create_wide_dataset(
    hospitalization_ids=small_id_list,  # < 100 hospitalizations
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp'],
        'labs': ['hemoglobin']
    },
    batch_size=-1  # Disable batching
)
```


## Memory Management and Batch Processing

### Understanding Batch Processing

Batch processing divides hospitalizations into smaller groups to prevent memory (larger-than-memory, OOM) issues:

```python
# Large dataset - use batching
wide_df = co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={...},
    batch_size=1000,  # Process 1000 hospitalizations at a time
    memory_limit="8GB"
)

# Small dataset - disable batching for better performance
wide_df = co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={...},
    batch_size=-1  # Process all at once
)
```

### Memory Optimization Guidelines

| Dataset Size | Batch Size | Memory Limit | Threads |
|-------------|------------|--------------|---------|
| < 1,000 hospitalizations | -1 (no batching) | 4GB | 2-4 |
| 1,000 - 10,000 hospitalizations | 1000 | 8GB | 4-8 |
| > 10,000 hospitalizations | 500 | 16GB+ | 6-12 |

## Hourly Aggregation

Convert the wide dataset to hourly aggregation:

```python
# Create aggregation configuration
aggregation_config = {
    'max': ['sbp', 'map'],
    'mean': ['heart_rate', 'respiratory_rate'],
    'min': ['spo2'],
    'median': ['temp_c'],
    'first': ['gcs_total', 'rass'],
    'last': ['assessment_value'],
    'boolean': ['norepinephrine', 'propofol'],  # 1 if present, 0 if absent
}

# Convert to hourly
hourly_df = co.convert_wide_to_hourly(
    wide_df,
    aggregation_config=aggregation_config,
    memory_limit='8GB'
)

print(f"Hourly dataset: {hourly_df.shape}")
print(f"Hour range: {hourly_df['nth_hour'].min()} to {hourly_df['nth_hour'].max()}")
```

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
wide_df = co.create_wide_dataset(
    batch_size=250,  # Smaller batches
    memory_limit="4GB",  # Conservative limit
    sample=True  # Test with sample first
)
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
wide_df = co.create_wide_dataset(
    tables_to_load=['vitals'],  # Start with one table
    category_filters={
        'vitals': ['heart_rate', 'sbp']  # Limit categories
    },
    threads=co.get_sys_resource_info(print_summary=False)['max_recommended_threads']
)
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
wide_df = co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp'],
        'labs': ['hemoglobin']
    }
)

# Continue with validation and analysis
co.validate_all()
analysis_results = your_analysis_function(wide_df)
```

## Next Steps

- Learn about [data validation](validation.md) to ensure data quality
- Explore [individual table guides](tables/index.md) for detailed table documentation
- See the [orchestrator guide](orchestrator.md) for advanced orchestrator features
- Review [timezone handling](timezones.md) for multi-site data