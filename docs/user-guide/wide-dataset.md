# Wide Dataset Creation

Transform your CLIF data into analysis-ready datasets with automatic table joining, pivoting, and temporal aggregation.

## What You Can Do

With wide dataset functionality, you can:

-   **Create event-level datasets** - Join vitals, labs, medications, and assessments into a single timeline
-   **Aggregate to time windows** - Convert irregular measurements into hourly, daily, or custom time buckets
-   **Prepare for machine learning** - Generate consistent time-series features with configurable aggregation methods

**Key benefit**: Turn complex, multi-table ICU data into analysis-ready DataFrames in just a few lines of code.

## Quick Start

``` python
from clifpy.clif_orchestrator import ClifOrchestrator

# Initialize
co = ClifOrchestrator(
    data_directory='/path/to/clif/data',
    filetype='parquet',
    timezone='US/Central'
)

# Create wide dataset
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'spo2'],
        'labs': ['hemoglobin', 'sodium', 'creatinine']
    },
    sample=True  # Start with 20 random patients
)

# Access the result
wide_df = co.wide_df
print(f"Created dataset: {wide_df.shape}")
```

That's it! You now have a wide dataset with vitals and labs joined by patient and timestamp.

## Understanding the Two-Step Process

Wide dataset creation is a **two-step process**. You can use one or both steps depending on your needs:

### Step 1: Create Wide Dataset (Event-Level Data)

This joins multiple tables into a single DataFrame with **one row per measurement event**.

``` python
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp'],
        'labs': ['hemoglobin']
    }
)
# Result stored in: co.wide_df
```

**Output**: Every measurement is a separate row - Patient has 10 heart rate measurements in hour 1 → 10 rows - Each row has timestamp, patient info, and measurement values

**When to use**: - Need every individual measurement - Analyzing measurement frequency or timing - Creating custom aggregations

### Step 2: Aggregate to Time Windows (Optional)

Convert irregular measurements into consistent time windows with aggregation.

``` python
hourly_df = co.convert_wide_to_hourly(
    aggregation_config={
        'mean': ['heart_rate', 'sbp'],
        'median': ['hemoglobin']
    },
    hourly_window=1  # 1-hour windows
)
```

**Output**: One row per time window - Patient has 10 heart rate measurements in hour 1 → 1 row with average - Consistent time intervals (hourly, 6-hour, daily, etc.)

**When to use**: - Training machine learning models - Calculating clinical scores (SOFA, APACHE) - Analyzing trends over time - Need consistent time intervals

## Which Approach Do I Need?

| Your Goal | Use This |
|--------------------------------------|----------------------------------|
| Explore all individual measurements | **Wide dataset only** (skip aggregation) |
| Train ML models (LSTM, XGBoost, etc.) | **Wide dataset + hourly aggregation** |
| Calculate SOFA or other clinical scores | **Wide dataset + hourly aggregation** |
| Analyze daily trends | **Wide dataset + daily aggregation** (24-hour windows) |
| Custom analysis | **Wide dataset** (then aggregate yourself) |

## Choosing Your Data

### What is category_filters?

`category_filters` tells the function **which measurements you want** from each table.

**Simple rule**: List the measurement names you care about.

``` python
category_filters = {
    'vitals': ['heart_rate', 'sbp', 'dbp', 'spo2', 'map'],
    'labs': ['hemoglobin', 'wbc', 'sodium', 'potassium', 'creatinine'],
    'medication_admin_continuous': ['norepinephrine', 'propofol', 'fentanyl']
}
```

### How to Find Available Measurements

Not sure what's in your data? Check before creating the wide dataset:

``` python
# Load tables first
co.initialize(['vitals', 'labs', 'medication_admin_continuous'])

# See what's available
print("Vitals:", co.vitals.df['vital_category'].unique())
print("Labs:", co.labs.df['lab_category'].unique())
print("Medications:", co.medication_admin_continuous.df['med_category'].unique())

# Then create wide dataset with what you found
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp'],  # Pick from the list above
        'labs': ['hemoglobin', 'sodium']
    }
)
```

### Special Case: Respiratory Support & Other wide tables

`respiratory_support` is already in wide format, so you specify **column names** instead of categories:

``` python
category_filters = {
    'vitals': ['heart_rate', 'spo2'],  # Category values (pivoted)
    'respiratory_support': [            # Column names (already wide)
        'device_category',
        'fio2_set',
        'peep_set',
        'tidal_volume_set'
    ]
}
```

**How to find respiratory support columns**:

``` python
co.initialize(['respiratory_support'])
print("Available columns:", co.respiratory_support.df.columns.tolist())
```

## Common Workflows

### Workflow 1: Explore Vital Signs for Sample Patients

**Goal**: Understand vital sign patterns in a small sample

``` python
co.create_wide_dataset(
    tables_to_load=['vitals'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'dbp', 'map', 'spo2', 'temp_c']
    },
    sample=True  # Just 20 random patients
)

# Explore the data
wide_df = co.wide_df
print(f"Patients: {wide_df['patient_id'].nunique()}")
print(f"Events: {len(wide_df)}")
print(f"Date range: {wide_df['event_time'].min()} to {wide_df['event_time'].max()}")

# Example analysis: measurements per patient
measurements_per_patient = wide_df.groupby('patient_id').size()
print(f"Average measurements per patient: {measurements_per_patient.mean():.0f}")
```

### Workflow 2: Prepare Hourly Data for Machine Learning

**Goal**: Create hourly aggregated features for predictive modeling

``` python
# Step 1: Create wide dataset
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs', 'medication_admin_continuous'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'map', 'spo2', 'temp_c'],
        'labs': ['hemoglobin', 'wbc', 'platelet', 'creatinine', 'bilirubin'],
        'medication_admin_continuous': ['norepinephrine', 'vasopressin', 'propofol']
    }
)

# Step 2: Aggregate to hourly windows
hourly_df = co.convert_wide_to_hourly(
    aggregation_config={
        # Vital signs: mean for typical, max/min for extremes
        'mean': ['heart_rate', 'sbp', 'map', 'temp_c'],
        'max': ['heart_rate', 'sbp'],
        'min': ['spo2', 'map'],

        # Labs: max for values where peaks matter
        'max': ['creatinine', 'bilirubin'],
        'median': ['hemoglobin', 'wbc', 'platelet'],

        # Medications: boolean for presence
        'boolean': ['norepinephrine', 'vasopressin', 'propofol']
    },
    hourly_window=1,
    fill_gaps=True  # Create complete time series for ML
)

# Now ready for sklearn, PyTorch, etc.
print(f"ML-ready dataset: {hourly_df.shape}")
print(f"Features: {[c for c in hourly_df.columns if any(s in c for s in ['_mean', '_max', '_min', '_boolean'])]}")
```

### Workflow 3: Calculate SOFA Scores

**Goal**: Compute hourly SOFA scores for patients

``` python
# Create wide dataset with SOFA-required variables
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs', 'medication_admin_continuous', 'respiratory_support'],
    category_filters={
        'vitals': ['sbp', 'map'],
        'labs': ['platelet', 'bilirubin', 'creatinine'],
        'medication_admin_continuous': ['norepinephrine', 'dopamine', 'epinephrine'],
        'respiratory_support': ['fio2_set', 'pao2']
    }
)

# Aggregate to hourly windows (SOFA typically calculated hourly)
hourly_df = co.convert_wide_to_hourly(
    aggregation_config={
        'min': ['sbp', 'map', 'platelet'],  # Worst values
        'max': ['bilirubin', 'creatinine'],  # Worst values
        'max': ['norepinephrine', 'dopamine', 'epinephrine'],  # Maximum doses
        'mean': ['fio2_set', 'pao2']
    },
    hourly_window=1
)

# Now calculate SOFA scores using hourly aggregated data
# (Use co.compute_sofa_scores() or custom SOFA calculation)
```

### Workflow 4: Analyze Daily Summaries for Outcomes

**Goal**: Daily statistics for outcome analysis

``` python
# Create wide dataset
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'temp_c'],
        'labs': ['hemoglobin', 'wbc', 'creatinine']
    }
)

# Aggregate to DAILY windows (24 hours)
daily_df = co.convert_wide_to_hourly(
    aggregation_config={
        'mean': ['heart_rate', 'sbp', 'temp_c'],
        'max': ['heart_rate', 'temp_c'],
        'min': ['sbp'],
        'median': ['hemoglobin', 'wbc', 'creatinine'],
        'first': ['hemoglobin'],  # Admission value
        'last': ['creatinine']     # Discharge value
    },
    hourly_window=24  # 24-hour windows = daily
)

print(f"Daily summaries: {len(daily_df)} patient-days")
print(f"Average days per patient: {daily_df.groupby('hospitalization_id').size().mean():.1f}")
```

## Temporal Aggregation Deep Dive

### Understanding Aggregation Methods

When you convert to time windows, you specify **how to aggregate** multiple measurements into one value:

| Method | Use For | Example |
|----------------------|-------------------------|-------------------------|
| `mean` | Typical vital signs, average doses | Mean heart rate over the hour |
| `max` | Worst case, peaks | Maximum creatinine (worst kidney function) |
| `min` | Best case, lows | Minimum SpO2 (worst oxygenation) |
| `median` | Robust central tendency | Median hemoglobin (less affected by outliers) |
| `first` | Initial/admission values | First GCS score of the day |
| `last` | Final/discharge values | Last assessment before discharge |
| `boolean` | Presence/absence | Was norepinephrine given? (1=yes, 0=no) |
| `one_hot_encode` | Categorical variables | Convert device types to binary columns |

### Choosing Window Sizes

| Window Size | Use Case | Output Density |
|------------------------|-------------------|-----------------------------|
| **1 hour** | High-resolution analysis, ML models | \~24 rows per day per patient |
| **6 hours** | Shift-based analysis (morning/afternoon/evening/night) | \~4 rows per day per patient |
| **12 hours** | Bi-daily patterns | \~2 rows per day per patient |
| **24 hours** (daily) | Daily summaries, outcome studies | \~1 row per day per patient |

``` python
# Hourly (high resolution)
hourly = co.convert_wide_to_hourly(aggregation_config=config, hourly_window=1)

# 6-hour windows (shift-based)
shift_based = co.convert_wide_to_hourly(aggregation_config=config, hourly_window=6)

# Daily summaries
daily = co.convert_wide_to_hourly(aggregation_config=config, hourly_window=24)
```

### Gap Filling for Machine Learning

By default, only windows **with data** are created (sparse output). For ML models that need complete time series:

``` python
# Dense output - ALL time windows created, gaps filled with NaN
ml_ready = co.convert_wide_to_hourly(
    aggregation_config={
        'mean': ['heart_rate', 'sbp'],
        'boolean': ['norepinephrine']
    },
    hourly_window=1,
    fill_gaps=True  # Creates rows for ALL hours, even without data
)

# Now every patient has hours 0, 1, 2, 3, ..., N
# Missing data appears as NaN (ready for imputation)
```

**When to use `fill_gaps`**: - ✅ Training LSTM, RNN, or time-series models - ✅ Need regular time intervals - ✅ Will apply imputation strategies

**When NOT to use `fill_gaps`**: - ❌ Descriptive statistics (don't need empty hours) - ❌ Memory-constrained environments (denser data = more rows) - ❌ Will handle missing windows yourself

### Using Encounter Blocks

When encounter stitching is enabled, you can group by `encounter_block` instead of individual hospitalizations:

``` python
# Enable encounter stitching
co = ClifOrchestrator(
    data_directory='/path/to/data',
    stitch_encounter=True,
    stitch_time_interval=6  # Link hospitalizations within 6 hours
)

# Create wide dataset
co.create_wide_dataset(tables_to_load=['vitals'], category_filters={'vitals': ['heart_rate']})

# Aggregate by ENCOUNTER BLOCK (treats linked hospitalizations as one continuous stay)
hourly_encounter = co.convert_wide_to_hourly(
    aggregation_config={'mean': ['heart_rate']},
    id_name='encounter_block'  # Group by encounter, not hospitalization
)

# Compare: encounter blocks vs individual hospitalizations
print(f"Encounter blocks: {hourly_encounter['encounter_block'].nunique()}")
print(f"Max hours in encounter: {hourly_encounter.groupby('encounter_block')['window_number'].max().max()}")
```

## Performance & Optimization

### When to Optimize

For most use cases (\< 1,000 hospitalizations, \< 1M rows), **default settings work fine**. Only optimize if: - Processing \> 10,000 hospitalizations - Experiencing memory errors - Processing takes \> 10 minutes

### Check System Resources First

``` python
# Always start here for large datasets
resources = co.get_sys_resource_info()
print(f"Available RAM: {resources['memory_available_gb']:.1f} GB")
print(f"Recommended threads: {resources['max_recommended_threads']}")
```

### Optimization Strategies

**1. Use Sampling for Development**

``` python
# Test with sample first
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={...},
    sample=True  # 20 random patients
)
```

**2. Adjust Batch Size for Large Datasets**

``` python
# For > 10,000 hospitalizations
co.create_wide_dataset(
    tables_to_load=['vitals'],
    category_filters={'vitals': ['heart_rate', 'sbp']},
    batch_size=500,  # Smaller batches = less memory per batch
    memory_limit='8GB'
)
```

**3. Configure Memory and Threads**

``` python
# Based on system resources
resources = co.get_sys_resource_info(print_summary=False)

co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={...},
    memory_limit=f"{int(resources['memory_available_gb'] * 0.7)}GB",  # Use 70% of available
    threads=resources['max_recommended_threads']
)
```

**4. Save Large Datasets to Disk**

``` python
# Don't keep huge DataFrames in memory
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={...},
    save_to_data_location=True,
    output_format='parquet',  # Compressed format
    output_filename='my_wide_dataset',
    return_dataframe=False  # Don't keep in memory if not needed
)
# Later, load as needed
import pandas as pd
wide_df = pd.read_parquet('/path/to/data/my_wide_dataset.parquet')
```

### Performance Guidelines

| Hospitalizations | Batch Size       | Memory Limit | Expected Time |
|------------------|------------------|--------------|---------------|
| \< 1,000         | -1 (no batching) | 4GB          | \< 1 minute   |
| 1,000 - 10,000   | 1000             | 8GB          | 2-10 minutes  |
| \> 10,000        | 500              | 16GB+        | 10-60 minutes |

## Quick Reference

### create_wide_dataset() Parameters

| Parameter | Type | Default | Description |
|------------------|----------------|----------------|----------------------|
| `tables_to_load` | List\[str\] | None | Tables to include: 'vitals', 'labs', 'medication_admin_continuous', 'patient_assessments', 'respiratory_support', 'crrt_therapy' |
| `category_filters` | Dict | None | Measurements to include from each table |
| `sample` | bool | False | Use 20 random patients for testing |
| `hospitalization_ids` | List\[str\] | None | Specific patient IDs to process |
| `cohort_df` | DataFrame | None | Time windows (columns: 'hospitalization_id', 'start_time', 'end_time') |
| `batch_size` | int | 1000 | Hospitalizations per batch (-1 = no batching) |
| `memory_limit` | str | None | DuckDB memory limit (e.g., '8GB') |
| `threads` | int | None | Processing threads (None = auto) |
| `output_format` | str | 'dataframe' | 'dataframe', 'csv', or 'parquet' |
| `save_to_data_location` | bool | False | Save to file |

**Access result**: `co.wide_df`

### convert_wide_to_hourly() Parameters

| Parameter | Type | Default | Description |
|------------------|----------------|----------------|----------------------|
| `aggregation_config` | Dict | Required | Maps methods to columns: `{'mean': ['hr'], 'max': ['sbp']}` |
| `wide_df` | DataFrame | None | Input data (uses `co.wide_df` if None) |
| `id_name` | str | 'hospitalization_id' | Grouping column: 'hospitalization_id' or 'encounter_block' |
| `hourly_window` | int | 1 | Window size in hours (1-72) |
| `fill_gaps` | bool | False | Create rows for all windows (True = dense, False = sparse) |
| `memory_limit` | str | '4GB' | DuckDB memory limit |
| `batch_size` | int | Auto | Batch size (auto-determined if None) |

**Available aggregation methods**: `max`, `min`, `mean`, `median`, `first`, `last`, `boolean`, `one_hot_encode`

## Troubleshooting

### Memory Errors

**Symptom**: "Memory limit exceeded" or system crash

**Solutions**:

``` python
# 1. Check available memory first
resources = co.get_sys_resource_info()

# 2. Reduce batch size
co.create_wide_dataset(
    batch_size=250,  # Smaller batches
    memory_limit='4GB'
)

# 3. Start with sample
co.create_wide_dataset(sample=True)  # Test with 20 patients first

# 4. Save to disk instead of memory
co.create_wide_dataset(
    save_to_data_location=True,
    output_format='parquet',
    return_dataframe=False
)
```

### Empty or No Results

**Symptom**: `co.wide_df` is None or has no rows

**Solutions**:

``` python
# 1. Check if tables are loaded
print("Loaded tables:", co.get_loaded_tables())

# 2. Check if data exists
co.initialize(['vitals'])
print("Vitals data:", len(co.vitals.df))
print("Available categories:", co.vitals.df['vital_category'].unique())

# 3. Verify category names match exactly (case-sensitive!)
co.create_wide_dataset(
    tables_to_load=['vitals'],
    category_filters={
        'vitals': ['heart_rate']  # Not 'Heart_Rate' or 'heartrate'
    }
)
```

### Slow Performance

**Symptom**: Takes very long to process

**Solutions**:

``` python
# 1. Use fewer tables/categories
co.create_wide_dataset(
    tables_to_load=['vitals'],  # Start with one table
    category_filters={'vitals': ['heart_rate', 'sbp']}  # Just a few categories
)

# 2. Optimize threads
resources = co.get_sys_resource_info(print_summary=False)
co.create_wide_dataset(
    tables_to_load=['vitals'],
    category_filters={'vitals': ['heart_rate']},
    threads=resources['max_recommended_threads']
)

# 3. Use sampling for testing
co.create_wide_dataset(sample=True)  # Much faster
```

### Wrong Column Names in Output

**Symptom**: Expected 'heart_rate' but got 'heart_rate_mean'

**Explanation**: After temporal aggregation, columns get suffixes based on aggregation method: - `mean` → `_mean` - `max` → `_max` - `min` → `_min` - `boolean` → `_boolean`

``` python
# Wide dataset (no aggregation) - original column names
co.create_wide_dataset(...)
print(co.wide_df.columns)  # ['event_time', 'heart_rate', 'sbp', ...]

# After hourly aggregation - columns get suffixes
hourly = co.convert_wide_to_hourly(aggregation_config={'mean': ['heart_rate', 'sbp']})
print(hourly.columns)  # ['window_start_dttm', 'heart_rate_mean', 'sbp_mean', ...]
```

## Next Steps

-   [**Data Validation**](validation.md) - Ensure data quality
-   [**Individual Table Guides**](../api/tables.md) - Detailed table documentation
-   [**Orchestrator Guide**](../api/orchestrator.md) - Advanced orchestrator features
-   [**Timezone Handling**](timezones.md) - Multi-site data considerations